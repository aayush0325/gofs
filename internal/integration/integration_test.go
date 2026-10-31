package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/api"
	"github.com/aayush0325/gofs/internal/chunkclient"
	"github.com/aayush0325/gofs/internal/chunkserver"
	"github.com/aayush0325/gofs/internal/chunkstore"
	"github.com/aayush0325/gofs/internal/logging"
	"github.com/aayush0325/gofs/internal/master"
	"github.com/aayush0325/gofs/internal/metadata"
	"github.com/aayush0325/gofs/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cluster is an in-process master, three chunkservers, and an HTTP endpoint.
type cluster struct {
	client *api.Client
}

// startCluster builds a fully connected in-process GFS cluster using insecure
// gRPC so the test exercise focuses on protocol behavior rather than PKI.
func startCluster(t *testing.T) *cluster {
	t.Helper()
	log, err := logging.New("error")
	if err != nil {
		t.Fatal(err)
	}
	met := metrics.New()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "master.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	pool := chunkclient.New(nil, log)
	t.Cleanup(func() { _ = pool.Close() })

	service := master.NewService(store, pool, log, met, master.Options{
		ChunkSize: 1024, Replication: 3, LeaseDuration: time.Minute,
		HeartbeatTimeout: 5 * time.Second, GCGrace: time.Minute, MaintenanceInterval: time.Hour,
	})
	service.Start(ctx)

	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	masterGRPC := grpc.NewServer()
	gfsv1.RegisterMasterServer(masterGRPC, master.NewGRPCServer(service))
	go func() { _ = masterGRPC.Serve(masterListener) }()
	t.Cleanup(masterGRPC.Stop)

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("cs%d", i+1)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		chunkDir := filepath.Join(t.TempDir(), "chunks")
		chunkStore, err := chunkstore.Open(chunkDir, 1024, log)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient(masterListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		csPool := chunkclient.New(nil, log)
		cs := chunkserver.NewService(chunkStore, csPool, gfsv1.NewMasterClient(conn), chunkserver.Options{
			ID: id, Address: listener.Addr().String(), Capacity: 1 << 30, HeartbeatInterval: 200 * time.Millisecond,
		}, log, met)
		cs.Start(ctx)
		server := grpc.NewServer()
		gfsv1.RegisterChunkServerServer(server, cs)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(func() {
			server.Stop()
			_ = conn.Close()
			_ = csPool.Close()
			_ = chunkStore.Close()
		})
	}

	waitUntil(t, 5*time.Second, func() bool {
		status, err := service.GetClusterStatus(ctx)
		if err != nil {
			return false
		}
		alive := 0
		for _, server := range status.Chunkservers {
			if server.Alive {
				alive++
			}
		}
		return alive == 3
	})

	handler := master.NewHTTPHandler(service, pool, log, met, "")
	httpServer := httptest.NewServer(handler.Routes())
	t.Cleanup(httpServer.Close)
	t.Cleanup(cancel)
	return &cluster{client: api.New(httpServer.URL, "", 30*time.Second)}
}

// waitUntil polls condition until it returns true or the timeout expires.
func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// TestEndToEnd exercises namespace operations, multi-chunk write/read, record
// append padding, and snapshot copy-on-write through the HTTP API.
func TestEndToEnd(t *testing.T) {
	c := startCluster(t)
	ctx := context.Background()
	if _, err := c.client.Mkdir(ctx, "/data", false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.client.Create(ctx, "/data/file", 0o644); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("abcdef"), 334)
	payload = payload[:2000]
	result, err := c.client.Write(ctx, "/data/file", 0, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if result.Size != 2000 {
		t.Fatalf("expected size 2000, got %d", result.Size)
	}
	reader, err := c.client.Read(ctx, "/data/file", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("read payload mismatch")
	}

	appendPayload := []byte("record-that-does-not-fit-in-the-last-chunk-at-all")
	appended, err := c.client.Append(ctx, "/data/file", appendPayload)
	if err != nil {
		t.Fatal(err)
	}
	if appended.Offset != 2048 {
		t.Fatalf("expected record offset 2048 after padding, got %d", appended.Offset)
	}
	if appended.Size != 2048+int64(len(appendPayload)) {
		t.Fatalf("unexpected final size %d", appended.Size)
	}
	reader, err = c.client.Read(ctx, "/data/file", appended.Offset, int64(len(appendPayload)))
	if err != nil {
		t.Fatal(err)
	}
	tail, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tail, appendPayload) {
		t.Fatalf("append payload mismatch: %q", tail)
	}

	if _, err := c.client.Snapshot(ctx, "/data/file", "/data/file.snap"); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("new")
	if _, err := c.client.Write(ctx, "/data/file", 0, bytes.NewReader(replacement)); err != nil {
		t.Fatal(err)
	}
	reader, err = c.client.Read(ctx, "/data/file.snap", 0, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	snapshotBytes, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snapshotBytes, payload) {
		t.Fatal("snapshot was not isolated from copy-on-write update")
	}
	list, err := c.client.List(ctx, "/data", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Entries) != 2 {
		t.Fatalf("expected two entries, got %d", len(list.Entries))
	}
}
