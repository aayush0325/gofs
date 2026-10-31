package master

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/logging"
	"github.com/aayush0325/gofs/internal/metadata"
	"github.com/aayush0325/gofs/internal/metrics"
)

// fakeAdmin records chunk placement operations without opening real gRPC
// connections, allowing deterministic master tests.
type fakeAdmin struct {
	mu      sync.Mutex
	created map[string][]string
	copies  map[string]string
	deleted map[string]bool
}

// newFakeAdmin returns an empty fake chunk administration backend.
func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{created: map[string][]string{}, copies: map[string]string{}, deleted: map[string]bool{}}
}

// CreateChunk records that a chunk was created on a chunkserver.
func (f *fakeAdmin) CreateChunk(_ context.Context, address, handle string, _ uint64) (int64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created[handle] = append(f.created[handle], address)
	return 0, 1, nil
}

// AdvanceVersion accepts every version fence.
func (f *fakeAdmin) AdvanceVersion(_ context.Context, _ string, _ string, version uint64) (uint64, error) {
	return version, nil
}

// CopyChunk records the target handle for copy-on-write and repair.
func (f *fakeAdmin) CopyChunk(_ context.Context, _, target, source string, _ uint64, _ []*gfsv1.Replica) (int64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copies[target] = source
	return 0, 1, nil
}

// DeleteChunk records physical chunk deletion.
func (f *fakeAdmin) DeleteChunk(_ context.Context, _, handle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted[handle] = true
	return nil
}

// newTestService builds a three-server master with a controllable clock.
func newTestService(t *testing.T, chunkSize int64) (*Service, *fakeAdmin, *time.Time) {
	t.Helper()
	dir := t.TempDir()
	store, err := metadata.Open(filepath.Join(dir, "master.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	log, err := logging.New("error")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	clock := func() time.Time { return now }
	admin := newFakeAdmin()
	service := NewService(store, admin, log, metrics.New(), Options{
		ChunkSize: chunkSize, Replication: 3, LeaseDuration: time.Minute,
		HeartbeatTimeout: time.Minute, GCGrace: time.Minute, Clock: clock,
	})
	ctx := context.Background()
	for _, id := range []string{"cs1", "cs2", "cs3"} {
		if _, err := service.Register(ctx, id, id+":9091", 1<<30, 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	return service, admin, &now
}

// mkdirAndCreate creates a directory and file used by multiple tests.
func mkdirAndCreate(t *testing.T, service *Service) {
	t.Helper()
	if _, err := service.Mkdir(context.Background(), "/data", false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateFile(context.Background(), "/data/file", 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestNamespaceOperations covers create, list, rename, snapshot, and delete.
func TestNamespaceOperations(t *testing.T) {
	service, _, _ := newTestService(t, 64)
	ctx := context.Background()
	if _, err := service.Mkdir(ctx, "/a/b", true); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateFile(ctx, "/a/b/file", 0o644); err != nil {
		t.Fatal(err)
	}
	entries, err := service.List(ctx, "/a", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 recursive entries, got %d", len(entries))
	}
	if err := service.Rename(ctx, "/a", "/renamed"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Stat(ctx, "/renamed/b/file"); err != nil {
		t.Fatalf("renamed file missing: %v", err)
	}
	if err := service.Delete(ctx, "/renamed", false); err == nil {
		t.Fatal("expected non-empty directory delete to fail")
	}
	if err := service.Delete(ctx, "/renamed", true); err != nil {
		t.Fatal(err)
	}
}

// TestChunkLocationAndReports verifies chunk allocation, lease metadata, and
// file size updates from a primary report.
func TestChunkLocationAndReports(t *testing.T) {
	service, admin, _ := newTestService(t, 64)
	ctx := context.Background()
	mkdirAndCreate(t, service)
	location, err := service.GetChunkLocation(ctx, "/data/file", 0, gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(admin.created[location.Handle]) != 3 {
		t.Fatalf("expected 3 creates, got %d", len(admin.created[location.Handle]))
	}
	if location.PrimaryServerId == "" || location.Version < 2 {
		t.Fatalf("expected a leased primary and incremented version: %+v", location)
	}
	if err := service.ReportChunk(ctx, location.PrimaryServerId, location.Handle, location.Version, 11); err != nil {
		t.Fatal(err)
	}
	info, err := service.Stat(ctx, "/data/file")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 11 {
		t.Fatalf("expected size 11, got %d", info.Size)
	}
}

// TestSnapshotCopyOnWrite verifies that snapshot metadata sharing a chunk
// triggers a new handle before the next mutation.
func TestSnapshotCopyOnWrite(t *testing.T) {
	service, admin, _ := newTestService(t, 64)
	ctx := context.Background()
	mkdirAndCreate(t, service)
	first, err := service.GetChunkLocation(ctx, "/data/file", 0, gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReportChunk(ctx, first.PrimaryServerId, first.Handle, first.Version, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Snapshot(ctx, "/data/file", "/data/file.snap"); err != nil {
		t.Fatal(err)
	}
	second, err := service.GetChunkLocation(ctx, "/data/file", 0, gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE, 0)
	if err != nil {
		t.Fatal(err)
	}
	if second.Handle == first.Handle {
		t.Fatal("expected copy-on-write to allocate a new chunk handle")
	}
	if admin.copies[second.Handle] != first.Handle {
		t.Fatalf("expected copy from %s, got %q", first.Handle, admin.copies[second.Handle])
	}
	snapshot, err := service.Stat(ctx, "/data/file.snap")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Size != 5 {
		t.Fatalf("expected snapshot size 5, got %d", snapshot.Size)
	}
}

// TestAppendChunkRollover verifies that a full last chunk causes allocation of
// the next chunk rather than crossing the configured chunk boundary.
func TestAppendChunkRollover(t *testing.T) {
	service, _, _ := newTestService(t, 8)
	ctx := context.Background()
	mkdirAndCreate(t, service)
	first, err := service.GetChunkLocation(ctx, "/data/file", 0, gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReportChunk(ctx, first.PrimaryServerId, first.Handle, first.Version, 7); err != nil {
		t.Fatal(err)
	}
	second, err := service.GetChunkLocation(ctx, "/data/file", 7, gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND, 4)
	if err != nil {
		t.Fatal(err)
	}
	if second.Handle != first.Handle {
		t.Fatal("a record that fits in the last chunk should stay on that chunk")
	}
	if err := service.ReportChunk(ctx, second.PrimaryServerId, first.Handle, second.Version, 8); err != nil {
		t.Fatal(err)
	}
	third, err := service.GetChunkLocation(ctx, "/data/file", 8, gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND, 4)
	if err != nil {
		t.Fatal(err)
	}
	if third.Handle == first.Handle || third.ChunkIndex != 1 {
		t.Fatalf("expected a new chunk at index 1, got %+v", third)
	}
}

// TestGarbageCollection verifies orphaned chunk metadata is physically deleted
// after the configured grace period.
func TestGarbageCollection(t *testing.T) {
	service, admin, now := newTestService(t, 64)
	ctx := context.Background()
	mkdirAndCreate(t, service)
	location, err := service.GetChunkLocation(ctx, "/data/file", 0, gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReportChunk(ctx, location.PrimaryServerId, location.Handle, location.Version, 4); err != nil {
		t.Fatal(err)
	}
	if err := service.Delete(ctx, "/data/file", false); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Minute)
	if err := service.RunMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if !admin.deleted[location.Handle] {
		t.Fatal("expected orphan chunk to be physically deleted")
	}
}
