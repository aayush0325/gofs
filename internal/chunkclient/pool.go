// Package chunkclient provides a reusable gRPC connection pool and the data
// path helpers used by the master HTTP API and chunkserver repair worker.
package chunkclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// MaxBlockBytes is the largest data payload sent in one gRPC request. The
// chunkserver enforces the same limit to keep memory bounded.
const MaxBlockBytes = 4 << 20

// Pool caches mutually authenticated gRPC connections by address.
type Pool struct {
	cfg *tls.Config
	log *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// New creates a pool. A nil TLS configuration is allowed only for tests and
// explicitly insecure local runs.
func New(cfg *tls.Config, log *slog.Logger) *Pool {
	return &Pool{cfg: cfg, log: log, conns: make(map[string]*grpc.ClientConn)}
}

// Close closes all cached connections.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var first error
	for address, conn := range p.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
		delete(p.conns, address)
	}
	return first
}

// Conn returns a cached connection, dialing the address if necessary.
func (p *Pool) Conn(ctx context.Context, address string) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.New("empty chunkserver address")
	}
	p.mu.Lock()
	if conn := p.conns[address]; conn != nil {
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(8<<20)),
	}
	if p.cfg != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(p.cfg.Clone())))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.DialContext(dialCtx, address, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial chunkserver %s: %w", address, err)
	}

	p.mu.Lock()
	if existing := p.conns[address]; existing != nil {
		p.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	p.conns[address] = conn
	p.mu.Unlock()
	p.log.Debug("connected to chunkserver", "address", address)
	return conn, nil
}

// Client returns the generated chunkserver stub for an address.
func (p *Pool) Client(ctx context.Context, address string) (gfsv1.ChunkServerClient, error) {
	conn, err := p.Conn(ctx, address)
	if err != nil {
		return nil, err
	}
	return gfsv1.NewChunkServerClient(conn), nil
}

// CreateChunk asks a chunkserver to create an empty chunk.
func (p *Pool) CreateChunk(ctx context.Context, address, handle string, version uint64) (int64, uint64, error) {
	client, err := p.Client(ctx, address)
	if err != nil {
		return 0, 0, err
	}
	resp, err := client.CreateChunk(ctx, &gfsv1.CreateChunkRequest{Handle: handle, Version: version})
	if err != nil {
		return 0, 0, fmt.Errorf("create chunk %s on %s: %w", handle, address, err)
	}
	return resp.Size, resp.Version, nil
}

// AdvanceVersion fences a replica at a newer master-issued version.
func (p *Pool) AdvanceVersion(ctx context.Context, address, handle string, version uint64) (uint64, error) {
	client, err := p.Client(ctx, address)
	if err != nil {
		return 0, err
	}
	resp, err := client.AdvanceVersion(ctx, &gfsv1.AdvanceVersionRequest{Handle: handle, Version: version})
	if err != nil {
		return 0, fmt.Errorf("advance chunk %s version on %s: %w", handle, address, err)
	}
	return resp.Version, nil
}

// CopyChunk copies a chunk to a target address. sources tells the destination
// where to fetch data when the source chunk is not local.
func (p *Pool) CopyChunk(ctx context.Context, address, target, source string, version uint64, sources []*gfsv1.Replica) (int64, uint64, error) {
	client, err := p.Client(ctx, address)
	if err != nil {
		return 0, 0, err
	}
	resp, err := client.CopyChunk(ctx, &gfsv1.CopyChunkRequest{
		Handle: target, SourceHandle: source, Version: version, Sources: sources,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("copy chunk %s -> %s on %s: %w", source, target, address, err)
	}
	return resp.Size, resp.Version, nil
}

// DeleteChunk removes a chunk from a chunkserver.
func (p *Pool) DeleteChunk(ctx context.Context, address, handle string) error {
	client, err := p.Client(ctx, address)
	if err != nil {
		return err
	}
	_, err = client.DeleteChunk(ctx, &gfsv1.DeleteChunkRequest{Handle: handle})
	if err != nil {
		return fmt.Errorf("delete chunk %s on %s: %w", handle, address, err)
	}
	return nil
}

// CheckChunk verifies a chunk's checksums on one chunkserver.
func (p *Pool) CheckChunk(ctx context.Context, address, handle string) (*gfsv1.CheckChunkResponse, error) {
	client, err := p.Client(ctx, address)
	if err != nil {
		return nil, err
	}
	return client.CheckChunk(ctx, &gfsv1.CheckChunkRequest{Handle: handle})
}

// WriteChunk sends an overwrite to the lease primary. The primary is
// responsible for forwarding to all replicas listed in location.
func (p *Pool) WriteChunk(ctx context.Context, location *gfsv1.ChunkLocation, offset int64, data []byte, mutationID string) (*gfsv1.WriteChunkResponse, error) {
	if location.PrimaryServerId == "" {
		return nil, errors.New("chunk has no primary lease")
	}
	primary, err := p.replicaAddress(location, location.PrimaryServerId)
	if err != nil {
		return nil, err
	}
	client, err := p.Client(ctx, primary)
	if err != nil {
		return nil, err
	}
	secondaries := secondaries(location, location.PrimaryServerId)
	return client.WriteChunk(ctx, &gfsv1.WriteChunkRequest{
		Handle: location.Handle, Version: location.Version, Offset: offset,
		Data: data, MutationId: mutationID, Secondaries: secondaries,
	})
}

// AppendChunk sends an atomic record append to the lease primary.
func (p *Pool) AppendChunk(ctx context.Context, location *gfsv1.ChunkLocation, data []byte, mutationID string) (*gfsv1.AppendChunkResponse, error) {
	if location.PrimaryServerId == "" {
		return nil, errors.New("chunk has no primary lease")
	}
	primary, err := p.replicaAddress(location, location.PrimaryServerId)
	if err != nil {
		return nil, err
	}
	client, err := p.Client(ctx, primary)
	if err != nil {
		return nil, err
	}
	secondaries := secondaries(location, location.PrimaryServerId)
	return client.AppendChunk(ctx, &gfsv1.AppendChunkRequest{
		Handle: location.Handle, Version: location.Version,
		Data: data, MutationId: mutationID, Secondaries: secondaries,
	})
}

// ReadChunk reads one block from the first healthy replica, trying the primary
// first and then every other replica. It returns the source address that
// produced the data.
func (p *Pool) ReadChunk(ctx context.Context, location *gfsv1.ChunkLocation, offset, length int64) (data []byte, size int64, version uint64, address string, err error) {
	order := replicaOrder(location)
	if len(order) == 0 {
		return nil, 0, 0, "", errors.New("chunk has no replicas")
	}
	var lastErr error
	for _, replica := range order {
		client, dialErr := p.Client(ctx, replica.Address)
		if dialErr != nil {
			lastErr = dialErr
			continue
		}
		resp, readErr := client.ReadChunk(ctx, &gfsv1.ReadChunkRequest{
			Handle: location.Handle, Version: location.Version, Offset: offset, Length: length,
		})
		if readErr == nil {
			return resp.Data, resp.Size, resp.Version, replica.Address, nil
		}
		lastErr = readErr
	}
	return nil, 0, 0, "", fmt.Errorf("read chunk %s from %d replicas: %w", location.Handle, len(order), lastErr)
}

// ReadChunkFrom reads from a specific chunkserver, used by remote copy.
func (p *Pool) ReadChunkFrom(ctx context.Context, address string, request *gfsv1.ReadChunkRequest) (*gfsv1.ReadChunkResponse, error) {
	client, err := p.Client(ctx, address)
	if err != nil {
		return nil, err
	}
	return client.ReadChunk(ctx, request)
}

// replicaAddress resolves a server ID to its registered address.
func (p *Pool) replicaAddress(location *gfsv1.ChunkLocation, serverID string) (string, error) {
	for _, replica := range location.Replicas {
		if replica.ServerId == serverID {
			return replica.Address, nil
		}
	}
	return "", fmt.Errorf("primary %s has no address in chunk location", serverID)
}

// secondaries returns every replica except the lease primary.
func secondaries(location *gfsv1.ChunkLocation, primary string) []*gfsv1.Replica {
	var out []*gfsv1.Replica
	for _, replica := range location.Replicas {
		if replica.ServerId != primary {
			out = append(out, replica)
		}
	}
	return out
}

// replicaOrder puts the primary first and removes duplicate server IDs.
func replicaOrder(location *gfsv1.ChunkLocation) []*gfsv1.Replica {
	var out []*gfsv1.Replica
	seen := make(map[string]bool)
	if location.PrimaryServerId != "" {
		for _, replica := range location.Replicas {
			if replica.ServerId == location.PrimaryServerId {
				out = append(out, replica)
				seen[replica.ServerId] = true
			}
		}
	}
	for _, replica := range location.Replicas {
		if !seen[replica.ServerId] {
			out = append(out, replica)
			seen[replica.ServerId] = true
		}
	}
	return out
}
