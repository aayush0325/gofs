// Package chunkserver implements the data plane: chunk persistence, primary
// replication, record append, checksums, and registration heartbeats.
package chunkserver

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/chunkclient"
	"github.com/aayush0325/gofs/internal/chunkstore"
	"github.com/aayush0325/gofs/internal/keyedmutex"
	"github.com/aayush0325/gofs/internal/metrics"
	"google.golang.org/grpc"
)

// MasterClient is the narrow subset of the master service a chunkserver needs.
// The generated gRPC client satisfies it directly.
type MasterClient interface {
	RegisterChunkserver(ctx context.Context, in *gfsv1.RegisterChunkserverRequest, opts ...grpc.CallOption) (*gfsv1.RegisterChunkserverResponse, error)
	Heartbeat(ctx context.Context, in *gfsv1.HeartbeatRequest, opts ...grpc.CallOption) (*gfsv1.HeartbeatResponse, error)
	ReportChunk(ctx context.Context, in *gfsv1.ReportChunkRequest, opts ...grpc.CallOption) (*gfsv1.ReportChunkResponse, error)
}

// Options configures a chunkserver data service.
type Options struct {
	ID                string
	Address           string
	Capacity          int64
	HeartbeatInterval time.Duration
}

// Service implements the generated ChunkServer service and its heartbeat loop.
type Service struct {
	gfsv1.UnimplementedChunkServerServer

	store  *chunkstore.Store
	pool   *chunkclient.Pool
	master MasterClient
	opts   Options
	log    *slog.Logger
	met    *metrics.Metrics

	locks *keyedmutex.Map
	ready atomic.Bool
}

// NewService constructs a chunkserver service.
func NewService(store *chunkstore.Store, pool *chunkclient.Pool, master MasterClient, opts Options, log *slog.Logger, met *metrics.Metrics) *Service {
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 5 * time.Second
	}
	return &Service{
		store: store, pool: pool, master: master, opts: opts, log: log, met: met,
		locks: keyedmutex.New(),
	}
}

// Ready reports whether the chunkserver has successfully registered with the
// master and its last heartbeat succeeded.
func (s *Service) Ready() bool { return s.ready.Load() }

// Start launches registration and heartbeat workers. It returns immediately.
func (s *Service) Start(ctx context.Context) {
	go func() {
		backoff := time.Second
		for {
			if err := s.register(ctx); err != nil {
				s.ready.Store(false)
				s.log.Warn("chunkserver registration failed", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			s.ready.Store(true)
			backoff = time.Second
			break
		}
		ticker := time.NewTicker(s.opts.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.heartbeat(ctx); err != nil {
					s.ready.Store(false)
					s.log.Warn("chunkserver heartbeat failed", "error", err)
				} else {
					s.ready.Store(true)
				}
			}
		}
	}()
}

// register performs the initial registration and locally deletes chunks that
// the master does not know about.
func (s *Service) register(ctx context.Context) error {
	states := s.store.States()
	req := &gfsv1.RegisterChunkserverRequest{
		ServerId: s.opts.ID, Address: s.opts.Address,
		CapacityBytes: s.opts.Capacity, UsedBytes: s.store.UsedBytes(),
		Chunks: toProtoStates(states),
	}
	resp, err := s.master.RegisterChunkserver(ctx, req)
	if err != nil {
		return err
	}
	s.deleteHandles(resp.DeleteHandles)
	s.log.Info("registered with master", "server_id", s.opts.ID, "address", s.opts.Address, "chunks", len(states))
	return nil
}

// heartbeat refreshes liveness and reports local chunk state.
func (s *Service) heartbeat(ctx context.Context) error {
	states := s.store.States()
	resp, err := s.master.Heartbeat(ctx, &gfsv1.HeartbeatRequest{
		ServerId: s.opts.ID, Address: s.opts.Address,
		CapacityBytes: s.opts.Capacity, UsedBytes: s.store.UsedBytes(),
		Chunks: toProtoStates(states),
	})
	if err != nil {
		return err
	}
	s.deleteHandles(resp.DeleteHandles)
	return nil
}

// deleteHandles removes chunks the master declared unknown or orphaned.
func (s *Service) deleteHandles(handles []string) {
	for _, handle := range handles {
		if err := s.store.Delete(handle); err != nil {
			s.log.Warn("failed to delete stale chunk", "handle", handle, "error", err)
		} else {
			s.log.Info("deleted stale chunk", "handle", handle)
		}
	}
}

// report informs the master of the committed chunk size and version.
func (s *Service) report(ctx context.Context, handle string, version uint64, size int64) error {
	if s.master == nil {
		return nil
	}
	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.master.ReportChunk(reportCtx, &gfsv1.ReportChunkRequest{
		ServerId: s.opts.ID, Handle: handle, Version: version, Size: size,
	})
	return err
}

// observe records a chunk operation metric.
func (s *Service) observe(operation string, err error) {
	if s.met != nil {
		s.met.ObserveChunk(operation, err)
	}
}
