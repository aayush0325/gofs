package master

import (
	"context"
	"log/slog"
	"sync"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/metadata"
	"github.com/aayush0325/gofs/internal/metrics"
)

// ChunkAdmin is the subset of chunkserver operations the master needs. It is
// an interface so placement, snapshot, and repair logic can be unit tested with
// an in-memory implementation.
type ChunkAdmin interface {
	CreateChunk(ctx context.Context, address, handle string, version uint64) (int64, uint64, error)
	AdvanceVersion(ctx context.Context, address, handle string, version uint64) (uint64, error)
	CopyChunk(ctx context.Context, address, target, source string, version uint64, sources []*gfsv1.Replica) (int64, uint64, error)
	DeleteChunk(ctx context.Context, address, handle string) error
}

// Options tunes the master's placement and background workers.
type Options struct {
	ChunkSize           int64
	Replication         int
	LeaseDuration       time.Duration
	HeartbeatTimeout    time.Duration
	GCGrace             time.Duration
	MaintenanceInterval time.Duration
	Clock               func() time.Time
}

// DefaultOptions returns the GFS defaults used by the master binary: 64 MiB
// chunks, three replicas, 60 second leases, and a one minute deletion grace.
func DefaultOptions() Options {
	return Options{
		ChunkSize:           64 << 20,
		Replication:         3,
		LeaseDuration:       60 * time.Second,
		HeartbeatTimeout:    15 * time.Second,
		GCGrace:             time.Minute,
		MaintenanceInterval: 10 * time.Second,
		Clock:               time.Now,
	}
}

// withDefaults fills zero-valued options with production defaults.
func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o.ChunkSize <= 0 {
		o.ChunkSize = d.ChunkSize
	}
	if o.Replication <= 0 {
		o.Replication = d.Replication
	}
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = d.LeaseDuration
	}
	if o.HeartbeatTimeout <= 0 {
		o.HeartbeatTimeout = d.HeartbeatTimeout
	}
	if o.GCGrace < 0 {
		o.GCGrace = d.GCGrace
	}
	if o.MaintenanceInterval <= 0 {
		o.MaintenanceInterval = d.MaintenanceInterval
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return o
}

// Service is the in-process master implementation shared by the gRPC server and
// the HTTP client API.
type Service struct {
	store *metadata.Store
	admin ChunkAdmin
	log   *slog.Logger
	opts  Options
	met   *metrics.Metrics
	start time.Time

	mu sync.Mutex
}

// FileInfo is a protocol-neutral namespace view used by HTTP and gRPC.
type FileInfo struct {
	Path       string            `json:"path"`
	Type       metadata.NodeType `json:"type"`
	Mode       uint32            `json:"mode"`
	Size       int64             `json:"size"`
	ModifiedAt int64             `json:"mtime_unix_nano"`
	ChunkCount int               `json:"chunk_count"`
	SnapshotOf string            `json:"snapshot_of,omitempty"`
}

// NewService constructs a master service.
func NewService(store *metadata.Store, admin ChunkAdmin, log *slog.Logger, met *metrics.Metrics, opts Options) *Service {
	return &Service{
		store: store,
		admin: admin,
		log:   log,
		opts:  opts.withDefaults(),
		met:   met,
		start: time.Now(),
	}
}

// Start launches background re-replication and garbage collection. The workers
// stop when ctx is canceled.
func (s *Service) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.opts.MaintenanceInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.RunMaintenance(ctx); err != nil {
					s.log.Warn("maintenance pass failed", "error", err)
				}
			}
		}
	}()
}

// now returns the configured clock.
func (s *Service) now() time.Time { return s.opts.Clock() }

// ChunkSize returns the configured maximum chunk size.
func (s *Service) ChunkSize() int64 { return s.opts.ChunkSize }

// Replication returns the configured replication factor.
func (s *Service) Replication() int { return s.opts.Replication }

// GCGrace returns the configured deletion grace period.
func (s *Service) GCGrace() time.Duration { return s.opts.GCGrace }

// Started returns the process/service start time.
func (s *Service) Started() time.Time { return s.start }
