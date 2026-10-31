package master

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/aayush0325/gofs/internal/metadata"
)

// ReportedChunk is a chunkserver's local view of a chunk.
type ReportedChunk struct {
	Handle  string
	Version uint64
	Size    int64
}

// ServerStatus is a point-in-time chunkserver view for HTTP/gRPC status calls.
type ServerStatus struct {
	ID         string `json:"id"`
	Address    string `json:"address"`
	Capacity   int64  `json:"capacity_bytes"`
	Used       int64  `json:"used_bytes"`
	LastSeen   int64  `json:"last_seen_unix_nano"`
	Alive      bool   `json:"alive"`
	ChunkCount int64  `json:"chunk_count"`
}

// ClusterStatus summarizes the namespace and chunk placement.
type ClusterStatus struct {
	ChunkSize      int64          `json:"chunk_size_bytes"`
	Replication    int            `json:"replication_factor"`
	Files          int64          `json:"files"`
	Directories    int64          `json:"directories"`
	Chunks         int64          `json:"chunks"`
	OrphanedChunks int64          `json:"orphaned_chunks"`
	Chunkservers   []ServerStatus `json:"chunkservers"`
	StartedAt      int64          `json:"started_at_unix_nano"`
}

// Register records a chunkserver and reconciles its local chunk list with the
// master's metadata. Unknown chunks are returned for local deletion. Chunks the
// server no longer has are removed from that server's replica set.
func (s *Service) Register(_ context.Context, id, address string, capacity, used int64, reported []ReportedChunk) ([]string, error) {
	if id == "" || address == "" {
		return nil, fmt.Errorf("%w: server id and address are required", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UnixNano()
	var deleteHandles []string
	err := s.store.Update(func(tx *metadata.Tx) error {
		server, err := tx.GetServer(id)
		if errors.Is(err, metadata.ErrNotFound) {
			server = &metadata.Server{ID: id, FirstSeen: now}
		} else if err != nil {
			return err
		}
		server.Address = address
		server.Capacity = capacity
		server.Used = used
		server.LastSeen = now
		if err := tx.PutServer(server); err != nil {
			return err
		}

		reportedSet := make(map[string]ReportedChunk, len(reported))
		for _, state := range reported {
			reportedSet[state.Handle] = state
		}
		chunks, err := tx.ListChunks()
		if err != nil {
			return err
		}
		for _, chunk := range chunks {
			state, present := reportedSet[chunk.Handle]
			if !present {
				if _, ok := chunk.Replicas[id]; ok {
					delete(chunk.Replicas, id)
					if chunk.Primary == id {
						chunk.Primary = ""
						chunk.LeaseUntil = 0
					}
					if err := tx.PutChunk(chunk); err != nil {
						return err
					}
				}
				continue
			}
			delete(reportedSet, chunk.Handle)
			if chunk.Replicas == nil {
				chunk.Replicas = make(map[string]metadata.Replica)
			}
			chunk.Replicas[id] = metadata.Replica{ServerID: id, Address: address}
			if state.Version > chunk.Version {
				chunk.Version = state.Version
			}
			if state.Size > chunk.Size {
				chunk.Size = state.Size
			}
			if err := tx.PutChunk(chunk); err != nil {
				return err
			}
		}
		for handle := range reportedSet {
			deleteHandles = append(deleteHandles, handle)
		}
		return nil
	})
	sort.Strings(deleteHandles)
	return deleteHandles, err
}

// Heartbeat refreshes liveness and performs the same replica reconciliation as
// registration. It is intentionally idempotent.
func (s *Service) Heartbeat(ctx context.Context, id, address string, capacity, used int64, reported []ReportedChunk) ([]string, error) {
	handles, err := s.Register(ctx, id, address, capacity, used, reported)
	if s.met != nil {
		s.met.Heartbeats.Inc()
	}
	return handles, err
}

// ReportChunk records the committed size/version of a chunk after a mutation.
// It is called by the primary chunkserver before it acknowledges the client.
func (s *Service) ReportChunk(_ context.Context, serverID, handle string, version uint64, size int64) error {
	if serverID == "" || handle == "" {
		return fmt.Errorf("%w: server and handle are required", ErrInvalid)
	}
	if size < 0 {
		return fmt.Errorf("%w: negative chunk size", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Update(func(tx *metadata.Tx) error {
		chunk, err := tx.GetChunk(handle)
		if errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: chunk %s", ErrNotFound, handle)
		}
		if err != nil {
			return err
		}
		server, err := tx.GetServer(serverID)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if server != nil {
			if chunk.Replicas == nil {
				chunk.Replicas = make(map[string]metadata.Replica)
			}
			chunk.Replicas[serverID] = metadata.Replica{ServerID: serverID, Address: server.Address}
			server.Used += maxInt64(0, size-chunk.Size)
			server.LastSeen = s.now().UnixNano()
			if err := tx.PutServer(server); err != nil {
				return err
			}
		}
		if version > chunk.Version {
			chunk.Version = version
		}
		if size > chunk.Size {
			chunk.Size = size
		}
		return tx.PutChunk(chunk)
	})
}

// GetClusterStatus returns a consistent snapshot of namespace and placement
// statistics.
func (s *Service) GetClusterStatus(_ context.Context) (*ClusterStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := &ClusterStatus{
		ChunkSize: s.opts.ChunkSize, Replication: s.opts.Replication,
		StartedAt: s.start.UnixNano(), Chunkservers: []ServerStatus{},
	}
	cutoff := s.now().Add(-s.opts.HeartbeatTimeout).UnixNano()
	err := s.store.View(func(tx *metadata.Tx) error {
		files, dirs, err := tx.CountNodes()
		if err != nil {
			return err
		}
		status.Files, status.Directories = int64(files), int64(dirs)
		chunks, err := tx.ListChunks()
		if err != nil {
			return err
		}
		status.Chunks = int64(len(chunks))
		counts := make(map[string]int64)
		for _, chunk := range chunks {
			if chunk.RefCount <= 0 {
				status.OrphanedChunks++
			}
			for id := range chunk.Replicas {
				counts[id]++
			}
		}
		servers, err := tx.ListServers()
		if err != nil {
			return err
		}
		for _, server := range servers {
			status.Chunkservers = append(status.Chunkservers, ServerStatus{
				ID: server.ID, Address: server.Address, Capacity: server.Capacity,
				Used: server.Used, LastSeen: server.LastSeen, Alive: server.LastSeen >= cutoff,
				ChunkCount: counts[server.ID],
			})
		}
		sort.Slice(status.Chunkservers, func(i, j int) bool {
			return status.Chunkservers[i].ID < status.Chunkservers[j].ID
		})
		return nil
	})
	return status, err
}
