package master

import (
	"context"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/metadata"
)

// RunMaintenance performs one garbage-collection and re-replication pass. It is
// serialized with metadata operations to keep placement decisions consistent.
func (s *Service) RunMaintenance(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	cutoff := now.Add(-s.opts.HeartbeatTimeout).UnixNano()
	var chunks []*metadata.Chunk
	var servers []*metadata.Server
	if err := s.store.View(func(tx *metadata.Tx) error {
		var err error
		chunks, err = tx.ListChunks()
		if err != nil {
			return err
		}
		servers, err = tx.ListServers()
		return err
	}); err != nil {
		return err
	}
	serverByID := make(map[string]*metadata.Server, len(servers))
	for _, server := range servers {
		serverByID[server.ID] = server
	}

	for _, chunk := range chunks {
		if chunk.RefCount <= 0 {
			if chunk.OrphanedAt == 0 {
				chunk.OrphanedAt = now.UnixNano()
				_ = s.store.Update(func(tx *metadata.Tx) error { return tx.PutChunk(chunk) })
				continue
			}
			if now.UnixNano()-chunk.OrphanedAt < s.opts.GCGrace.Nanoseconds() {
				continue
			}
			for _, replica := range chunk.Replicas {
				opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := s.admin.DeleteChunk(opCtx, replica.Address, chunk.Handle); err != nil {
					s.log.Warn("orphan chunk deletion failed", "handle", chunk.Handle, "address", replica.Address, "error", err)
				}
				cancel()
			}
			if err := s.store.Update(func(tx *metadata.Tx) error { return tx.DeleteChunk(chunk.Handle) }); err != nil {
				return err
			}
			continue
		}

		active := make([]metadata.Replica, 0, len(chunk.Replicas))
		for id, replica := range chunk.Replicas {
			server, ok := serverByID[id]
			if !ok || server.LastSeen < cutoff {
				delete(chunk.Replicas, id)
				if chunk.Primary == id {
					chunk.Primary = ""
					chunk.LeaseUntil = 0
				}
				continue
			}
			replica.Address = server.Address
			active = append(active, replica)
		}
		if len(active) < len(chunk.Replicas) {
			if err := s.store.Update(func(tx *metadata.Tx) error { return tx.PutChunk(chunk) }); err != nil {
				return err
			}
		}
		if len(active) == 0 || len(active) >= s.opts.Replication {
			continue
		}
		// Re-replicate from live replicas to new chunkservers.
		exclude := make(map[string]bool, len(chunk.Replicas))
		for id := range chunk.Replicas {
			exclude[id] = true
		}
		targets, err := s.pickServersLocked(ctx, s.opts.Replication-len(active), exclude)
		if err != nil {
			continue
		}
		sources := make([]*gfsv1.Replica, 0, len(active))
		for _, replica := range active {
			sources = append(sources, &gfsv1.Replica{ServerId: replica.ServerID, Address: replica.Address})
		}
		for _, target := range targets {
			opCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			_, _, copyErr := s.admin.CopyChunk(opCtx, target.Address, chunk.Handle, chunk.Handle, chunk.Version, sources)
			cancel()
			if copyErr != nil {
				s.log.Warn("re-replication failed", "handle", chunk.Handle, "target", target.ID, "error", copyErr)
				continue
			}
			chunk.Replicas[target.ID] = metadata.Replica{ServerID: target.ID, Address: target.Address}
		}
		if err := s.store.Update(func(tx *metadata.Tx) error { return tx.PutChunk(chunk) }); err != nil {
			return err
		}
	}
	return nil
}

// maxInt64 returns the larger of two int64 values.
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
