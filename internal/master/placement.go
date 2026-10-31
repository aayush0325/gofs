package master

import (
	"context"
	"errors"
	"fmt"
	"sort"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/metadata"
)

// activeReplicasLocked filters a chunk's replicas to live chunkservers.
func (s *Service) activeReplicasLocked(chunk *metadata.Chunk) ([]metadata.Replica, error) {
	cutoff := s.now().Add(-s.opts.HeartbeatTimeout).UnixNano()
	var out []metadata.Replica
	err := s.store.View(func(tx *metadata.Tx) error {
		for id, replica := range chunk.Replicas {
			server, err := tx.GetServer(id)
			if errors.Is(err, metadata.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if server.LastSeen < cutoff {
				continue
			}
			replica.Address = server.Address
			out = append(out, replica)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ServerID < out[j].ServerID })
	return out, err
}

// locationReplicasLocked converts active replicas to wire form and enforces the
// need for a primary on mutations.
func (s *Service) locationReplicasLocked(chunk *metadata.Chunk, needPrimary bool) ([]*gfsv1.Replica, string, int64, error) {
	active, err := s.activeReplicasLocked(chunk)
	if err != nil {
		return nil, "", 0, err
	}
	if len(active) == 0 {
		return nil, "", 0, fmt.Errorf("%w: chunk %s", ErrNoReplica, chunk.Handle)
	}
	replicas := make([]*gfsv1.Replica, 0, len(active))
	for _, replica := range active {
		replicas = append(replicas, &gfsv1.Replica{ServerId: replica.ServerID, Address: replica.Address})
	}
	primary := chunk.Primary
	if needPrimary && primary == "" {
		return nil, "", 0, fmt.Errorf("%w: no primary for chunk %s", ErrNoReplica, chunk.Handle)
	}
	if !needPrimary && s.now().UnixNano() >= chunk.LeaseUntil {
		primary = ""
	}
	return replicas, primary, chunk.LeaseUntil, nil
}

// pickServersLocked selects up to count live chunkservers, preferring servers
// with fewer chunks and lower capacity utilization.
func (s *Service) pickServersLocked(_ context.Context, count int, exclude map[string]bool) ([]*metadata.Server, error) {
	now := s.now()
	cutoff := now.Add(-s.opts.HeartbeatTimeout).UnixNano()
	var candidates []*metadata.Server
	counts := make(map[string]int)
	err := s.store.View(func(tx *metadata.Tx) error {
		servers, err := tx.ListServers()
		if err != nil {
			return err
		}
		chunks, err := tx.ListChunks()
		if err != nil {
			return err
		}
		for _, chunk := range chunks {
			for id := range chunk.Replicas {
				counts[id]++
			}
		}
		for _, server := range servers {
			if exclude != nil && exclude[server.ID] {
				continue
			}
			if server.LastSeen < cutoff {
				continue
			}
			candidates = append(candidates, server)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(candidates, func(i, j int) bool {
		ci, cj := counts[candidates[i].ID], counts[candidates[j].ID]
		if ci != cj {
			return ci < cj
		}
		ui, uj := utilization(candidates[i]), utilization(candidates[j])
		if ui != uj {
			return ui < uj
		}
		return candidates[i].ID < candidates[j].ID
	})
	if len(candidates) > count {
		candidates = candidates[:count]
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: no live chunkservers", ErrNoReplica)
	}
	return candidates, nil
}

// utilization returns a chunkserver's used fraction, treating unknown capacity
// as zero utilization.
func utilization(server *metadata.Server) float64 {
	if server.Capacity <= 0 {
		return 0
	}
	return float64(server.Used) / float64(server.Capacity)
}
