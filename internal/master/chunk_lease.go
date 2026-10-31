package master

import (
	"context"
	"fmt"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/metadata"
	"github.com/google/uuid"
)

// createChunkLocked allocates a handle, creates it on selected chunkservers,
// and appends it to a file's chunk list.
func (s *Service) createChunkLocked(ctx context.Context, node *metadata.Node, index int64) error {
	if index != int64(len(node.Chunks)) {
		return fmt.Errorf("%w: cannot create chunk at index %d", ErrInvalid, index)
	}
	servers, err := s.pickServersLocked(ctx, s.opts.Replication, nil)
	if err != nil {
		return err
	}
	handle := uuid.NewString()
	replicas := make(map[string]metadata.Replica)
	for _, server := range servers {
		opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, _, createErr := s.admin.CreateChunk(opCtx, server.Address, handle, 1)
		cancel()
		if createErr != nil {
			s.log.Warn("chunk creation failed on server", "handle", handle, "server", server.ID, "error", createErr)
			continue
		}
		replicas[server.ID] = metadata.Replica{ServerID: server.ID, Address: server.Address}
	}
	if len(replicas) == 0 {
		return fmt.Errorf("%w: could not create chunk on any chunkserver", ErrNoReplica)
	}
	chunk := &metadata.Chunk{Handle: handle, Version: 1, Size: 0, RefCount: 1, Replicas: replicas}
	if err := s.store.Update(func(tx *metadata.Tx) error {
		if err := tx.PutChunk(chunk); err != nil {
			return err
		}
		node.Chunks = append(node.Chunks, handle)
		node.ModifiedAt = s.now().UnixNano()
		return tx.PutNode(node)
	}); err != nil {
		// Best-effort cleanup of chunks that never became visible.
		for _, replica := range replicas {
			_ = s.admin.DeleteChunk(ctx, replica.Address, handle)
		}
		return err
	}
	return nil
}

// unshareChunkLocked performs copy-on-write for a chunk shared with a snapshot.
// It copies data to a new handle and replaces the file's reference atomically.
func (s *Service) unshareChunkLocked(ctx context.Context, node *metadata.Node, index int) (*metadata.Chunk, error) {
	var old *metadata.Chunk
	err := s.store.View(func(tx *metadata.Tx) error {
		var err error
		old, err = tx.GetChunk(node.Chunks[index])
		return err
	})
	if err != nil {
		return nil, err
	}
	sources, err := s.activeReplicasLocked(old)
	if err != nil {
		return nil, err
	}
	targets, err := s.pickServersLocked(ctx, s.opts.Replication, nil)
	if err != nil {
		return nil, err
	}
	handle := uuid.NewString()
	newReplicas := make(map[string]metadata.Replica)
	sourceMessages := make([]*gfsv1.Replica, 0, len(sources))
	for _, source := range sources {
		sourceMessages = append(sourceMessages, &gfsv1.Replica{ServerId: source.ServerID, Address: source.Address})
	}
	for _, target := range targets {
		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, _, copyErr := s.admin.CopyChunk(opCtx, target.Address, handle, old.Handle, old.Version, sourceMessages)
		cancel()
		if copyErr != nil {
			s.log.Warn("copy-on-write failed on target", "handle", handle, "target", target.ID, "error", copyErr)
			continue
		}
		newReplicas[target.ID] = metadata.Replica{ServerID: target.ID, Address: target.Address}
	}
	if len(newReplicas) == 0 {
		return nil, fmt.Errorf("%w: copy-on-write could not create a replica", ErrNoReplica)
	}
	now := s.now().UnixNano()
	replacement := &metadata.Chunk{Handle: handle, Version: old.Version, Size: old.Size, RefCount: 1, Replicas: newReplicas}
	err = s.store.Update(func(tx *metadata.Tx) error {
		current, err := tx.GetChunk(old.Handle)
		if err != nil {
			return err
		}
		current.RefCount--
		if current.RefCount <= 0 {
			current.RefCount = 0
			current.OrphanedAt = now
			current.Primary = ""
			current.LeaseUntil = 0
		}
		if err := tx.PutChunk(current); err != nil {
			return err
		}
		if err := tx.PutChunk(replacement); err != nil {
			return err
		}
		currentNode, err := tx.GetNode(node.Path)
		if err != nil {
			return err
		}
		if index < 0 || index >= len(currentNode.Chunks) {
			return fmt.Errorf("%w: chunk index disappeared", ErrInvalid)
		}
		currentNode.Chunks[index] = handle
		currentNode.ModifiedAt = now
		return tx.PutNode(currentNode)
	})
	if err != nil {
		for _, replica := range newReplicas {
			_ = s.admin.DeleteChunk(ctx, replica.Address, handle)
		}
		return nil, err
	}
	return replacement, nil
}

// grantLeaseLocked makes chunk.Primary the lease holder. If no valid lease
// exists, it increments the chunk version and advances every active replica so
// stale replicas cannot serve reads as if they were current.
func (s *Service) grantLeaseLocked(ctx context.Context, chunk *metadata.Chunk) error {
	now := s.now()
	replicas, err := s.activeReplicasLocked(chunk)
	if err != nil {
		return err
	}
	if chunk.Primary != "" && now.UnixNano() < chunk.LeaseUntil {
		for _, replica := range replicas {
			if replica.ServerID == chunk.Primary {
				return nil
			}
		}
	}
	primary := replicas[0]
	for _, replica := range replicas {
		if replica.ServerID == chunk.Primary {
			primary = replica
			break
		}
	}
	chunk.Primary = primary.ServerID
	chunk.Version++
	chunk.LeaseUntil = now.Add(s.opts.LeaseDuration).UnixNano()
	if err := s.store.Update(func(tx *metadata.Tx) error {
		return tx.PutChunk(chunk)
	}); err != nil {
		return err
	}
	for _, replica := range replicas {
		opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, advanceErr := s.admin.AdvanceVersion(opCtx, replica.Address, chunk.Handle, chunk.Version)
		cancel()
		if advanceErr != nil {
			s.log.Warn("advance version failed", "handle", chunk.Handle, "server", replica.ServerID, "version", chunk.Version, "error", advanceErr)
		}
	}
	return nil
}
