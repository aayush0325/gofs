package master

import (
	"context"
	"errors"
	"fmt"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/metadata"
)

// GetChunkLocation returns routing information for one chunk. For mutations it
// acquires a primary lease, fences replicas at the new version, and performs
// copy-on-write if a snapshot shares the chunk.
func (s *Service) GetChunkLocation(ctx context.Context, input string, offset int64, access gfsv1.ChunkAccess, appendSize int64) (*gfsv1.ChunkLocation, error) {
	p, err := metadata.NormalizePath(input)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if offset < 0 {
		return nil, fmt.Errorf("%w: negative offset", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var node *metadata.Node
	var chunks []*metadata.Chunk
	var size int64
	err = s.store.View(func(tx *metadata.Tx) error {
		var err error
		node, err = tx.GetNode(p)
		if errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, p)
		}
		if err != nil {
			return err
		}
		if node.Type != metadata.FileNode {
			return fmt.Errorf("%w: %s", ErrIsDirectory, p)
		}
		size, chunks, err = s.fileSizeLocked(tx, node)
		return err
	})
	if err != nil {
		return nil, err
	}

	index := int64(-1)
	switch access {
	case gfsv1.ChunkAccess_CHUNK_ACCESS_READ:
		if size == 0 || offset >= size {
			return nil, fmt.Errorf("%w: offset %d, size %d", ErrOutOfRange, offset, size)
		}
		index = offset / s.opts.ChunkSize
		if index >= int64(len(chunks)) {
			return nil, fmt.Errorf("%w: offset %d", ErrOutOfRange, offset)
		}
	case gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE:
		if offset > size {
			return nil, fmt.Errorf("%w: write offset %d beyond file size %d", ErrInvalid, offset, size)
		}
		index = offset / s.opts.ChunkSize
		if index > int64(len(chunks)) {
			return nil, fmt.Errorf("%w: non-contiguous chunk index %d", ErrInvalid, index)
		}
	case gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND:
		if appendSize <= 0 {
			appendSize = 1
		}
		if appendSize > s.opts.ChunkSize {
			return nil, fmt.Errorf("%w: record append is larger than a chunk", ErrInvalid)
		}
		index = int64(len(chunks)) - 1
		if index < 0 {
			index = 0
		} else if chunks[index].Size >= s.opts.ChunkSize {
			// The client pads a chunk that cannot hold the whole record and then
			// retries on a fresh chunk. Chunks remain aligned to chunkSize.
			index = int64(len(chunks))
		}
	default:
		return nil, fmt.Errorf("%w: unknown access mode", ErrInvalid)
	}

	if index == int64(len(chunks)) {
		if err := s.createChunkLocked(ctx, node, index); err != nil {
			return nil, err
		}
		chunks = append(chunks, nil)
		// Reload just the new chunk to get its metadata.
		err = s.store.View(func(tx *metadata.Tx) error {
			chunk, err := tx.GetChunk(node.Chunks[index])
			if err != nil {
				return err
			}
			chunks[index] = chunk
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if index < 0 || index >= int64(len(chunks)) || chunks[index] == nil {
		return nil, fmt.Errorf("%w: chunk index %d", ErrInvalid, index)
	}
	chunk := chunks[index]
	if access != gfsv1.ChunkAccess_CHUNK_ACCESS_READ && chunk.RefCount > 1 {
		chunk, err = s.unshareChunkLocked(ctx, node, int(index))
		if err != nil {
			return nil, err
		}
	}
	if access != gfsv1.ChunkAccess_CHUNK_ACCESS_READ {
		if err := s.grantLeaseLocked(ctx, chunk); err != nil {
			return nil, err
		}
		// Record the mutation time when the lease is issued. The authoritative
		// chunk size arrives later through ReportChunk.
		if err := s.store.Update(func(tx *metadata.Tx) error {
			current, err := tx.GetNode(p)
			if err != nil {
				return err
			}
			current.ModifiedAt = s.now().UnixNano()
			return tx.PutNode(current)
		}); err != nil {
			return nil, err
		}
	}

	replicas, primary, leaseUntil, err := s.locationReplicasLocked(chunk, access != gfsv1.ChunkAccess_CHUNK_ACCESS_READ)
	if err != nil {
		return nil, err
	}
	return &gfsv1.ChunkLocation{
		Handle: chunk.Handle, Version: chunk.Version, Size: chunk.Size,
		ChunkIndex: index, ChunkOffset: offset - index*s.opts.ChunkSize,
		Replicas: replicas, PrimaryServerId: primary, LeaseExpiresUnixNano: leaseUntil,
	}, nil
}
