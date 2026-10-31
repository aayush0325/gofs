package master

import (
	"context"
	"errors"
	"fmt"

	"github.com/aayush0325/gofs/internal/metadata"
)

// Stat returns metadata and the logical file size.
func (s *Service) Stat(_ context.Context, input string) (*FileInfo, error) {
	p, err := metadata.NormalizePath(input)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var info *FileInfo
	err = s.store.View(func(tx *metadata.Tx) error {
		node, err := tx.GetNode(p)
		if errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, p)
		}
		if err != nil {
			return err
		}
		info, err = s.infoLocked(tx, node)
		return err
	})
	return info, err
}

// List returns direct children or, when recursive is true, every descendant.
// The requested directory itself is not included.
func (s *Service) List(_ context.Context, input string, recursive bool) ([]*FileInfo, error) {
	p, err := metadata.NormalizePath(input)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*FileInfo
	err = s.store.View(func(tx *metadata.Tx) error {
		node, err := tx.GetNode(p)
		if errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, p)
		}
		if err != nil {
			return err
		}
		if node.Type != metadata.DirectoryNode {
			return fmt.Errorf("%w: %s", ErrNotDirectory, p)
		}
		nodes, err := tx.ListChildren(p)
		if err != nil {
			return err
		}
		if recursive {
			nodes, err = tx.ListSubtree(p)
			if err != nil {
				return err
			}
			if len(nodes) > 0 && nodes[0].Path == p {
				nodes = nodes[1:]
			}
		}
		for _, child := range nodes {
			info, err := s.infoLocked(tx, child)
			if err != nil {
				return err
			}
			result = append(result, info)
		}
		return nil
	})
	return result, err
}

// infoLocked builds the public view of a node. The size is the sum of chunk
// sizes; a zero-length file has no chunks.
func (s *Service) infoLocked(tx *metadata.Tx, node *metadata.Node) (*FileInfo, error) {
	info := &FileInfo{
		Path: node.Path, Type: node.Type, Mode: node.Mode, ModifiedAt: node.ModifiedAt,
		ChunkCount: len(node.Chunks), SnapshotOf: node.SnapshotOf,
	}
	for _, handle := range node.Chunks {
		chunk, err := tx.GetChunk(handle)
		if errors.Is(err, metadata.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		info.Size += chunk.Size
	}
	return info, nil
}

// fileSizeLocked computes the logical file size and verifies that every chunk
// referenced by a file still has metadata.
func (s *Service) fileSizeLocked(tx *metadata.Tx, node *metadata.Node) (int64, []*metadata.Chunk, error) {
	chunks := make([]*metadata.Chunk, 0, len(node.Chunks))
	var size int64
	for _, handle := range node.Chunks {
		chunk, err := tx.GetChunk(handle)
		if errors.Is(err, metadata.ErrNotFound) {
			return 0, nil, fmt.Errorf("%w: file references missing chunk %s", ErrCorrupt, handle)
		}
		if err != nil {
			return 0, nil, err
		}
		chunks = append(chunks, chunk)
		size += chunk.Size
	}
	return size, chunks, nil
}

// FileSize computes a file's logical size by summing its chunk sizes.
func (s *Service) FileSize(ctx context.Context, path string) (int64, error) {
	info, err := s.Stat(ctx, path)
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}
