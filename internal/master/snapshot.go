package master

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aayush0325/gofs/internal/metadata"
)

// Snapshot creates a copy of a file or directory subtree. Namespace metadata is
// copied in one transaction; chunk data is shared and copied on the first
// mutation of either copy.
func (s *Service) Snapshot(_ context.Context, sourceInput, destinationInput string) (*FileInfo, error) {
	source, err := metadata.NormalizePath(sourceInput)
	if err != nil {
		return nil, fmt.Errorf("%w: source: %v", ErrInvalid, err)
	}
	destination, err := metadata.NormalizePath(destinationInput)
	if err != nil {
		return nil, fmt.Errorf("%w: destination: %v", ErrInvalid, err)
	}
	if source == "/" {
		return nil, fmt.Errorf("%w: cannot snapshot root", ErrInvalid)
	}
	if metadata.IsDescendant(destination, source) {
		return nil, fmt.Errorf("%w: destination cannot be inside source", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var rootInfo *FileInfo
	err = s.store.Update(func(tx *metadata.Tx) error {
		if _, err := tx.GetNode(source); errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, source)
		} else if err != nil {
			return err
		}
		if _, err := tx.GetNode(destination); err == nil {
			return fmt.Errorf("%w: %s", ErrExists, destination)
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if err := s.ensureParentDirLocked(tx, destination, false); err != nil {
			return err
		}
		nodes, err := tx.ListSubtree(source)
		if err != nil {
			return err
		}
		now := s.now().UnixNano()
		for _, old := range nodes {
			newPath := destination + strings.TrimPrefix(old.Path, source)
			if _, err := tx.GetNode(newPath); err == nil {
				return fmt.Errorf("%w: %s", ErrExists, newPath)
			} else if !errors.Is(err, metadata.ErrNotFound) {
				return err
			}
		}
		for _, old := range nodes {
			newPath := destination + strings.TrimPrefix(old.Path, source)
			copy := *old
			copy.Path = newPath
			copy.CreatedAt = now
			copy.ModifiedAt = now
			copy.SnapshotOf = old.Path
			if copy.Type == metadata.FileNode {
				copy.Chunks = append([]string(nil), old.Chunks...)
				for _, handle := range copy.Chunks {
					chunk, err := tx.GetChunk(handle)
					if err != nil {
						return fmt.Errorf("snapshot chunk %s: %w", handle, err)
					}
					chunk.RefCount++
					if err := tx.PutChunk(chunk); err != nil {
						return err
					}
				}
			}
			if err := tx.PutNode(&copy); err != nil {
				return err
			}
			if old.Path == source {
				rootInfo, err = s.infoLocked(tx, &copy)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return rootInfo, err
}
