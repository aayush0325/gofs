package master

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aayush0325/gofs/internal/metadata"
)

// Mkdir creates a directory. When recursive is true, missing ancestors are
// created and an existing directory is treated as success.
func (s *Service) Mkdir(_ context.Context, input string, recursive bool) (*FileInfo, error) {
	p, err := metadata.NormalizePath(input)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var info *FileInfo
	err = s.store.Update(func(tx *metadata.Tx) error {
		if p == "/" {
			root, err := tx.GetNode("/")
			if err != nil {
				return err
			}
			info, err = s.infoLocked(tx, root)
			return err
		}
		if existing, err := tx.GetNode(p); err == nil {
			if existing.Type != metadata.DirectoryNode {
				return fmt.Errorf("%w: %s exists and is a file", ErrExists, p)
			}
			if !recursive {
				return fmt.Errorf("%w: %s", ErrExists, p)
			}
			info, err = s.infoLocked(tx, existing)
			return err
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if err := s.ensureParentDirLocked(tx, p, recursive); err != nil {
			return err
		}
		now := s.now().UnixNano()
		node := &metadata.Node{
			Path: p, Type: metadata.DirectoryNode, Mode: 0o755, CreatedAt: now, ModifiedAt: now,
		}
		if err := tx.PutNode(node); err != nil {
			return err
		}
		info, err = s.infoLocked(tx, node)
		return err
	})
	return info, err
}

// ensureParentDirLocked verifies that a parent exists, optionally creating all
// missing ancestors.
func (s *Service) ensureParentDirLocked(tx *metadata.Tx, p string, recursive bool) error {
	parent := metadata.Parent(p)
	if recursive {
		// Walk from the root to the immediate parent, creating missing dirs.
		parts := strings.Split(strings.TrimPrefix(parent, "/"), "/")
		current := ""
		for _, part := range parts {
			if part == "" {
				continue
			}
			current += "/" + part
			node, err := tx.GetNode(current)
			if errors.Is(err, metadata.ErrNotFound) {
				now := s.now().UnixNano()
				node = &metadata.Node{Path: current, Type: metadata.DirectoryNode, Mode: 0o755, CreatedAt: now, ModifiedAt: now}
				if err := tx.PutNode(node); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if node.Type != metadata.DirectoryNode {
				return fmt.Errorf("%w: %s", ErrNotDirectory, current)
			}
		}
		return nil
	}
	parentNode, err := tx.GetNode(parent)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: parent %s", ErrNotFound, parent)
		}
		return err
	}
	if parentNode.Type != metadata.DirectoryNode {
		return fmt.Errorf("%w: %s", ErrNotDirectory, parent)
	}
	return nil
}

// CreateFile creates an empty regular file. The parent directory must exist.
func (s *Service) CreateFile(_ context.Context, input string, mode uint32) (*FileInfo, error) {
	p, err := metadata.NormalizePath(input)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if p == "/" {
		return nil, fmt.Errorf("%w: cannot create root as a file", ErrInvalid)
	}
	if mode == 0 {
		mode = 0o644
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var info *FileInfo
	err = s.store.Update(func(tx *metadata.Tx) error {
		if _, err := tx.GetNode(p); err == nil {
			return fmt.Errorf("%w: %s", ErrExists, p)
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if err := s.ensureParentDirLocked(tx, p, false); err != nil {
			return err
		}
		now := s.now().UnixNano()
		node := &metadata.Node{Path: p, Type: metadata.FileNode, Mode: mode, CreatedAt: now, ModifiedAt: now, Chunks: []string{}}
		if err := tx.PutNode(node); err != nil {
			return err
		}
		var err error
		info, err = s.infoLocked(tx, node)
		return err
	})
	return info, err
}

// Delete removes a namespace entry. Directories require recursive=true unless
// empty. Chunk metadata becomes orphaned and is physically removed after the
// configured grace period.
func (s *Service) Delete(_ context.Context, input string, recursive bool) error {
	p, err := metadata.NormalizePath(input)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if p == "/" {
		return fmt.Errorf("%w: cannot delete root", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Update(func(tx *metadata.Tx) error {
		node, err := tx.GetNode(p)
		if errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, p)
		}
		if err != nil {
			return err
		}
		nodes := []*metadata.Node{node}
		if node.Type == metadata.DirectoryNode {
			if !recursive {
				count, err := tx.CountChildren(p)
				if err != nil {
					return err
				}
				if count > 0 {
					return fmt.Errorf("%w: %s", ErrNotEmpty, p)
				}
			} else {
				nodes, err = tx.ListSubtree(p)
				if err != nil {
					return err
				}
			}
		}
		now := s.now().UnixNano()
		for _, n := range nodes {
			for _, handle := range n.Chunks {
				chunk, err := tx.GetChunk(handle)
				if errors.Is(err, metadata.ErrNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				chunk.RefCount--
				if chunk.RefCount <= 0 {
					chunk.RefCount = 0
					chunk.OrphanedAt = now
					chunk.Primary = ""
					chunk.LeaseUntil = 0
				}
				if err := tx.PutChunk(chunk); err != nil {
					return err
				}
			}
			if err := tx.DeleteNode(n.Path); err != nil {
				return err
			}
		}
		return nil
	})
}

// Rename atomically moves a file or directory subtree. The destination must
// not exist and cannot be located inside the source subtree.
func (s *Service) Rename(_ context.Context, sourceInput, destinationInput string) error {
	source, err := metadata.NormalizePath(sourceInput)
	if err != nil {
		return fmt.Errorf("%w: source: %v", ErrInvalid, err)
	}
	destination, err := metadata.NormalizePath(destinationInput)
	if err != nil {
		return fmt.Errorf("%w: destination: %v", ErrInvalid, err)
	}
	if source == "/" || destination == "/" {
		return fmt.Errorf("%w: cannot rename root", ErrInvalid)
	}
	if source == destination {
		return nil
	}
	if metadata.IsDescendant(destination, source) {
		return fmt.Errorf("%w: cannot move %s inside itself", ErrInvalid, source)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Update(func(tx *metadata.Tx) error {
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
		type move struct {
			oldPath string
			node    *metadata.Node
		}
		moves := make([]move, 0, len(nodes))
		for _, node := range nodes {
			newPath := destination + strings.TrimPrefix(node.Path, source)
			if existing, err := tx.GetNode(newPath); err == nil && existing.Path != node.Path {
				return fmt.Errorf("%w: %s", ErrExists, newPath)
			} else if err != nil && !errors.Is(err, metadata.ErrNotFound) {
				return err
			}
			copy := *node
			copy.Path = newPath
			copy.ModifiedAt = now
			moves = append(moves, move{oldPath: node.Path, node: &copy})
		}
		for _, m := range moves {
			if err := tx.DeleteNode(m.oldPath); err != nil {
				return err
			}
		}
		for _, m := range moves {
			if err := tx.PutNode(m.node); err != nil {
				return err
			}
		}
		return nil
	})
}
