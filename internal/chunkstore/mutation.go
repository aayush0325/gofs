package chunkstore

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Write applies an overwrite at an explicit offset. A mutation ID makes retries
// idempotent. Writes may extend the chunk only at its current end.
func (s *Store) Write(handle string, version uint64, offset int64, data []byte, mutationID string) (MutationResult, uint64, int64, error) {
	return s.apply(handle, version, offset, data, mutationID, false)
}

// Append applies a record append. appendOffset is chosen by the primary and
// must equal the local chunk size so all replicas agree on record placement.
func (s *Store) Append(handle string, version uint64, appendOffset int64, data []byte, mutationID string) (MutationResult, uint64, int64, error) {
	return s.apply(handle, version, appendOffset, data, mutationID, true)
}

// apply performs a write or append under the chunk lock.
func (s *Store) apply(handle string, version uint64, offset int64, data []byte, mutationID string, appendMode bool) (MutationResult, uint64, int64, error) {
	if err := validateHandle(handle); err != nil {
		return MutationResult{}, 0, 0, err
	}
	unlock := s.locks.Lock(handle)
	defer unlock()

	s.mu.RLock()
	current := s.chunks[handle]
	s.mu.RUnlock()
	if current == nil {
		return MutationResult{}, 0, 0, ErrNotFound
	}
	meta := cloneMeta(current)
	if mutationID != "" {
		if result, ok := meta.Mutations[mutationID]; ok {
			return result, meta.Version, meta.Size, nil
		}
	}
	if version < meta.Version {
		return MutationResult{}, 0, 0, fmt.Errorf("%w: request=%d local=%d", ErrStaleVersion, version, meta.Version)
	}
	if len(data) == 0 {
		return MutationResult{Offset: offset, Length: 0}, meta.Version, meta.Size, nil
	}
	if appendMode {
		if offset != meta.Size {
			return MutationResult{}, 0, 0, fmt.Errorf("%w: append offset %d, local size %d", ErrInvalidOffset, offset, meta.Size)
		}
	} else if offset > meta.Size {
		return MutationResult{}, 0, 0, fmt.Errorf("%w: write offset %d beyond size %d", ErrInvalidOffset, offset, meta.Size)
	}
	if offset < 0 || int64(len(data)) > s.chunkSize || offset+int64(len(data)) > s.chunkSize {
		return MutationResult{}, 0, 0, fmt.Errorf("%w: write [%d,%d) exceeds chunk size %d", ErrChunkFull, offset, offset+int64(len(data)), s.chunkSize)
	}

	file, err := os.OpenFile(s.dataPath(handle), os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return MutationResult{}, 0, 0, fmt.Errorf("open chunk data: %w", err)
	}
	if _, err := file.WriteAt(data, offset); err != nil {
		_ = file.Close()
		return MutationResult{}, 0, 0, fmt.Errorf("write chunk data: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return MutationResult{}, 0, 0, fmt.Errorf("sync chunk data: %w", err)
	}
	if err := file.Close(); err != nil {
		return MutationResult{}, 0, 0, err
	}

	newSize := meta.Size
	if offset+int64(len(data)) > newSize {
		newSize = offset + int64(len(data))
	}
	meta.Size = newSize
	meta.Version = version
	if err := s.rehashBlocks(&meta, offset, int64(len(data))); err != nil {
		return MutationResult{}, 0, 0, err
	}
	result := MutationResult{Offset: offset, Length: int64(len(data))}
	if mutationID != "" {
		if meta.Mutations == nil {
			meta.Mutations = make(map[string]MutationResult)
		}
		pruneMutations(meta.Mutations)
		meta.Mutations[mutationID] = result
	}
	if err := s.persist(&meta); err != nil {
		return MutationResult{}, 0, 0, err
	}
	s.mu.Lock()
	s.chunks[handle] = &meta
	s.mu.Unlock()
	return result, meta.Version, meta.Size, nil
}

// rehashBlocks updates checksums for all blocks touched by a write.
func (s *Store) rehashBlocks(meta *Meta, offset, length int64) error {
	if length == 0 {
		return nil
	}
	file, err := os.Open(s.dataPath(meta.Handle))
	if err != nil {
		return err
	}
	defer file.Close()
	first := offset / BlockSize
	last := (offset + length - 1) / BlockSize
	for block := first; block <= last; block++ {
		start := block * BlockSize
		n := minInt64(BlockSize, meta.Size-start)
		if n <= 0 {
			break
		}
		buf := make([]byte, n)
		if _, err := file.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for int64(len(meta.Blocks)) <= block {
			meta.Blocks = append(meta.Blocks, 0)
		}
		meta.Blocks[block] = crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli))
	}
	return nil
}
