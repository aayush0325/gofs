package chunkstore

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Read returns a bounded range and verifies every block touched by the range.
// expectedVersion may be zero to skip the version fence; otherwise a request
// older than the local version is rejected.
func (s *Store) Read(handle string, expectedVersion uint64, offset, length int64) ([]byte, int64, uint64, error) {
	unlock := s.locks.Lock(handle)
	defer unlock()

	s.mu.RLock()
	meta := s.chunks[handle]
	s.mu.RUnlock()
	if meta == nil {
		return nil, 0, 0, ErrNotFound
	}
	if expectedVersion > 0 && expectedVersion < meta.Version {
		return nil, 0, 0, fmt.Errorf("%w: request=%d local=%d", ErrStaleVersion, expectedVersion, meta.Version)
	}
	if offset < 0 || length < 0 {
		return nil, 0, 0, ErrInvalidOffset
	}
	if offset > meta.Size {
		return nil, 0, 0, ErrInvalidOffset
	}
	if length == 0 {
		return []byte{}, meta.Size, meta.Version, nil
	}
	if offset+length > meta.Size {
		length = meta.Size - offset
	}
	if length == 0 {
		return []byte{}, meta.Size, meta.Version, nil
	}
	if int64(len(meta.Blocks)) != (meta.Size+BlockSize-1)/BlockSize {
		return nil, 0, 0, fmt.Errorf("%w: checksum sidecar is incomplete", ErrCorrupt)
	}
	if err := s.verifyRangeLocked(meta, offset, length); err != nil {
		return nil, 0, 0, err
	}
	file, err := os.Open(s.dataPath(handle))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("open chunk data: %w", err)
	}
	defer file.Close()
	data := make([]byte, length)
	if _, err := file.ReadAt(data, offset); err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, 0, fmt.Errorf("read chunk data: %w", err)
	}
	return data, meta.Size, meta.Version, nil
}

// Check verifies the data file length and every block checksum.
func (s *Store) Check(handle string) (bool, int64, uint64, []int64, error) {
	unlock := s.locks.Lock(handle)
	defer unlock()

	s.mu.RLock()
	meta := s.chunks[handle]
	s.mu.RUnlock()
	if meta == nil {
		return false, 0, 0, nil, ErrNotFound
	}
	info, err := os.Stat(s.dataPath(handle))
	if err != nil {
		return false, 0, meta.Version, []int64{0}, fmt.Errorf("stat chunk data: %w", err)
	}
	if info.Size() < meta.Size {
		return false, meta.Size, meta.Version, []int64{info.Size()}, nil
	}
	if int64(len(meta.Blocks)) != (meta.Size+BlockSize-1)/BlockSize {
		return false, meta.Size, meta.Version, []int64{0}, nil
	}
	file, err := os.Open(s.dataPath(handle))
	if err != nil {
		return false, meta.Size, meta.Version, nil, err
	}
	defer file.Close()
	var corrupt []int64
	for block := 0; block < len(meta.Blocks); block++ {
		start := int64(block) * BlockSize
		n := minInt64(BlockSize, meta.Size-start)
		if n <= 0 {
			break
		}
		buf := make([]byte, n)
		if _, err := file.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
			return false, meta.Size, meta.Version, nil, err
		}
		if crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli)) != meta.Blocks[block] {
			corrupt = append(corrupt, start)
		}
	}
	return len(corrupt) == 0, meta.Size, meta.Version, corrupt, nil
}

// verifyRangeLocked verifies every checksum block overlapped by a read.
func (s *Store) verifyRangeLocked(meta *Meta, offset, length int64) error {
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
		if block >= int64(len(meta.Blocks)) || crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli)) != meta.Blocks[block] {
			return fmt.Errorf("%w: block at offset %d", ErrCorrupt, start)
		}
	}
	return nil
}
