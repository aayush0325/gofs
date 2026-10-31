package chunkstore

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// Create creates an empty chunk. The operation is idempotent for an existing
// handle: a higher version updates the local version and returns success.
func (s *Store) Create(handle string, version uint64) (int64, uint64, error) {
	if err := validateHandle(handle); err != nil {
		return 0, 0, err
	}
	unlock := s.locks.Lock(handle)
	defer unlock()

	s.mu.RLock()
	existing := s.chunks[handle]
	s.mu.RUnlock()
	if existing != nil {
		if version > existing.Version {
			copy := *existing
			copy.Version = version
			if err := s.persist(&copy); err != nil {
				return 0, 0, err
			}
			s.mu.Lock()
			s.chunks[handle] = &copy
			s.mu.Unlock()
			return copy.Size, copy.Version, nil
		}
		return existing.Size, existing.Version, nil
	}

	file, err := os.OpenFile(s.dataPath(handle), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return 0, 0, fmt.Errorf("create chunk data: %w", err)
	}
	if err := file.Close(); err != nil {
		return 0, 0, err
	}
	meta := &Meta{Handle: handle, Version: version, Size: 0, Blocks: []uint32{}, Mutations: map[string]MutationResult{}}
	if err := s.persist(meta); err != nil {
		_ = os.Remove(s.dataPath(handle))
		return 0, 0, err
	}
	s.mu.Lock()
	s.chunks[handle] = meta
	s.mu.Unlock()
	return 0, version, nil
}

// AdvanceVersion fences an existing chunk at a newer version without changing
// its data. It is called when the master grants a new lease.
func (s *Store) AdvanceVersion(handle string, version uint64) (uint64, error) {
	if err := validateHandle(handle); err != nil {
		return 0, err
	}
	unlock := s.locks.Lock(handle)
	defer unlock()

	s.mu.RLock()
	current := s.chunks[handle]
	s.mu.RUnlock()
	if current == nil {
		return 0, ErrNotFound
	}
	if version <= current.Version {
		return current.Version, nil
	}
	copy := cloneMeta(current)
	copy.Version = version
	if err := s.persist(&copy); err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.chunks[handle] = &copy
	s.mu.Unlock()
	return version, nil
}

// Delete removes a chunk and its sidecar. Missing chunks are treated as
// success, which makes garbage-collection retries safe.
func (s *Store) Delete(handle string) error {
	if err := validateHandle(handle); err != nil {
		return err
	}
	unlock := s.locks.Lock(handle)
	defer unlock()

	s.mu.Lock()
	delete(s.chunks, handle)
	s.mu.Unlock()
	errData := os.Remove(s.dataPath(handle))
	errMeta := os.Remove(s.metaPath(handle))
	if errData != nil && !errors.Is(errData, os.ErrNotExist) {
		return errData
	}
	if errMeta != nil && !errors.Is(errMeta, os.ErrNotExist) {
		return errMeta
	}
	return nil
}

// CopyLocal copies a local source chunk to a new local handle. Both chunk locks
// are acquired in lexical order to avoid deadlocks.
func (s *Store) CopyLocal(sourceHandle, targetHandle string, version uint64) (int64, uint64, error) {
	if sourceHandle == targetHandle {
		return 0, 0, errors.New("source and target handles must differ")
	}
	first, second := sourceHandle, targetHandle
	if second < first {
		first, second = second, first
	}
	unlockFirst := s.locks.Lock(first)
	defer unlockFirst()
	unlockSecond := s.locks.Lock(second)
	defer unlockSecond()

	s.mu.RLock()
	source := s.chunks[sourceHandle]
	s.mu.RUnlock()
	if source == nil {
		return 0, 0, ErrNotFound
	}
	file, err := os.Open(s.dataPath(sourceHandle))
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	meta, err := s.ingestLocked(targetHandle, version, source.Size, func(w io.Writer) (int64, error) {
		n, err := io.Copy(w, io.LimitReader(file, source.Size))
		return n, err
	})
	if err != nil {
		return 0, 0, err
	}
	return meta.Size, meta.Version, nil
}

// Ingest creates a chunk from a stream. The callback must write exactly size
// bytes. Checksums are computed locally after the data is written.
func (s *Store) Ingest(handle string, version uint64, size int64, fn func(io.Writer) (int64, error)) (int64, uint64, error) {
	if err := validateHandle(handle); err != nil {
		return 0, 0, err
	}
	unlock := s.locks.Lock(handle)
	defer unlock()
	meta, err := s.ingestLocked(handle, version, size, fn)
	if err != nil {
		return 0, 0, err
	}
	return meta.Size, meta.Version, nil
}

// ingestLocked creates a new chunk file from fn while already holding the
// target chunk lock.
func (s *Store) ingestLocked(handle string, version uint64, size int64, fn func(io.Writer) (int64, error)) (*Meta, error) {
	if size < 0 || size > s.chunkSize {
		return nil, fmt.Errorf("invalid chunk size %d", size)
	}
	s.mu.RLock()
	existing := s.chunks[handle]
	s.mu.RUnlock()
	if existing != nil {
		return nil, fmt.Errorf("chunk %s already exists", handle)
	}

	tmp, err := os.CreateTemp(s.root, handle+".data.*")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	written, err := fn(tmp)
	if err != nil {
		cleanup()
		return nil, err
	}
	if written != size {
		cleanup()
		return nil, fmt.Errorf("ingest size mismatch: wrote %d, expected %d", written, size)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	meta := &Meta{Handle: handle, Version: version, Size: size, Mutations: map[string]MutationResult{}}
	if err := s.computeBlocksFromFile(tmpName, meta); err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	if err := os.Rename(tmpName, s.dataPath(handle)); err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	if err := s.persist(meta); err != nil {
		_ = os.Remove(s.dataPath(handle))
		return nil, err
	}
	s.mu.Lock()
	s.chunks[handle] = meta
	s.mu.Unlock()
	return meta, nil
}
