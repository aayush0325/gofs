package chunkstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// computeBlocksFromFile computes all block checksums for a completed data file.
func (s *Store) computeBlocksFromFile(filename string, meta *Meta) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	meta.Blocks = make([]uint32, 0, (meta.Size+BlockSize-1)/BlockSize)
	for offset := int64(0); offset < meta.Size; offset += BlockSize {
		n := minInt64(BlockSize, meta.Size-offset)
		buf := make([]byte, n)
		if _, err := file.ReadAt(buf, offset); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		meta.Blocks = append(meta.Blocks, crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli)))
	}
	return nil
}

// persist atomically replaces a chunk's JSON sidecar.
func (s *Store) persist(meta *Meta) error {
	if err := validateHandle(meta.Handle); err != nil {
		return err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.root, meta.Handle+".meta.*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, s.metaPath(meta.Handle)); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

// loadMeta decodes a sidecar and fills in defaults for older files.
func (s *Store) loadMeta(filename string) (*Meta, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var meta Meta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	if err := validateHandle(meta.Handle); err != nil {
		return nil, err
	}
	if meta.Mutations == nil {
		meta.Mutations = make(map[string]MutationResult)
	}
	if meta.Size > s.chunkSize {
		return nil, fmt.Errorf("chunk size %d exceeds limit %d", meta.Size, s.chunkSize)
	}
	return &meta, nil
}

// dataPath returns the data file path for a chunk handle.
func (s *Store) dataPath(handle string) string { return filepath.Join(s.root, handle+".data") }

// metaPath returns the checksum sidecar path for a chunk handle.
func (s *Store) metaPath(handle string) string { return filepath.Join(s.root, handle+".meta") }

// validateHandle prevents path traversal and accidental special files.
func validateHandle(handle string) error {
	if !handlePattern.MatchString(handle) {
		return fmt.Errorf("invalid chunk handle %q", handle)
	}
	return nil
}

// cloneMeta deep-copies mutable slices and maps before a transaction.
func cloneMeta(meta *Meta) Meta {
	copy := *meta
	copy.Blocks = append([]uint32(nil), meta.Blocks...)
	copy.Mutations = make(map[string]MutationResult, len(meta.Mutations))
	for k, v := range meta.Mutations {
		copy.Mutations[k] = v
	}
	return copy
}

// pruneMutations keeps the persisted idempotency cache bounded.
func pruneMutations(m map[string]MutationResult) {
	for len(m) >= maxMutations {
		for key := range m {
			delete(m, key)
			break
		}
	}
}

// minInt64 returns the smaller of two int64 values.
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
