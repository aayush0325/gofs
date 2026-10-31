// Package chunkstore implements the on-disk chunk format used by chunkservers:
// a data file plus an atomic JSON sidecar containing version, size, block
// checksums, and recent mutation IDs for idempotent retries.
package chunkstore

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/aayush0325/gofs/internal/keyedmutex"
)

const (
	// BlockSize is the granularity of checksum verification.
	BlockSize = 64 * 1024
	// maxMutations bounds the persisted idempotency cache per chunk.
	maxMutations = 128
)

var (
	// ErrNotFound means the chunk handle is unknown.
	ErrNotFound = errors.New("chunk not found")
	// ErrStaleVersion means a request used an older version than the local copy.
	ErrStaleVersion = errors.New("stale chunk version")
	// ErrChunkFull means an append would exceed the configured chunk size.
	ErrChunkFull = errors.New("chunk is full")
	// ErrInvalidOffset means a write would leave a hole or extend past the chunk.
	ErrInvalidOffset = errors.New("invalid chunk offset")
	// ErrCorrupt means a block checksum did not match the data file.
	ErrCorrupt = errors.New("chunk checksum mismatch")
)

var handlePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// MutationResult is the cached outcome of a mutation, used to make retries
// idempotent. Offset is the record offset for appends and the write offset for
// writes; Length is the number of bytes applied.
type MutationResult struct {
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

// Meta is the JSON sidecar for a chunk.
type Meta struct {
	Handle    string                    `json:"handle"`
	Version   uint64                    `json:"version"`
	Size      int64                     `json:"size"`
	Blocks    []uint32                  `json:"blocks"`
	Mutations map[string]MutationResult `json:"mutations,omitempty"`
}

// Store manages the chunk files below one root directory.
type Store struct {
	root      string
	chunkSize int64
	log       *slog.Logger

	mu     sync.RWMutex
	chunks map[string]*Meta
	locks  *keyedmutex.Map
}

// Open scans the chunk directory and loads every valid metadata sidecar. Chunks
// whose data file is missing are ignored with a warning.
func Open(root string, chunkSize int64, log *slog.Logger) (*Store, error) {
	if chunkSize <= 0 {
		return nil, errors.New("chunk size must be positive")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create chunk directory: %w", err)
	}
	s := &Store{
		root:      root,
		chunkSize: chunkSize,
		log:       log,
		chunks:    make(map[string]*Meta),
		locks:     keyedmutex.New(),
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("scan chunk directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".meta") {
			continue
		}
		meta, err := s.loadMeta(filepath.Join(root, entry.Name()))
		if err != nil {
			log.Warn("ignoring invalid chunk metadata", "file", entry.Name(), "error", err)
			continue
		}
		if _, err := os.Stat(s.dataPath(meta.Handle)); err != nil {
			log.Warn("ignoring chunk without data file", "handle", meta.Handle, "error", err)
			continue
		}
		s.chunks[meta.Handle] = meta
	}
	log.Info("loaded chunk store", "root", root, "chunks", len(s.chunks), "chunk_size", chunkSize)
	return s, nil
}

// Close releases any resources held by the store. Chunk mutations are synced
// to disk before returning, so closing currently has no additional work.
func (s *Store) Close() error { return nil }

// Root returns the directory containing chunk files.
func (s *Store) Root() string { return s.root }

// ChunkSize returns the configured maximum chunk size.
func (s *Store) ChunkSize() int64 { return s.chunkSize }

// UsedBytes returns the sum of all chunk sizes.
func (s *Store) UsedBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, meta := range s.chunks {
		total += meta.Size
	}
	return total
}

// States returns a snapshot of every local chunk for registration/heartbeat.
func (s *Store) States() []ChunkState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	states := make([]ChunkState, 0, len(s.chunks))
	for _, meta := range s.chunks {
		states = append(states, ChunkState{Handle: meta.Handle, Version: meta.Version, Size: meta.Size})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Handle < states[j].Handle })
	return states
}

// ChunkState is the public view of one local chunk.
type ChunkState struct {
	Handle  string
	Version uint64
	Size    int64
}

// Has reports whether a chunk exists locally.
func (s *Store) Has(handle string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.chunks[handle]
	return ok
}

// Size returns the local chunk size.
func (s *Store) Size(handle string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.chunks[handle]
	if !ok {
		return 0, ErrNotFound
	}
	return meta.Size, nil
}

// Version returns the local chunk version.
func (s *Store) Version(handle string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.chunks[handle]
	if !ok {
		return 0, ErrNotFound
	}
	return meta.Version, nil
}

// Mutation returns a cached mutation result, if any.
func (s *Store) Mutation(handle, mutationID string) (MutationResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.chunks[handle]
	if !ok || mutationID == "" {
		return MutationResult{}, false
	}
	result, ok := meta.Mutations[mutationID]
	return result, ok
}
