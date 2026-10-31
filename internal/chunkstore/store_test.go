package chunkstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aayush0325/gofs/internal/logging"
)

// newTestStore creates a chunk store with a small chunk size for tests.
func newTestStore(t *testing.T, chunkSize int64) *Store {
	t.Helper()
	log, err := logging.New("error")
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(t.TempDir(), chunkSize, log)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// TestWriteAppendRead verifies basic mutation, checksum, and read behavior.
func TestWriteAppendRead(t *testing.T) {
	store := newTestStore(t, 1<<20)
	if _, _, err := store.Create("chunk-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Write("chunk-1", 1, 0, []byte("hello"), "m1"); err != nil {
		t.Fatal(err)
	}
	result, version, size, err := store.Append("chunk-1", 1, 5, []byte(" world"), "m2")
	if err != nil {
		t.Fatal(err)
	}
	if result.Offset != 5 || version != 1 || size != 11 {
		t.Fatalf("unexpected append result: %+v version=%d size=%d", result, version, size)
	}
	data, readSize, readVersion, err := store.Read("chunk-1", 1, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" || readSize != 11 || readVersion != 1 {
		t.Fatalf("unexpected read: %q size=%d version=%d", data, readSize, readVersion)
	}
	valid, _, _, corrupt, err := store.Check("chunk-1")
	if err != nil || !valid || len(corrupt) != 0 {
		t.Fatalf("expected valid chunk, valid=%v corrupt=%v err=%v", valid, corrupt, err)
	}
}

// TestMutationIdempotency verifies retries do not duplicate appended records.
func TestMutationIdempotency(t *testing.T) {
	store := newTestStore(t, 1<<20)
	if _, _, err := store.Create("chunk-1", 1); err != nil {
		t.Fatal(err)
	}
	first, _, size, err := store.Append("chunk-1", 1, 0, []byte("once"), "same-id")
	if err != nil {
		t.Fatal(err)
	}
	second, _, size2, err := store.Append("chunk-1", 1, 0, []byte("once"), "same-id")
	if err != nil {
		t.Fatal(err)
	}
	if first.Offset != second.Offset || size != size2 || size != 4 {
		t.Fatalf("duplicate append changed state: first=%+v second=%+v size=%d/%d", first, second, size, size2)
	}
}

// TestVersionFence verifies stale writers are rejected after a lease bump.
func TestVersionFence(t *testing.T) {
	store := newTestStore(t, 1<<20)
	if _, _, err := store.Create("chunk-1", 1); err != nil {
		t.Fatal(err)
	}
	if version, err := store.AdvanceVersion("chunk-1", 3); err != nil || version != 3 {
		t.Fatalf("advance version: version=%d err=%v", version, err)
	}
	if _, _, _, err := store.Write("chunk-1", 2, 0, []byte("old"), "m-old"); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("expected stale version, got %v", err)
	}
	if _, _, _, err := store.Write("chunk-1", 3, 0, []byte("new"), "m-new"); err != nil {
		t.Fatal(err)
	}
}

// TestCopyLocal verifies copy-on-write source copies preserve bytes.
func TestCopyLocal(t *testing.T) {
	store := newTestStore(t, 1<<20)
	if _, _, err := store.Create("source", 2); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Write("source", 2, 0, []byte("shared"), "m1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CopyLocal("source", "target", 3); err != nil {
		t.Fatal(err)
	}
	data, _, version, err := store.Read("target", 3, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "shared" || version != 3 {
		t.Fatalf("unexpected copy: %q version=%d", data, version)
	}
}

// TestIngest verifies stream-based chunk creation and checksum calculation.
func TestIngest(t *testing.T) {
	store := newTestStore(t, 1<<20)
	payload := bytes.Repeat([]byte("x"), BlockSize+17)
	if _, _, err := store.Ingest("streamed", 1, int64(len(payload)), func(w io.Writer) (int64, error) {
		return io.Copy(w, strings.NewReader(string(payload)))
	}); err != nil {
		t.Fatal(err)
	}
	data, _, _, err := store.Read("streamed", 1, 0, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("ingested payload mismatch")
	}
}

// TestCorruptionDetection verifies a changed data byte is reported as corrupt.
func TestCorruptionDetection(t *testing.T) {
	store := newTestStore(t, 1<<20)
	if _, _, err := store.Create("chunk-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Write("chunk-1", 1, 0, []byte("hello"), "m1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.Root(), "chunk-1.data"), []byte("hELLO"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Read("chunk-1", 1, 0, 5); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected corruption error, got %v", err)
	}
	valid, _, _, corrupt, err := store.Check("chunk-1")
	if err != nil {
		t.Fatal(err)
	}
	if valid || len(corrupt) != 1 {
		t.Fatalf("expected one corrupt block, valid=%v corrupt=%v", valid, corrupt)
	}
}

// TestAppendFullAndHoles verifies chunk capacity and hole-free write rules.
func TestAppendFullAndHoles(t *testing.T) {
	store := newTestStore(t, 8)
	if _, _, err := store.Create("chunk-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Write("chunk-1", 1, 0, []byte("123456"), "w1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Write("chunk-1", 1, 8, []byte("x"), "w2"); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("expected hole rejection, got %v", err)
	}
	if _, _, _, err := store.Append("chunk-1", 1, 6, []byte("789"), "a1"); !errors.Is(err, ErrChunkFull) {
		t.Fatalf("expected chunk full, got %v", err)
	}
}
