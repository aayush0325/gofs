package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

// openTestStore creates a temporary BoltDB metadata store.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "master.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestNodeAndChunkPersistence verifies the basic typed BoltDB helpers.
func TestNodeAndChunkPersistence(t *testing.T) {
	store := openTestStore(t)
	now := time.Now().UnixNano()
	err := store.Update(func(tx *Tx) error {
		if err := tx.PutNode(&Node{Path: "/", Type: DirectoryNode, Mode: 0o755, CreatedAt: now, ModifiedAt: now}); err != nil {
			return err
		}
		if err := tx.PutNode(&Node{Path: "/file", Type: FileNode, Mode: 0o644, CreatedAt: now, ModifiedAt: now, Chunks: []string{"c1"}}); err != nil {
			return err
		}
		return tx.PutChunk(&Chunk{Handle: "c1", Version: 2, Size: 7, RefCount: 1, Replicas: map[string]Replica{}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.View(func(tx *Tx) error {
		node, err := tx.GetNode("/file")
		if err != nil {
			return err
		}
		if len(node.Chunks) != 1 {
			t.Fatalf("unexpected chunks %v", node.Chunks)
		}
		chunk, err := tx.GetChunk("c1")
		if err != nil {
			return err
		}
		if chunk.Version != 2 || chunk.Size != 7 {
			t.Fatalf("unexpected chunk %+v", chunk)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestListChildrenAndSubtree verifies prefix-safe directory traversal.
func TestListChildrenAndSubtree(t *testing.T) {
	store := openTestStore(t)
	now := time.Now().UnixNano()
	err := store.Update(func(tx *Tx) error {
		for _, p := range []string{"/", "/a", "/a/b", "/a/b/c", "/ab"} {
			kind := DirectoryNode
			if p == "/a/b/c" {
				kind = FileNode
			}
			if err := tx.PutNode(&Node{Path: p, Type: kind, CreatedAt: now, ModifiedAt: now}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.View(func(tx *Tx) error {
		children, err := tx.ListChildren("/a")
		if err != nil {
			return err
		}
		if len(children) != 1 || children[0].Path != "/a/b" {
			t.Fatalf("unexpected children: %+v", children)
		}
		subtree, err := tx.ListSubtree("/a")
		if err != nil {
			return err
		}
		if len(subtree) != 3 {
			t.Fatalf("expected 3 subtree entries, got %d", len(subtree))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestServerPersistence verifies chunkserver registration records.
func TestServerPersistence(t *testing.T) {
	store := openTestStore(t)
	if err := store.Update(func(tx *Tx) error {
		return tx.PutServer(&Server{ID: "cs1", Address: "localhost:9091", Capacity: 100, LastSeen: 1})
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.View(func(tx *Tx) error {
		server, err := tx.GetServer("cs1")
		if err != nil {
			return err
		}
		if server.Address != "localhost:9091" {
			t.Fatalf("unexpected server %+v", server)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
