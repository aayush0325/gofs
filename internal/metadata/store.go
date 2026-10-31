package metadata

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	nodesBucket   = []byte("nodes")
	chunksBucket  = []byte("chunks")
	serversBucket = []byte("servers")
	metaBucket    = []byte("meta")
	schemaKey     = []byte("schema")
	schemaVersion = []byte("1")
)

// ErrNotFound is returned when a namespace entry or chunk does not exist.
var ErrNotFound = errors.New("not found")

// Store persists the master's namespace, chunk, and server metadata in a
// single BoltDB file. Bolt's write transactions make multi-entry operations
// such as rename and snapshot atomic.
type Store struct {
	db *bolt.DB
}

// Open opens (or creates) a metadata database. The parent directory is created
// with restrictive permissions if it does not exist.
func Open(filename string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0o750); err != nil {
		return nil, fmt.Errorf("create metadata directory: %w", err)
	}
	db, err := bolt.Open(filename, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open metadata database: %w", err)
	}
	store := &Store{db: db}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{nodesBucket, chunksBucket, serversBucket, metaBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		meta := tx.Bucket(metaBucket)
		current := meta.Get(schemaKey)
		if current != nil && !bytes.Equal(current, schemaVersion) {
			return fmt.Errorf("unsupported metadata schema %q", string(current))
		}
		if err := meta.Put(schemaKey, schemaVersion); err != nil {
			return err
		}
		nodes := tx.Bucket(nodesBucket)
		if nodes.Get([]byte("/")) == nil {
			now := time.Now().UnixNano()
			root := &Node{Path: "/", Type: DirectoryNode, Mode: 0o755, CreatedAt: now, ModifiedAt: now}
			data, err := EncodeNode(root)
			if err != nil {
				return err
			}
			if err := nodes.Put([]byte("/"), data); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize metadata database: %w", err)
	}
	return store, nil
}

// Close flushes and closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// View runs a read-only transaction.
func (s *Store) View(fn func(*Tx) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// Update runs an atomic read-write transaction.
func (s *Store) Update(fn func(*Tx) error) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// Tx is a typed wrapper around a Bolt transaction.
type Tx struct {
	tx *bolt.Tx
}

// GetNode looks up a namespace entry. It returns ErrNotFound when absent.
func (t *Tx) GetNode(p string) (*Node, error) {
	data := t.tx.Bucket(nodesBucket).Get([]byte(p))
	if data == nil {
		return nil, ErrNotFound
	}
	return DecodeNode(data)
}

// PutNode inserts or replaces a namespace entry.
func (t *Tx) PutNode(n *Node) error {
	data, err := EncodeNode(n)
	if err != nil {
		return err
	}
	return t.tx.Bucket(nodesBucket).Put([]byte(n.Path), data)
}

// DeleteNode removes a namespace entry. Deleting a missing entry succeeds.
func (t *Tx) DeleteNode(p string) error {
	return t.tx.Bucket(nodesBucket).Delete([]byte(p))
}

// ListChildren returns the direct children of a directory, sorted by BoltDB's
// byte ordering. It does not include the directory itself.
func (t *Tx) ListChildren(dir string) ([]*Node, error) {
	prefix := []byte(ChildPrefix(dir))
	var nodes []*Node
	err := t.tx.Bucket(nodesBucket).ForEach(func(k, v []byte) error {
		key := string(k)
		if key == dir || !strings.HasPrefix(key, string(prefix)) {
			return nil
		}
		rest := strings.TrimPrefix(key, string(prefix))
		if rest == "" || strings.Contains(rest, "/") {
			return nil
		}
		n, err := DecodeNode(v)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
		return nil
	})
	return nodes, err
}

// ListSubtree returns a directory and every descendant. The result is sorted
// by path, with the root of the subtree first.
func (t *Tx) ListSubtree(root string) ([]*Node, error) {
	var nodes []*Node
	rootNode, err := t.GetNode(root)
	if err == nil {
		nodes = append(nodes, rootNode)
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	prefix := []byte(ChildPrefix(root))
	bucket := t.tx.Bucket(nodesBucket)
	cursor := bucket.Cursor()
	for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
		n, err := DecodeNode(v)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// CountChildren returns the number of direct children of a directory.
func (t *Tx) CountChildren(dir string) (int, error) {
	nodes, err := t.ListChildren(dir)
	if err != nil {
		return 0, err
	}
	return len(nodes), nil
}

// GetChunk looks up chunk metadata. It returns ErrNotFound when absent.
func (t *Tx) GetChunk(handle string) (*Chunk, error) {
	data := t.tx.Bucket(chunksBucket).Get([]byte(handle))
	if data == nil {
		return nil, ErrNotFound
	}
	return DecodeChunk(data)
}

// PutChunk inserts or replaces chunk metadata.
func (t *Tx) PutChunk(c *Chunk) error {
	data, err := EncodeChunk(c)
	if err != nil {
		return err
	}
	return t.tx.Bucket(chunksBucket).Put([]byte(c.Handle), data)
}

// DeleteChunk removes chunk metadata.
func (t *Tx) DeleteChunk(handle string) error {
	return t.tx.Bucket(chunksBucket).Delete([]byte(handle))
}

// ListChunks returns every chunk in handle order.
func (t *Tx) ListChunks() ([]*Chunk, error) {
	var chunks []*Chunk
	err := t.tx.Bucket(chunksBucket).ForEach(func(_, v []byte) error {
		c, err := DecodeChunk(v)
		if err != nil {
			return err
		}
		chunks = append(chunks, c)
		return nil
	})
	return chunks, err
}

// GetServer looks up a chunkserver record.
func (t *Tx) GetServer(id string) (*Server, error) {
	data := t.tx.Bucket(serversBucket).Get([]byte(id))
	if data == nil {
		return nil, ErrNotFound
	}
	return DecodeServer(data)
}

// PutServer inserts or replaces a chunkserver record.
func (t *Tx) PutServer(s *Server) error {
	data, err := EncodeServer(s)
	if err != nil {
		return err
	}
	return t.tx.Bucket(serversBucket).Put([]byte(s.ID), data)
}

// DeleteServer removes a chunkserver record.
func (t *Tx) DeleteServer(id string) error {
	return t.tx.Bucket(serversBucket).Delete([]byte(id))
}

// ListServers returns every registered chunkserver in ID order.
func (t *Tx) ListServers() ([]*Server, error) {
	var servers []*Server
	err := t.tx.Bucket(serversBucket).ForEach(func(_, v []byte) error {
		s, err := DecodeServer(v)
		if err != nil {
			return err
		}
		servers = append(servers, s)
		return nil
	})
	return servers, err
}

// CountNodes returns the number of files and directories.
func (t *Tx) CountNodes() (int, int, error) {
	var files, dirs int
	err := t.tx.Bucket(nodesBucket).ForEach(func(_, v []byte) error {
		n, err := DecodeNode(v)
		if err != nil {
			return err
		}
		if n.Type == FileNode {
			files++
		} else if n.Type == DirectoryNode {
			dirs++
		}
		return nil
	})
	return files, dirs, err
}
