// Package metadata contains the persisted GFS namespace and chunk metadata.
package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
)

// NodeType is the persisted type of a namespace entry.
type NodeType string

const (
	// FileNode represents a regular file.
	FileNode NodeType = "file"
	// DirectoryNode represents a directory.
	DirectoryNode NodeType = "directory"
)

// Node is one entry in the flat namespace tree. The full path is the database
// key. Files hold an ordered list of chunk handles.
type Node struct {
	Path       string   `json:"path"`
	Type       NodeType `json:"type"`
	Mode       uint32   `json:"mode"`
	CreatedAt  int64    `json:"created_at_unix_nano"`
	ModifiedAt int64    `json:"modified_at_unix_nano"`
	Chunks     []string `json:"chunks,omitempty"`
	SnapshotOf string   `json:"snapshot_of,omitempty"`
}

// Replica is a chunkserver placement recorded by the master.
type Replica struct {
	ServerID string `json:"server_id"`
	Address  string `json:"address"`
}

// Chunk is the master's metadata for one chunk. RefCount counts namespace
// references. A value greater than one means a snapshot shares the chunk and
// the next mutation must copy it first (copy-on-write).
type Chunk struct {
	Handle     string             `json:"handle"`
	Version    uint64             `json:"version"`
	Size       int64              `json:"size"`
	RefCount   int                `json:"ref_count"`
	Replicas   map[string]Replica `json:"replicas"`
	Primary    string             `json:"primary,omitempty"`
	LeaseUntil int64              `json:"lease_until_unix_nano,omitempty"`
	OrphanedAt int64              `json:"orphaned_at_unix_nano,omitempty"`
}

// Server is the master's record of one chunkserver.
type Server struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Capacity  int64  `json:"capacity_bytes"`
	Used      int64  `json:"used_bytes"`
	LastSeen  int64  `json:"last_seen_unix_nano"`
	FirstSeen int64  `json:"first_seen_unix_nano"`
}

// NormalizePath validates and canonicalizes an absolute GFS path. It rejects
// relative paths, NUL bytes, and paths that escape the root after cleaning.
func NormalizePath(p string) (string, error) {
	if p == "" {
		return "", errors.New("path is required")
	}
	if strings.ContainsRune(p, '\x00') {
		return "", errors.New("path contains NUL byte")
	}
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("path %q must be absolute", p)
	}
	clean := path.Clean(p)
	if clean != "/" && strings.HasSuffix(p, "/") {
		// path.Clean already removed the suffix; this branch documents that
		// trailing slashes are accepted and canonicalized.
		clean = path.Clean(clean)
	}
	if strings.HasPrefix(clean, "/..") {
		return "", errors.New("path escapes root")
	}
	return clean, nil
}

// Parent returns the parent of a canonical absolute path. The root has no
// parent and is returned as the empty string.
func Parent(p string) string {
	if p == "/" {
		return ""
	}
	parent := path.Dir(p)
	if parent == "." {
		return "/"
	}
	return parent
}

// Base returns the final component of a canonical path.
func Base(p string) string {
	if p == "/" {
		return "/"
	}
	return path.Base(p)
}

// IsDescendant reports whether p is strictly below ancestor.
func IsDescendant(p, ancestor string) bool {
	if ancestor == "/" {
		return p != "/" && strings.HasPrefix(p, "/")
	}
	return strings.HasPrefix(p, ancestor+"/")
}

// ChildPrefix returns the key prefix shared by all direct and indirect
// children of a directory.
func ChildPrefix(dir string) string {
	if dir == "/" {
		return "/"
	}
	return dir + "/"
}

// Encode serializes a node for BoltDB.
func EncodeNode(n *Node) ([]byte, error) {
	return json.Marshal(n)
}

// DecodeNode deserializes a node from BoltDB.
func DecodeNode(data []byte) (*Node, error) {
	var n Node
	if err := json.Unmarshal(data, &n); err != nil {
		return nil, fmt.Errorf("decode node: %w", err)
	}
	return &n, nil
}

// EncodeChunk serializes chunk metadata for BoltDB.
func EncodeChunk(c *Chunk) ([]byte, error) {
	return json.Marshal(c)
}

// DecodeChunk deserializes chunk metadata from BoltDB.
func DecodeChunk(data []byte) (*Chunk, error) {
	var c Chunk
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decode chunk: %w", err)
	}
	if c.Replicas == nil {
		c.Replicas = make(map[string]Replica)
	}
	return &c, nil
}

// EncodeServer serializes a chunkserver record.
func EncodeServer(s *Server) ([]byte, error) {
	return json.Marshal(s)
}

// DecodeServer deserializes a chunkserver record.
func DecodeServer(data []byte) (*Server, error) {
	var s Server
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("decode server: %w", err)
	}
	return &s, nil
}
