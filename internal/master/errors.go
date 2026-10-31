// Package master implements the single-master metadata authority, leases,
// chunk placement, snapshots, replication repair, and garbage collection.
package master

import "errors"

// Domain errors returned by Service. HTTP and gRPC adapters translate these to
// protocol-specific status codes.
var (
	ErrNotFound      = errors.New("not found")
	ErrExists        = errors.New("already exists")
	ErrInvalid       = errors.New("invalid argument")
	ErrNotEmpty      = errors.New("directory not empty")
	ErrNotDirectory  = errors.New("not a directory")
	ErrIsDirectory   = errors.New("is a directory")
	ErrNoReplica     = errors.New("no live replica available")
	ErrOutOfRange    = errors.New("offset out of range")
	ErrLeaseConflict = errors.New("lease conflict")
	ErrCorrupt       = errors.New("chunk corrupt")
)
