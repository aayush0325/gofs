package chunkserver

import (
	"context"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/chunkclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReadChunk reads a checksum-verified block.
func (s *Service) ReadChunk(_ context.Context, req *gfsv1.ReadChunkRequest) (*gfsv1.ReadChunkResponse, error) {
	if req.Length < 0 || req.Length > chunkclient.MaxBlockBytes {
		return nil, status.Error(codes.InvalidArgument, "read length is out of bounds")
	}
	data, size, version, err := s.store.Read(req.Handle, req.Version, req.Offset, req.Length)
	s.observe("read", err)
	if err != nil {
		return nil, mapError(err)
	}
	if s.met != nil {
		s.met.ChunkBytes.WithLabelValues("out").Add(float64(len(data)))
	}
	return &gfsv1.ReadChunkResponse{Data: data, Size: size, Version: version, ChecksumVerified: true}, nil
}

// WriteChunk applies an overwrite. A primary forwards the same mutation to all
// secondaries and applies locally only after every forward succeeds.
func (s *Service) WriteChunk(ctx context.Context, req *gfsv1.WriteChunkRequest) (*gfsv1.WriteChunkResponse, error) {
	if len(req.Data) > chunkclient.MaxBlockBytes {
		return nil, status.Error(codes.InvalidArgument, "write payload exceeds the transfer limit")
	}
	if req.MutationId == "" {
		return nil, status.Error(codes.InvalidArgument, "mutation_id is required")
	}
	unlock := s.locks.Lock(req.Handle)
	defer unlock()

	primary := !req.Forward
	if primary {
		if result, ok := s.store.Mutation(req.Handle, req.MutationId); ok {
			size, err := s.store.Size(req.Handle)
			if err != nil {
				return nil, mapError(err)
			}
			version, _ := s.store.Version(req.Handle)
			if reportErr := s.report(ctx, req.Handle, version, size); reportErr != nil {
				return nil, status.Errorf(codes.Unavailable, "report retried mutation: %v", reportErr)
			}
			return &gfsv1.WriteChunkResponse{BytesWritten: result.Length, Size: size, Version: version, Deduplicated: true}, nil
		}
		localSize, err := s.store.Size(req.Handle)
		if err != nil {
			return nil, mapError(err)
		}
		if req.Offset > localSize {
			return nil, status.Error(codes.OutOfRange, "write would leave a hole")
		}
		if req.Offset+int64(len(req.Data)) > s.store.ChunkSize() {
			return nil, status.Error(codes.ResourceExhausted, "write exceeds chunk size")
		}
		localVersion, err := s.store.Version(req.Handle)
		if err != nil {
			return nil, mapError(err)
		}
		if req.Version < localVersion {
			return nil, status.Error(codes.Aborted, "stale write version")
		}
	}
	if len(req.Secondaries) > 0 && !req.Forward {
		if err := s.forwardWrites(ctx, req); err != nil {
			s.observe("write", err)
			return nil, status.Errorf(codes.Unavailable, "forward write: %v", err)
		}
	}
	result, version, size, err := s.store.Write(req.Handle, req.Version, req.Offset, req.Data, req.MutationId)
	s.observe("write", err)
	if err != nil {
		return nil, mapError(err)
	}
	if primary {
		if err := s.report(ctx, req.Handle, version, size); err != nil {
			return nil, status.Errorf(codes.Unavailable, "commit mutation: %v", err)
		}
	}
	if s.met != nil {
		s.met.ChunkBytes.WithLabelValues("in").Add(float64(len(req.Data)))
	}
	return &gfsv1.WriteChunkResponse{BytesWritten: result.Length, Size: size, Version: version}, nil
}

// AppendChunk appends one atomic record. The primary chooses the offset and
// forwards that exact offset to every secondary.
func (s *Service) AppendChunk(ctx context.Context, req *gfsv1.AppendChunkRequest) (*gfsv1.AppendChunkResponse, error) {
	if len(req.Data) > chunkclient.MaxBlockBytes {
		return nil, status.Error(codes.InvalidArgument, "append payload exceeds the transfer limit")
	}
	if req.MutationId == "" {
		return nil, status.Error(codes.InvalidArgument, "mutation_id is required")
	}
	unlock := s.locks.Lock(req.Handle)
	defer unlock()

	primary := !req.Forward
	if primary {
		if result, ok := s.store.Mutation(req.Handle, req.MutationId); ok {
			size, err := s.store.Size(req.Handle)
			if err != nil {
				return nil, mapError(err)
			}
			version, _ := s.store.Version(req.Handle)
			if reportErr := s.report(ctx, req.Handle, version, size); reportErr != nil {
				return nil, status.Errorf(codes.Unavailable, "report retried append: %v", reportErr)
			}
			return &gfsv1.AppendChunkResponse{Offset: result.Offset, BytesAppended: result.Length, Size: size, Version: version, Deduplicated: true}, nil
		}
		size, err := s.store.Size(req.Handle)
		if err != nil {
			return nil, mapError(err)
		}
		localVersion, err := s.store.Version(req.Handle)
		if err != nil {
			return nil, mapError(err)
		}
		if req.Version < localVersion {
			return nil, status.Error(codes.Aborted, "stale append version")
		}
		req.Offset = size
	}
	if int64(len(req.Data))+req.Offset > s.store.ChunkSize() {
		return nil, status.Error(codes.ResourceExhausted, "append exceeds chunk size")
	}
	if len(req.Secondaries) > 0 && !req.Forward {
		if err := s.forwardAppends(ctx, req); err != nil {
			s.observe("append", err)
			return nil, status.Errorf(codes.Unavailable, "forward append: %v", err)
		}
	}
	result, version, size, err := s.store.Append(req.Handle, req.Version, req.Offset, req.Data, req.MutationId)
	s.observe("append", err)
	if err != nil {
		return nil, mapError(err)
	}
	if primary {
		if err := s.report(ctx, req.Handle, version, size); err != nil {
			return nil, status.Errorf(codes.Unavailable, "commit append: %v", err)
		}
	}
	if s.met != nil {
		s.met.ChunkBytes.WithLabelValues("in").Add(float64(len(req.Data)))
	}
	return &gfsv1.AppendChunkResponse{Offset: result.Offset, BytesAppended: result.Length, Size: size, Version: version}, nil
}
