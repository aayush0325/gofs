package chunkserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/chunkclient"
)

// CreateChunk creates an empty chunk.
func (s *Service) CreateChunk(_ context.Context, req *gfsv1.CreateChunkRequest) (*gfsv1.CreateChunkResponse, error) {
	size, version, err := s.store.Create(req.Handle, req.Version)
	s.observe("create", err)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.CreateChunkResponse{Size: size, Version: version}, nil
}

// AdvanceVersion fences a local chunk at a newer master version.
func (s *Service) AdvanceVersion(_ context.Context, req *gfsv1.AdvanceVersionRequest) (*gfsv1.AdvanceVersionResponse, error) {
	version, err := s.store.AdvanceVersion(req.Handle, req.Version)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.AdvanceVersionResponse{Version: version}, nil
}

// CopyChunk creates a new local chunk from a local or remote source. Remote
// copies stream blocks from a healthy source replica.
func (s *Service) CopyChunk(ctx context.Context, req *gfsv1.CopyChunkRequest) (*gfsv1.CopyChunkResponse, error) {
	if s.store.Has(req.Handle) {
		size, err := s.store.Size(req.Handle)
		if err != nil {
			return nil, mapError(err)
		}
		version, err := s.store.Version(req.Handle)
		if err != nil {
			return nil, mapError(err)
		}
		return &gfsv1.CopyChunkResponse{Size: size, Version: version}, nil
	}
	var (
		size    int64
		version uint64
		err     error
	)
	if s.store.Has(req.SourceHandle) {
		size, version, err = s.store.CopyLocal(req.SourceHandle, req.Handle, req.Version)
	} else {
		size, version, err = s.copyRemote(ctx, req)
	}
	s.observe("copy", err)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.CopyChunkResponse{Size: size, Version: version}, nil
}

// copyRemote tries each source replica until one can supply the whole chunk.
func (s *Service) copyRemote(ctx context.Context, req *gfsv1.CopyChunkRequest) (int64, uint64, error) {
	var lastErr error
	for _, source := range req.Sources {
		if source.Address == "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		probe, err := s.pool.ReadChunkFrom(probeCtx, source.Address, &gfsv1.ReadChunkRequest{
			Handle: req.SourceHandle, Version: req.Version, Offset: 0, Length: 0,
		})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		size := probe.Size
		sourceAddress := source.Address
		_, version, err := s.store.Ingest(req.Handle, req.Version, size, func(w io.Writer) (int64, error) {
			var written int64
			for written < size {
				n := minInt64(chunkclient.MaxBlockBytes, size-written)
				readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				resp, readErr := s.pool.ReadChunkFrom(readCtx, sourceAddress, &gfsv1.ReadChunkRequest{
					Handle: req.SourceHandle, Version: req.Version, Offset: written, Length: n,
				})
				cancel()
				if readErr != nil {
					return written, readErr
				}
				if len(resp.Data) == 0 {
					return written, io.ErrUnexpectedEOF
				}
				if _, writeErr := w.Write(resp.Data); writeErr != nil {
					return written, writeErr
				}
				written += int64(len(resp.Data))
			}
			return written, nil
		})
		if err == nil {
			return size, version, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no source replicas supplied")
	}
	return 0, 0, fmt.Errorf("remote chunk copy failed: %w", lastErr)
}

// DeleteChunk removes a local chunk.
func (s *Service) DeleteChunk(_ context.Context, req *gfsv1.DeleteChunkRequest) (*gfsv1.DeleteChunkResponse, error) {
	err := s.store.Delete(req.Handle)
	s.observe("delete", err)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.DeleteChunkResponse{}, nil
}

// CheckChunk verifies all local checksums.
func (s *Service) CheckChunk(_ context.Context, req *gfsv1.CheckChunkRequest) (*gfsv1.CheckChunkResponse, error) {
	valid, size, version, corrupt, err := s.store.Check(req.Handle)
	s.observe("check", err)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.CheckChunkResponse{Valid: valid, Size: size, Version: version, CorruptOffsets: corrupt}, nil
}
