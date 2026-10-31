package chunkserver

import (
	"context"
	"errors"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/chunkstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toProtoStates converts local chunk metadata into heartbeat messages.
func toProtoStates(states []chunkstore.ChunkState) []*gfsv1.ChunkState {
	out := make([]*gfsv1.ChunkState, 0, len(states))
	for _, state := range states {
		out = append(out, &gfsv1.ChunkState{Handle: state.Handle, Version: state.Version, Size: state.Size})
	}
	return out
}

// mapError converts chunkstore errors to gRPC status codes.
func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, chunkstore.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, chunkstore.ErrStaleVersion):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, chunkstore.ErrChunkFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, chunkstore.ErrInvalidOffset):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, chunkstore.ErrCorrupt):
		return status.Error(codes.DataLoss, err.Error())
	case status.Code(err) != codes.Unknown:
		return err
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// minInt64 returns the smaller of two int64 values.
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
