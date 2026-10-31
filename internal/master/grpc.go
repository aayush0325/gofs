package master

import (
	"context"
	"errors"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer adapts Service to the generated Master service. It also translates
// domain errors into stable gRPC status codes.
type GRPCServer struct {
	gfsv1.UnimplementedMasterServer
	service *Service
}

// NewGRPCServer wraps a master service.
func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// RegisterChunkserver implements the chunkserver registration RPC.
func (g *GRPCServer) RegisterChunkserver(ctx context.Context, req *gfsv1.RegisterChunkserverRequest) (*gfsv1.RegisterChunkserverResponse, error) {
	handles, err := g.service.Register(ctx, req.ServerId, req.Address, req.CapacityBytes, req.UsedBytes, toReported(req.Chunks))
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.RegisterChunkserverResponse{
		DeleteHandles: handles, HeartbeatIntervalSeconds: 5,
	}, nil
}

// Heartbeat implements the periodic chunkserver heartbeat RPC.
func (g *GRPCServer) Heartbeat(ctx context.Context, req *gfsv1.HeartbeatRequest) (*gfsv1.HeartbeatResponse, error) {
	handles, err := g.service.Heartbeat(ctx, req.ServerId, req.Address, req.CapacityBytes, req.UsedBytes, toReported(req.Chunks))
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.HeartbeatResponse{DeleteHandles: handles}, nil
}

// GetChunkLocations implements lease acquisition and copy-on-write routing.
func (g *GRPCServer) GetChunkLocations(ctx context.Context, req *gfsv1.GetChunkLocationsRequest) (*gfsv1.GetChunkLocationsResponse, error) {
	location, err := g.service.GetChunkLocation(ctx, req.Path, req.Offset, req.Access, req.AppendSize)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.GetChunkLocationsResponse{Location: location}, nil
}

// ReportChunk implements size/version reporting from a primary chunkserver.
func (g *GRPCServer) ReportChunk(ctx context.Context, req *gfsv1.ReportChunkRequest) (*gfsv1.ReportChunkResponse, error) {
	if err := g.service.ReportChunk(ctx, req.ServerId, req.Handle, req.Version, req.Size); err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.ReportChunkResponse{}, nil
}

// Mkdir implements the directory creation RPC.
func (g *GRPCServer) Mkdir(ctx context.Context, req *gfsv1.MkdirRequest) (*gfsv1.MkdirResponse, error) {
	info, err := g.service.Mkdir(ctx, req.Path, req.Recursive)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.MkdirResponse{File: toProtoInfo(info)}, nil
}

// CreateFile implements the empty-file creation RPC.
func (g *GRPCServer) CreateFile(ctx context.Context, req *gfsv1.CreateFileRequest) (*gfsv1.CreateFileResponse, error) {
	info, err := g.service.CreateFile(ctx, req.Path, req.Mode)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.CreateFileResponse{File: toProtoInfo(info)}, nil
}

// Delete implements the namespace deletion RPC.
func (g *GRPCServer) Delete(ctx context.Context, req *gfsv1.DeleteRequest) (*gfsv1.DeleteResponse, error) {
	if err := g.service.Delete(ctx, req.Path, req.Recursive); err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.DeleteResponse{}, nil
}

// Rename implements the atomic subtree rename RPC.
func (g *GRPCServer) Rename(ctx context.Context, req *gfsv1.RenameRequest) (*gfsv1.RenameResponse, error) {
	if err := g.service.Rename(ctx, req.Source, req.Destination); err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.RenameResponse{}, nil
}

// Stat implements the metadata lookup RPC.
func (g *GRPCServer) Stat(ctx context.Context, req *gfsv1.StatRequest) (*gfsv1.StatResponse, error) {
	info, err := g.service.Stat(ctx, req.Path)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.StatResponse{File: toProtoInfo(info)}, nil
}

// List implements the directory listing RPC.
func (g *GRPCServer) List(ctx context.Context, req *gfsv1.ListRequest) (*gfsv1.ListResponse, error) {
	entries, err := g.service.List(ctx, req.Path, req.Recursive)
	if err != nil {
		return nil, mapError(err)
	}
	response := &gfsv1.ListResponse{Entries: make([]*gfsv1.FileInfo, 0, len(entries))}
	for _, entry := range entries {
		response.Entries = append(response.Entries, toProtoInfo(entry))
	}
	return response, nil
}

// Snapshot implements copy-on-write namespace snapshots.
func (g *GRPCServer) Snapshot(ctx context.Context, req *gfsv1.SnapshotRequest) (*gfsv1.SnapshotResponse, error) {
	info, err := g.service.Snapshot(ctx, req.Source, req.Destination)
	if err != nil {
		return nil, mapError(err)
	}
	return &gfsv1.SnapshotResponse{File: toProtoInfo(info)}, nil
}

// GetClusterStatus implements the cluster status RPC.
func (g *GRPCServer) GetClusterStatus(ctx context.Context, _ *gfsv1.GetClusterStatusRequest) (*gfsv1.GetClusterStatusResponse, error) {
	status, err := g.service.GetClusterStatus(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	response := &gfsv1.GetClusterStatusResponse{Status: &gfsv1.ClusterStatus{
		ChunkSizeBytes: status.ChunkSize, ReplicationFactor: int32(status.Replication),
		Files: status.Files, Directories: status.Directories, Chunks: status.Chunks,
		OrphanedChunks: status.OrphanedChunks,
	}}
	for _, server := range status.Chunkservers {
		response.Status.Chunkservers = append(response.Status.Chunkservers, &gfsv1.ChunkserverStatus{
			ServerId: server.ID, Address: server.Address, CapacityBytes: server.Capacity,
			UsedBytes: server.Used, LastSeenUnixNano: server.LastSeen, Alive: server.Alive,
			ChunkCount: server.ChunkCount,
		})
	}
	return response, nil
}

// toReported converts generated chunk state messages into domain values.
func toReported(chunks []*gfsv1.ChunkState) []ReportedChunk {
	out := make([]ReportedChunk, 0, len(chunks))
	for _, chunk := range chunks {
		out = append(out, ReportedChunk{Handle: chunk.Handle, Version: chunk.Version, Size: chunk.Size})
	}
	return out
}

// toProtoInfo converts a domain file view into the wire response.
func toProtoInfo(info *FileInfo) *gfsv1.FileInfo {
	t := gfsv1.NodeType_NODE_TYPE_FILE
	if info.Type == metadata.DirectoryNode {
		t = gfsv1.NodeType_NODE_TYPE_DIRECTORY
	}
	return &gfsv1.FileInfo{
		Path: info.Path, Type: t, Mode: info.Mode, Size: info.Size,
		MtimeUnixNano: info.ModifiedAt, ChunkCount: int64(info.ChunkCount), SnapshotOf: info.SnapshotOf,
	}
}

// mapError converts master domain errors into gRPC codes while preserving the
// original message.
func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrNotDirectory), errors.Is(err, ErrIsDirectory):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrNoReplica):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ErrOutOfRange):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, ErrLeaseConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ErrCorrupt):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
