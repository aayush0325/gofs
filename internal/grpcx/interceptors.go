// Package grpcx provides shared gRPC server interceptors for logging and
// Prometheus metrics.
package grpcx

import (
	"context"
	"log/slog"
	"time"

	"github.com/aayush0325/gofs/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryInterceptor records latency/status for every unary RPC.
func UnaryInterceptor(met *metrics.Metrics, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)
		code := status.Code(err).String()
		if met != nil {
			met.GRPCRequests.WithLabelValues(info.FullMethod, code).Inc()
			met.GRPCDuration.WithLabelValues(info.FullMethod).Observe(duration.Seconds())
		}
		log.Debug("grpc request", "method", info.FullMethod, "code", code, "duration_ms", duration.Milliseconds())
		return resp, err
	}
}

// StreamInterceptor records latency/status for streaming RPCs. The current
// protocol is unary-only, but the interceptor keeps the server ready for future
// streaming operations.
func StreamInterceptor(met *metrics.Metrics, log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		duration := time.Since(start)
		code := status.Code(err).String()
		if met != nil {
			met.GRPCRequests.WithLabelValues(info.FullMethod, code).Inc()
			met.GRPCDuration.WithLabelValues(info.FullMethod).Observe(duration.Seconds())
		}
		log.Debug("grpc stream", "method", info.FullMethod, "code", code, "duration_ms", duration.Milliseconds())
		return err
	}
}
