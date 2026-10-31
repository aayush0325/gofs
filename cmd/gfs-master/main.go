// Command gfs-master runs the GFS metadata master and the client-facing HTTP
// API. All internal RPCs use mutual TLS by default.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/chunkclient"
	"github.com/aayush0325/gofs/internal/config"
	"github.com/aayush0325/gofs/internal/grpcx"
	"github.com/aayush0325/gofs/internal/logging"
	"github.com/aayush0325/gofs/internal/master"
	"github.com/aayush0325/gofs/internal/metadata"
	"github.com/aayush0325/gofs/internal/metrics"
	"github.com/aayush0325/gofs/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// main is the process entry point; it delegates to run so defers execute.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gfs-master:", err)
		os.Exit(1)
	}
}

// run parses flags, builds the master, and serves until a signal arrives.
func run() error {
	var (
		grpcAddr         = flag.String("grpc-listen", config.String("GFS_MASTER_GRPC_ADDR", ":9090"), "internal gRPC listen address")
		httpAddr         = flag.String("http-listen", config.String("GFS_MASTER_HTTP_ADDR", ":8080"), "client HTTP listen address")
		dataDir          = flag.String("data-dir", config.String("GFS_MASTER_DATA_DIR", "./data/master"), "metadata directory")
		chunkSize        = flag.Int64("chunk-size", config.Int64("GFS_CHUNK_SIZE", 64<<20), "maximum chunk size in bytes")
		replication      = flag.Int("replication", config.Int("GFS_REPLICATION", 3), "target chunk replication factor")
		leaseDuration    = flag.Duration("lease-duration", config.Duration("GFS_LEASE_DURATION", 60*time.Second), "primary lease duration")
		heartbeatTimeout = flag.Duration("heartbeat-timeout", config.Duration("GFS_HEARTBEAT_TIMEOUT", 15*time.Second), "time before a chunkserver is considered dead")
		gcGrace          = flag.Duration("gc-grace", config.Duration("GFS_GC_GRACE", time.Minute), "delay before orphaned chunks are deleted")
		maintenance      = flag.Duration("maintenance-interval", config.Duration("GFS_MAINTENANCE_INTERVAL", 10*time.Second), "replication and GC pass interval")
		tlsCA            = flag.String("tls-ca", config.String("GFS_TLS_CA", "./certs/ca.crt"), "CA certificate bundle for mutual TLS")
		tlsCert          = flag.String("tls-cert", config.String("GFS_TLS_CERT", "./certs/server.crt"), "server certificate")
		tlsKey           = flag.String("tls-key", config.String("GFS_TLS_KEY", "./certs/server.key"), "server private key")
		tlsClientCert    = flag.String("tls-client-cert", config.String("GFS_TLS_CLIENT_CERT", "./certs/client.crt"), "client certificate for outgoing gRPC")
		tlsClientKey     = flag.String("tls-client-key", config.String("GFS_TLS_CLIENT_KEY", "./certs/client.key"), "client private key for outgoing gRPC")
		tlsServerName    = flag.String("tls-server-name", config.String("GFS_TLS_SERVER_NAME", ""), "override TLS server name for chunkserver connections")
		insecureMode     = flag.Bool("insecure", config.Bool("GFS_INSECURE", false), "disable mTLS (development only)")
		httpToken        = flag.String("http-token", config.String("GFS_HTTP_TOKEN", ""), "optional bearer token for /v1 HTTP routes")
		logLevel         = flag.String("log-level", config.String("GFS_LOG_LEVEL", "info"), "debug, info, warn, or error")
		shutdownTimeout  = flag.Duration("shutdown-timeout", config.Duration("GFS_SHUTDOWN_TIMEOUT", 10*time.Second), "graceful shutdown timeout")
	)
	flag.Parse()

	log, err := logging.New(*logLevel)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var serverTLS, clientTLS *tls.Config
	if !*insecureMode {
		serverTLS, err = tlsutil.ServerConfig(*tlsCA, *tlsCert, *tlsKey)
		if err != nil {
			return fmt.Errorf("load server TLS: %w", err)
		}
		clientTLS, err = tlsutil.ClientConfig(*tlsCA, *tlsClientCert, *tlsClientKey, *tlsServerName)
		if err != nil {
			return fmt.Errorf("load client TLS: %w", err)
		}
	} else {
		log.Warn("insecure mode enabled: internal gRPC is not using mTLS")
	}

	if err := os.MkdirAll(*dataDir, 0o750); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	store, err := metadata.Open(filepath.Join(*dataDir, "master.db"))
	if err != nil {
		return err
	}
	defer store.Close()

	met := metrics.New()
	pool := chunkclient.New(clientTLS, log)
	defer pool.Close()
	service := master.NewService(store, pool, log, met, master.Options{
		ChunkSize: *chunkSize, Replication: *replication, LeaseDuration: *leaseDuration,
		HeartbeatTimeout: *heartbeatTimeout, GCGrace: *gcGrace, MaintenanceInterval: *maintenance,
	})
	service.Start(ctx)

	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on gRPC address %s: %w", *grpcAddr, err)
	}
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(8 << 20),
		grpc.MaxSendMsgSize(8 << 20),
		grpc.UnaryInterceptor(grpcx.UnaryInterceptor(met, log)),
		grpc.StreamInterceptor(grpcx.StreamInterceptor(met, log)),
	}
	if serverTLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(serverTLS)))
	} else {
		serverOpts = append(serverOpts, grpc.Creds(insecure.NewCredentials()))
	}
	grpcServer := grpc.NewServer(serverOpts...)
	gfsv1.RegisterMasterServer(grpcServer, master.NewGRPCServer(service))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	httpHandler := master.NewHTTPHandler(service, pool, log, met, *httpToken)
	httpServer := &http.Server{
		Addr: *httpAddr, Handler: httpHandler.Routes(),
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	go func() {
		log.Info("master HTTP API listening", "address", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server stopped", "error", err)
			stop()
		}
	}()
	go func() {
		log.Info("master gRPC listening", "address", *grpcAddr)
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("gRPC server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down master")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	log.Info("master stopped")
	return nil
}
