// Command gfs-chunkserver runs one GFS data node. It stores chunk files with
// checksums, serves chunk RPCs over mTLS, and heartbeats the master.
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
	"github.com/aayush0325/gofs/internal/chunkserver"
	"github.com/aayush0325/gofs/internal/chunkstore"
	"github.com/aayush0325/gofs/internal/config"
	"github.com/aayush0325/gofs/internal/grpcx"
	"github.com/aayush0325/gofs/internal/logging"
	"github.com/aayush0325/gofs/internal/metrics"
	"github.com/aayush0325/gofs/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// main is the process entry point; it delegates to run so defers execute.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gfs-chunkserver:", err)
		os.Exit(1)
	}
}

// run parses flags, builds the chunkserver, and serves until a signal arrives.
func run() error {
	hostname, _ := os.Hostname()
	var (
		id              = flag.String("id", config.String("GFS_CHUNKSERVER_ID", hostname), "unique chunkserver ID")
		grpcAddr        = flag.String("grpc-listen", config.String("GFS_CHUNKSERVER_GRPC_ADDR", ":9091"), "internal gRPC listen address")
		httpAddr        = flag.String("http-listen", config.String("GFS_CHUNKSERVER_HTTP_ADDR", ":8081"), "health and metrics listen address")
		advertise       = flag.String("advertise", config.String("GFS_CHUNKSERVER_ADVERTISE", ""), "address advertised to the master (defaults to localhost:listen-port)")
		masterAddr      = flag.String("master", config.String("GFS_MASTER_ADDRESS", "localhost:9090"), "master gRPC address")
		dataDir         = flag.String("data-dir", config.String("GFS_CHUNKSERVER_DATA_DIR", "./data/chunkserver"), "chunk storage directory")
		capacity        = flag.Int64("capacity", config.Int64("GFS_CHUNKSERVER_CAPACITY", 100<<30), "advertised capacity in bytes")
		chunkSize       = flag.Int64("chunk-size", config.Int64("GFS_CHUNK_SIZE", 64<<20), "maximum chunk size; must match the master")
		heartbeat       = flag.Duration("heartbeat-interval", config.Duration("GFS_HEARTBEAT_INTERVAL", 5*time.Second), "master heartbeat interval")
		tlsCA           = flag.String("tls-ca", config.String("GFS_TLS_CA", "./certs/ca.crt"), "CA certificate bundle for mutual TLS")
		tlsCert         = flag.String("tls-cert", config.String("GFS_TLS_CERT", "./certs/server.crt"), "server certificate")
		tlsKey          = flag.String("tls-key", config.String("GFS_TLS_KEY", "./certs/server.key"), "server private key")
		tlsClientCert   = flag.String("tls-client-cert", config.String("GFS_TLS_CLIENT_CERT", "./certs/client.crt"), "client certificate for outgoing gRPC")
		tlsClientKey    = flag.String("tls-client-key", config.String("GFS_TLS_CLIENT_KEY", "./certs/client.key"), "client private key for outgoing gRPC")
		tlsServerName   = flag.String("tls-server-name", config.String("GFS_TLS_SERVER_NAME", ""), "override TLS server name for outgoing gRPC")
		insecureMode    = flag.Bool("insecure", config.Bool("GFS_INSECURE", false), "disable mTLS (development only)")
		logLevel        = flag.String("log-level", config.String("GFS_LOG_LEVEL", "info"), "debug, info, warn, or error")
		shutdownTimeout = flag.Duration("shutdown-timeout", config.Duration("GFS_SHUTDOWN_TIMEOUT", 10*time.Second), "graceful shutdown timeout")
	)
	flag.Parse()

	log, err := logging.New(*logLevel)
	if err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("chunkserver id is empty")
	}
	if *advertise == "" {
		host, port, err := net.SplitHostPort(*grpcAddr)
		if err != nil {
			return fmt.Errorf("derive advertise address from %q: %w", *grpcAddr, err)
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "localhost"
		}
		*advertise = net.JoinHostPort(host, port)
	}
	if *chunkSize <= 0 || *chunkSize > 1<<30 {
		return fmt.Errorf("chunk-size must be between 1 and 1 GiB")
	}

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

	store, err := chunkstore.Open(filepath.Join(*dataDir, "chunks"), *chunkSize, log)
	if err != nil {
		return err
	}
	met := metrics.New()

	connOpts := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(8<<20))}
	if clientTLS != nil {
		connOpts = append(connOpts, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS.Clone())))
	} else {
		connOpts = append(connOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	masterConn, err := grpc.NewClient(*masterAddr, connOpts...)
	if err != nil {
		return fmt.Errorf("create master client: %w", err)
	}
	defer masterConn.Close()

	pool := chunkclient.New(clientTLS, log)
	defer pool.Close()
	service := chunkserver.NewService(store, pool, gfsv1.NewMasterClient(masterConn), chunkserver.Options{
		ID: *id, Address: *advertise, Capacity: *capacity, HeartbeatInterval: *heartbeat,
	}, log, met)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service.Start(ctx)

	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on gRPC address %s: %w", *grpcAddr, err)
	}
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(8 << 20), grpc.MaxSendMsgSize(8 << 20),
		grpc.UnaryInterceptor(grpcx.UnaryInterceptor(met, log)),
		grpc.StreamInterceptor(grpcx.StreamInterceptor(met, log)),
	}
	if serverTLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(serverTLS)))
	} else {
		serverOpts = append(serverOpts, grpc.Creds(insecure.NewCredentials()))
	}
	grpcServer := grpc.NewServer(serverOpts...)
	gfsv1.RegisterChunkServerServer(grpcServer, service)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	httpMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !service.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not-ready"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	httpMux.Handle("GET /metrics", met.Handler())
	httpServer := &http.Server{Addr: *httpAddr, Handler: httpMux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("chunkserver HTTP listening", "address", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server stopped", "error", err)
			stop()
		}
	}()
	go func() {
		log.Info("chunkserver gRPC listening", "address", *grpcAddr, "advertise", *advertise, "master", *masterAddr)
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("gRPC server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down chunkserver")
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
	log.Info("chunkserver stopped")
	return nil
}
