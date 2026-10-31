// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

// Package ui serves the React console bundled in the operator image and proxies
// GFS HTTP requests to the selected GFSCluster.
package ui

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Options configures the UI HTTP server.
type Options struct {
	// BindAddress is the UI listen address, for example ":8090".
	BindAddress string
	// AssetsDir is the directory containing the built Vite index.html/assets.
	AssetsDir string
}

// Server implements controller-runtime's Runnable interface for the web UI.
type Server struct {
	client    client.Client
	opts      Options
	log       logr.Logger
	targetFor func(context.Context, string, string) (string, error)
}

// NewServer creates a UI server. targetFor is replaceable in tests; the default
// resolves the GFS client Service DNS name inside the cluster.
func NewServer(c client.Client, opts Options, log logr.Logger) *Server {
	if opts.BindAddress == "" {
		opts.BindAddress = ":8090"
	}
	if opts.AssetsDir == "" {
		opts.AssetsDir = "/web"
	}
	return &Server{client: c, opts: opts, log: log, targetFor: clientServiceTarget}
}

// NeedLeaderElection lets every operator replica serve the UI and proxy.
func (s *Server) NeedLeaderElection() bool { return false }

// Start runs the UI HTTP server until the manager context is canceled.
func (s *Server) Start(ctx context.Context) error {
	index := filepath.Join(s.opts.AssetsDir, "index.html")
	if _, err := os.Stat(index); err != nil {
		s.log.Info("web UI assets are not present; build operator/web", "path", s.opts.AssetsDir)
	}
	server := &http.Server{
		Addr:              s.opts.BindAddress,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("serving web UI", "address", s.opts.BindAddress, "assets", s.opts.AssetsDir)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// Handler builds the UI, cluster API, proxy, and asset routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /api/clusters", s.listClusters)
	mux.HandleFunc("/api/clusters/", s.proxyCluster)
	mux.HandleFunc("/", s.serveAsset)
	return mux
}

// serveAsset serves the Vite build and falls back to index.html for SPA routes.
func (s *Server) serveAsset(w http.ResponseWriter, r *http.Request) {
	root, err := filepath.Abs(s.opts.AssetsDir)
	if err != nil {
		http.Error(w, "web assets are unavailable", http.StatusServiceUnavailable)
		return
	}
	requestPath := filepath.FromSlash(strings.TrimPrefix(filepath.ToSlash(r.URL.Path), "/"))
	candidate := filepath.Join(root, requestPath)
	if !strings.HasPrefix(candidate, root+string(os.PathSeparator)) && candidate != root {
		http.NotFound(w, r)
		return
	}
	if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
		http.ServeFile(w, r, candidate)
		return
	}
	index := filepath.Join(root, "index.html")
	if _, err := os.Stat(index); err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("<!doctype html><title>gofs</title><h1>Web UI is not built</h1>" +
			"<p>Run <code>cd operator/web && npm install && npm run build</code>, " +
			"or use the operator Docker image which includes the build.</p>"))
		return
	}
	http.ServeFile(w, r, index)
}

// errorResponse is the shared JSON error shape for UI API and proxy failures.
type errorResponse struct {
	Error string `json:"error"`
}

// writeJSONError writes a small JSON error response.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":%q}`+"\n", message)
}
