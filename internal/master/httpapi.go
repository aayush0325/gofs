package master

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aayush0325/gofs/internal/chunkclient"
	"github.com/aayush0325/gofs/internal/metrics"
	"github.com/google/uuid"
)

// HTTPHandler exposes the GFS client-facing HTTP/JSON API. Data operations act
// as an in-process GFS client: they obtain leases from the master service and
// send blocks directly to chunkservers over mTLS.
type HTTPHandler struct {
	service *Service
	chunks  *chunkclient.Pool
	log     *slog.Logger
	met     *metrics.Metrics
	token   string
}

// NewHTTPHandler constructs the HTTP API handler. token is optional; when set,
// every /v1 route requires a bearer token.
func NewHTTPHandler(service *Service, chunks *chunkclient.Pool, log *slog.Logger, met *metrics.Metrics, token string) *HTTPHandler {
	return &HTTPHandler{service: service, chunks: chunks, log: log, met: met, token: token}
}

// Routes builds the HTTP mux with health, readiness, metrics, and file API
// endpoints. Each route is documented in api/openapi.yaml and README.md.
func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()

	// Liveness: the process is running and can serve HTTP.
	mux.Handle("GET /healthz", h.wrap("healthz", http.HandlerFunc(h.health)))
	// Readiness: the master metadata store is reachable.
	mux.Handle("GET /readyz", h.wrap("readyz", http.HandlerFunc(h.ready)))
	// Prometheus scrape endpoint.
	mux.Handle("GET /metrics", h.met.Handler())

	// Namespace operations.
	mux.Handle("POST /v1/mkdir", h.api("mkdir", http.HandlerFunc(h.mkdir)))
	mux.Handle("POST /v1/create", h.api("create", http.HandlerFunc(h.create)))
	mux.Handle("GET /v1/stat", h.api("stat", http.HandlerFunc(h.stat)))
	mux.Handle("GET /v1/list", h.api("list", http.HandlerFunc(h.list)))
	mux.Handle("DELETE /v1/delete", h.api("delete", http.HandlerFunc(h.delete)))
	mux.Handle("POST /v1/rename", h.api("rename", http.HandlerFunc(h.rename)))
	mux.Handle("POST /v1/snapshot", h.api("snapshot", http.HandlerFunc(h.snapshot)))

	// Data operations.
	mux.Handle("GET /v1/read", h.api("read", http.HandlerFunc(h.read)))
	mux.Handle("PUT /v1/write", h.api("write", http.HandlerFunc(h.write)))
	mux.Handle("POST /v1/append", h.api("append", http.HandlerFunc(h.append)))

	// Operational/debug operations.
	mux.Handle("GET /v1/locations", h.api("locations", http.HandlerFunc(h.locations)))
	mux.Handle("GET /v1/status", h.api("status", http.HandlerFunc(h.status)))
	return mux
}

// health is a liveness probe with no dependency checks.
func (h *HTTPHandler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready checks the metadata store through a harmless root stat.
func (h *HTTPHandler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := h.service.Stat(ctx, "/"); err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// mkdirRequest is the JSON body for POST /v1/mkdir.
type mkdirRequest struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

// api wraps a /v1 handler with optional token authentication and common
// middleware.
func (h *HTTPHandler) api(route string, next http.Handler) http.Handler {
	return h.wrap(route, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.token != "" {
			provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if provided == "" {
				provided = r.Header.Get("X-GFS-Token")
			}
			if provided != h.token {
				writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid API token")
				return
			}
		}
		next.ServeHTTP(w, r)
	}))
}

// wrap adds request IDs, panic recovery, access logs, and Prometheus metrics.
func (h *HTTPHandler) wrap(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, requestID))
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if recovered := recover(); recovered != nil {
				h.log.Error("http handler panic", "route", route, "request_id", requestID, "panic", recovered)
				if !recorder.wroteHeader {
					writeError(recorder, r, http.StatusInternalServerError, "INTERNAL", "internal server error")
				}
			}
			duration := time.Since(start)
			h.log.Info("http request", "route", route, "method", r.Method, "path", r.URL.Path,
				"status", recorder.status, "duration_ms", duration.Milliseconds(), "request_id", requestID)
			if h.met != nil {
				h.met.ObserveHTTP(route, r.Method, recorder.status, recorder.bytes, duration.Seconds())
			}
		}()
		next.ServeHTTP(recorder, r)
	})
}

type requestIDKey struct{}

// statusRecorder captures status and bytes for logging/metrics.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

// WriteHeader records the first status code written by a handler.
func (s *statusRecorder) WriteHeader(code int) {
	if s.wroteHeader {
		return
	}
	s.status = code
	s.wroteHeader = true
	s.ResponseWriter.WriteHeader(code)
}

// Write records response body bytes while forwarding to the underlying writer.
func (s *statusRecorder) Write(data []byte) (int, error) {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	n, err := s.ResponseWriter.Write(data)
	s.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer for flushing.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
