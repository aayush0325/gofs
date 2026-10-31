// Package metrics defines the Prometheus metrics exposed by the gofs servers.
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a private Prometheus registry so multiple server instances can
// coexist in tests without duplicate registration panics.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
	HTTPBytes       *prometheus.CounterVec
	GRPCRequests    *prometheus.CounterVec
	GRPCDuration    *prometheus.HistogramVec
	ChunkOperations *prometheus.CounterVec
	ChunkBytes      *prometheus.CounterVec
	Heartbeats      prometheus.Counter
}

// New creates and registers the shared metric families.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: registry,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gofs", Name: "http_requests_total", Help: "HTTP requests by route, method, and status.",
		}, []string{"route", "method", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gofs", Name: "http_request_duration_seconds", Help: "HTTP request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		HTTPBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gofs", Name: "http_bytes_total", Help: "HTTP payload bytes by direction.",
		}, []string{"route", "direction"}),
		GRPCRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gofs", Name: "grpc_requests_total", Help: "gRPC requests by method and status code.",
		}, []string{"method", "code"}),
		GRPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gofs", Name: "grpc_request_duration_seconds", Help: "gRPC request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		ChunkOperations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gofs", Name: "chunk_operations_total", Help: "Chunk operations by operation and result.",
		}, []string{"operation", "result"}),
		ChunkBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gofs", Name: "chunk_bytes_total", Help: "Chunk bytes by direction.",
		}, []string{"direction"}),
		Heartbeats: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "gofs", Name: "chunkserver_heartbeats_total", Help: "Chunkserver heartbeats received.",
		}),
	}
	registry.MustRegister(m.HTTPRequests, m.HTTPDuration, m.HTTPBytes, m.GRPCRequests, m.GRPCDuration,
		m.ChunkOperations, m.ChunkBytes, m.Heartbeats)
	return m
}

// Handler returns the Prometheus scrape handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// ObserveHTTP records a completed HTTP request.
func (m *Metrics) ObserveHTTP(route, method string, status int, bytes int64, seconds float64) {
	m.HTTPRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.HTTPDuration.WithLabelValues(route, method).Observe(seconds)
	if bytes > 0 {
		m.HTTPBytes.WithLabelValues(route, "in").Add(float64(bytes))
	}
}

// ObserveChunk records a chunk operation and its result.
func (m *Metrics) ObserveChunk(operation string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.ChunkOperations.WithLabelValues(operation, result).Inc()
}
