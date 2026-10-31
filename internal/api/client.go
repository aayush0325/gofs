// Package api is a small HTTP client for the GFS client API. It is shared by
// the gfs-client command and is useful for integration tests.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// FileInfo mirrors the master's namespace metadata JSON.
type FileInfo struct {
	Path       string `json:"path"`
	Type       string `json:"type"`
	Mode       uint32 `json:"mode"`
	Size       int64  `json:"size"`
	ModifiedAt int64  `json:"mtime_unix_nano"`
	ChunkCount int    `json:"chunk_count"`
	SnapshotOf string `json:"snapshot_of,omitempty"`
}

// ListResult is the response from GET /v1/list.
type ListResult struct {
	Entries []FileInfo `json:"entries"`
}

// WriteResult is the response from PUT /v1/write.
type WriteResult struct {
	Path         string `json:"path"`
	BytesWritten int64  `json:"bytes_written"`
	Size         int64  `json:"size"`
}

// AppendResult is the response from POST /v1/append.
type AppendResult struct {
	Path          string `json:"path"`
	Offset        int64  `json:"offset"`
	BytesAppended int64  `json:"bytes_appended"`
	Size          int64  `json:"size"`
}

// ClusterStatus mirrors GET /v1/status.
type ClusterStatus struct {
	ChunkSize      int64          `json:"chunk_size_bytes"`
	Replication    int            `json:"replication_factor"`
	Files          int64          `json:"files"`
	Directories    int64          `json:"directories"`
	Chunks         int64          `json:"chunks"`
	OrphanedChunks int64          `json:"orphaned_chunks"`
	Chunkservers   []ServerStatus `json:"chunkservers"`
	StartedAt      int64          `json:"started_at_unix_nano"`
}

// ServerStatus is one chunkserver entry in GET /v1/status.
type ServerStatus struct {
	ID         string `json:"id"`
	Address    string `json:"address"`
	Capacity   int64  `json:"capacity_bytes"`
	Used       int64  `json:"used_bytes"`
	LastSeen   int64  `json:"last_seen_unix_nano"`
	Alive      bool   `json:"alive"`
	ChunkCount int64  `json:"chunk_count"`
}

// Error is a structured HTTP API error.
type Error struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
	RequestID  string `json:"request_id"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Client calls the master HTTP API.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New creates a client. The default timeout is 30 seconds; streaming reads and
// writes use the caller's context and can outlive that timeout.
func New(baseURL, token string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   token,
		HTTP: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns: 16, IdleConnTimeout: 90 * time.Second,
			},
		},
	}
}

// Mkdir creates a directory.
func (c *Client) Mkdir(ctx context.Context, path string, recursive bool) (*FileInfo, error) {
	var info FileInfo
	err := c.json(ctx, http.MethodPost, "/v1/mkdir", nil, map[string]any{"path": path, "recursive": recursive}, &info)
	return &info, err
}

// Create creates an empty file.
func (c *Client) Create(ctx context.Context, path string, mode uint32) (*FileInfo, error) {
	var info FileInfo
	err := c.json(ctx, http.MethodPost, "/v1/create", nil, map[string]any{"path": path, "mode": mode}, &info)
	return &info, err
}

// Stat returns metadata for a path.
func (c *Client) Stat(ctx context.Context, path string) (*FileInfo, error) {
	var info FileInfo
	err := c.json(ctx, http.MethodGet, "/v1/stat", url.Values{"path": {path}}, nil, &info)
	return &info, err
}

// List returns direct children or a recursive descendant list.
func (c *Client) List(ctx context.Context, path string, recursive bool) (*ListResult, error) {
	var result ListResult
	query := url.Values{"path": {path}, "recursive": {strconv.FormatBool(recursive)}}
	err := c.json(ctx, http.MethodGet, "/v1/list", query, nil, &result)
	return &result, err
}

// Delete removes a path, optionally recursively.
func (c *Client) Delete(ctx context.Context, path string, recursive bool) error {
	query := url.Values{"path": {path}, "recursive": {strconv.FormatBool(recursive)}}
	resp, err := c.do(ctx, http.MethodDelete, "/v1/delete", query, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return decodeError(resp)
	}
	return nil
}

// Rename moves a path.
func (c *Client) Rename(ctx context.Context, source, destination string) error {
	resp, err := c.jsonResponse(ctx, http.MethodPost, "/v1/rename", nil, map[string]any{"source": source, "destination": destination})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return decodeError(resp)
	}
	return nil
}

// Snapshot creates a copy-on-write snapshot.
func (c *Client) Snapshot(ctx context.Context, source, destination string) (*FileInfo, error) {
	var info FileInfo
	err := c.json(ctx, http.MethodPost, "/v1/snapshot", nil, map[string]any{"source": source, "destination": destination}, &info)
	return &info, err
}

// Status returns cluster statistics.
func (c *Client) Status(ctx context.Context) (*ClusterStatus, error) {
	var status ClusterStatus
	err := c.json(ctx, http.MethodGet, "/v1/status", nil, nil, &status)
	return &status, err
}

// Read returns a streaming reader for a range. The caller must close it.
func (c *Client) Read(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	query := url.Values{"path": {path}, "offset": {strconv.FormatInt(offset, 10)}}
	if length >= 0 {
		query.Set("length", strconv.FormatInt(length, 10))
	}
	resp, err := c.do(ctx, http.MethodGet, "/v1/read", query, nil, "")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, decodeError(resp)
	}
	return resp.Body, nil
}

// Write streams reader into path at offset.
func (c *Client) Write(ctx context.Context, path string, offset int64, reader io.Reader) (*WriteResult, error) {
	query := url.Values{"path": {path}, "offset": {strconv.FormatInt(offset, 10)}}
	resp, err := c.do(ctx, http.MethodPut, "/v1/write", query, reader, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeError(resp)
	}
	var result WriteResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Append sends one record of at most 4 MiB.
func (c *Client) Append(ctx context.Context, path string, data []byte) (*AppendResult, error) {
	resp, err := c.do(ctx, http.MethodPost, "/v1/append", url.Values{"path": {path}}, bytes.NewReader(data), "application/octet-stream")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeError(resp)
	}
	var result AppendResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// json performs a request with a JSON body and decodes a JSON response.
func (c *Client) json(ctx context.Context, method, route string, query url.Values, body any, target any) error {
	resp, err := c.jsonResponse(ctx, method, route, query, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeError(resp)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// jsonResponse performs a request with an optional JSON body and returns the
// open response.
func (c *Client) jsonResponse(ctx context.Context, method, route string, query url.Values, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	return c.do(ctx, method, route, query, reader, "application/json")
}

// do applies authentication and URL encoding.
func (c *Client) do(ctx context.Context, method, route string, query url.Values, body io.Reader, contentType string) (*http.Response, error) {
	target := c.BaseURL + route
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	return c.HTTP.Do(req)
}

// decodeError converts an HTTP error envelope into *Error.
func decodeError(resp *http.Response) error {
	var envelope struct {
		Error Error `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&envelope)
	envelope.Error.StatusCode = resp.StatusCode
	if envelope.Error.Message == "" {
		envelope.Error.Message = resp.Status
	}
	return &envelope.Error
}
