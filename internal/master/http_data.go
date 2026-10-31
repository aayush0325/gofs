package master

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
	"github.com/aayush0325/gofs/internal/chunkclient"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
)

// write streams the request body into the file at the requested offset. The
// stream is split at chunk boundaries and transfer-size boundaries. Each block
// is atomic; a failure part way through a long request can leave earlier blocks
// committed, matching GFS's per-chunk mutation semantics.
func (h *HTTPHandler) write(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	offset, err := parseInt64(r.URL.Query().Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be a non-negative integer")
		return
	}
	info, err := h.service.Stat(r.Context(), path)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	if info.Type != "file" {
		writeError(w, r, http.StatusBadRequest, "IS_DIRECTORY", "path is a directory")
		return
	}
	if offset > info.Size {
		writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "OUT_OF_RANGE", "write offset is beyond end of file")
		return
	}

	buf := make([]byte, chunkclient.MaxBlockBytes)
	var written int64
	for {
		n, readErr := io.ReadFull(r.Body, buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			writeError(w, r, http.StatusBadRequest, "READ_BODY", readErr.Error())
			return
		}
		if n == 0 {
			break
		}
		data := buf[:n]
		for len(data) > 0 {
			location, err := h.service.GetChunkLocation(r.Context(), path, offset, gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE, 0)
			if err != nil {
				writeAPIError(w, r, err)
				return
			}
			remainingInChunk := h.service.ChunkSize() - location.ChunkOffset
			part := int64(len(data))
			if part > remainingInChunk {
				part = remainingInChunk
			}
			if part <= 0 {
				writeError(w, r, http.StatusInternalServerError, "CHUNK_BOUNDARY", "chunk location has no writable capacity")
				return
			}
			response, err := h.writeBlock(r.Context(), location, location.ChunkOffset, data[:part], uuid.NewString())
			if err != nil {
				writeError(w, r, http.StatusBadGateway, "CHUNK_WRITE_FAILED", err.Error())
				return
			}
			if response.BytesWritten != part {
				writeError(w, r, http.StatusBadGateway, "SHORT_WRITE", fmt.Sprintf("chunkserver wrote %d of %d bytes", response.BytesWritten, part))
				return
			}
			offset += part
			written += part
			data = data[part:]
		}
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}
	if h.met != nil {
		h.met.HTTPBytes.WithLabelValues("write", "in").Add(float64(written))
	}
	info, err = h.service.Stat(r.Context(), path)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": path, "bytes_written": written, "size": info.Size})
}

// append reads one record (at most 4 MiB) and appends it atomically. The master
// allocates a fresh chunk when the record does not fit, so a successful call
// never splits a record.
func (h *HTTPHandler) append(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	limited := http.MaxBytesReader(w, r.Body, chunkclient.MaxBlockBytes)
	data, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "RECORD_TOO_LARGE", "append records are limited to 4 MiB")
			return
		}
		writeError(w, r, http.StatusBadRequest, "READ_BODY", err.Error())
		return
	}
	info, err := h.service.Stat(r.Context(), path)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	if info.Type != "file" {
		writeError(w, r, http.StatusBadRequest, "IS_DIRECTORY", "path is a directory")
		return
	}
	if len(data) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"path": path, "offset": info.Size, "bytes_appended": 0, "size": info.Size})
		return
	}
	location, err := h.service.GetChunkLocation(r.Context(), path, info.Size, gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND, int64(len(data)))
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	// GFS record append pads a chunk that cannot hold the whole record, then
	// retries the record on a fresh chunk. This preserves chunk-aligned file
	// offsets and keeps the record itself atomic.
	if available := h.service.ChunkSize() - location.Size; int64(len(data)) > available {
		if available > 0 {
			padding := make([]byte, available)
			if _, err := h.appendRecord(r.Context(), location, padding, uuid.NewString()); err != nil {
				writeError(w, r, http.StatusBadGateway, "CHUNK_PAD_FAILED", err.Error())
				return
			}
		}
		info, err = h.service.Stat(r.Context(), path)
		if err != nil {
			writeAPIError(w, r, err)
			return
		}
		location, err = h.service.GetChunkLocation(r.Context(), path, info.Size, gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND, int64(len(data)))
		if err != nil {
			writeAPIError(w, r, err)
			return
		}
		if location.Size != 0 || location.ChunkOffset != 0 {
			writeError(w, r, http.StatusInternalServerError, "APPEND_RETRY", "master did not allocate a fresh chunk after padding")
			return
		}
	}
	response, err := h.appendRecord(r.Context(), location, data, uuid.NewString())
	if err != nil {
		writeError(w, r, http.StatusBadGateway, "CHUNK_APPEND_FAILED", err.Error())
		return
	}
	absoluteOffset := location.ChunkIndex*h.service.ChunkSize() + response.Offset
	info, err = h.service.Stat(r.Context(), path)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	if h.met != nil {
		h.met.HTTPBytes.WithLabelValues("append", "in").Add(float64(len(data)))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"path": path, "offset": absoluteOffset, "bytes_appended": response.BytesAppended, "size": info.Size,
	})
}

// read streams a byte range. length defaults to the rest of the file and is
// clamped at EOF. Reading tries the primary first, then any other replica.
func (h *HTTPHandler) read(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	offset, err := parseInt64(r.URL.Query().Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be a non-negative integer")
		return
	}
	info, err := h.service.Stat(r.Context(), path)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	if info.Type != "file" {
		writeError(w, r, http.StatusBadRequest, "IS_DIRECTORY", "path is a directory")
		return
	}
	if offset > info.Size {
		writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "OUT_OF_RANGE", "offset is beyond end of file")
		return
	}
	length, err := parseInt64(r.URL.Query().Get("length"), -1)
	if err != nil || length < -1 {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "length must be a non-negative integer")
		return
	}
	if length < 0 || offset+length > info.Size {
		length = info.Size - offset
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("X-GFS-Size", strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)

	remaining := length
	flusher := http.NewResponseController(w)
	for remaining > 0 {
		location, err := h.service.GetChunkLocation(r.Context(), path, offset, gfsv1.ChunkAccess_CHUNK_ACCESS_READ, 0)
		if err != nil {
			h.log.Warn("read failed after headers", "path", path, "error", err)
			return
		}
		available := location.Size - location.ChunkOffset
		part := remaining
		if part > available {
			part = available
		}
		if part > chunkclient.MaxBlockBytes {
			part = chunkclient.MaxBlockBytes
		}
		data, _, _, _, err := h.chunks.ReadChunk(r.Context(), location, location.ChunkOffset, part)
		if err != nil {
			h.log.Warn("chunk read failed after headers", "path", path, "handle", location.Handle, "error", err)
			return
		}
		if len(data) == 0 {
			h.log.Warn("chunk read returned no data", "path", path, "handle", location.Handle)
			return
		}
		if _, err := w.Write(data); err != nil {
			return
		}
		_ = flusher.Flush()
		offset += int64(len(data))
		remaining -= int64(len(data))
	}
	if h.met != nil {
		h.met.HTTPBytes.WithLabelValues("read", "out").Add(float64(length))
	}
}

// locations returns chunk routing metadata. access=read is the default;
// access=write or access=append acquires a lease and may trigger copy-on-write.
func (h *HTTPHandler) locations(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	offset, err := parseInt64(r.URL.Query().Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be a non-negative integer")
		return
	}
	appendSize, err := parseInt64(r.URL.Query().Get("append_size"), 0)
	if err != nil || appendSize < 0 {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "append_size must be a non-negative integer")
		return
	}
	access := gfsv1.ChunkAccess_CHUNK_ACCESS_READ
	switch strings.ToLower(r.URL.Query().Get("access")) {
	case "", "read":
	case "write":
		access = gfsv1.ChunkAccess_CHUNK_ACCESS_WRITE
	case "append":
		access = gfsv1.ChunkAccess_CHUNK_ACCESS_APPEND
	default:
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "access must be read, write, or append")
		return
	}
	location, err := h.service.GetChunkLocation(r.Context(), path, offset, access, appendSize)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	data, err := protojson.Marshal(location)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "ENCODE_FAILED", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// writeBlock sends a write with the same mutation ID up to three times. Retries
// are idempotent on the chunkserver and cover lost acknowledgements or master
// report failures.
func (h *HTTPHandler) writeBlock(ctx context.Context, location *gfsv1.ChunkLocation, offset int64, data []byte, mutationID string) (*gfsv1.WriteChunkResponse, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		response, err := h.chunks.WriteChunk(ctx, location, offset, data, mutationID)
		if err == nil {
			return response, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
	}
	return nil, lastErr
}

// appendRecord sends an atomic append with the same mutation ID up to three
// times for the same reason as writeBlock.
func (h *HTTPHandler) appendRecord(ctx context.Context, location *gfsv1.ChunkLocation, data []byte, mutationID string) (*gfsv1.AppendChunkResponse, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		response, err := h.chunks.AppendChunk(ctx, location, data, mutationID)
		if err == nil {
			return response, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
	}
	return nil, lastErr
}
