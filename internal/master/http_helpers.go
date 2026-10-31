package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// decodeJSON reads one strict JSON object with a 1 MiB limit.
func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, 1<<20)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode JSON body: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

// writeJSON sends a JSON response.
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// apiError is the stable JSON error envelope.
type apiError struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id,omitempty"`
	} `json:"error"`
}

// writeError emits an error envelope with a stable machine-readable code.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	var body apiError
	body.Error.Code = code
	body.Error.Message = message
	if r != nil {
		if id, ok := r.Context().Value(requestIDKey{}).(string); ok {
			body.Error.RequestID = id
		}
	}
	writeJSON(w, status, body)
}

// writeAPIError maps a service error to an HTTP status and code.
func writeAPIError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := httpStatus(err)
	writeError(w, r, status, code, err.Error())
}

// httpStatus maps master domain errors to HTTP status codes.
func httpStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "NOT_FOUND"
	case errors.Is(err, ErrExists):
		return http.StatusConflict, "ALREADY_EXISTS"
	case errors.Is(err, ErrNotEmpty):
		return http.StatusConflict, "DIRECTORY_NOT_EMPTY"
	case errors.Is(err, ErrNotDirectory):
		return http.StatusBadRequest, "NOT_A_DIRECTORY"
	case errors.Is(err, ErrIsDirectory):
		return http.StatusBadRequest, "IS_A_DIRECTORY"
	case errors.Is(err, ErrOutOfRange):
		return http.StatusRequestedRangeNotSatisfiable, "OUT_OF_RANGE"
	case errors.Is(err, ErrNoReplica):
		return http.StatusServiceUnavailable, "NO_REPLICA"
	case errors.Is(err, ErrCorrupt):
		return http.StatusInternalServerError, "CORRUPT"
	case errors.Is(err, ErrInvalid):
		return http.StatusBadRequest, "INVALID_ARGUMENT"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "TIMEOUT"
	default:
		return http.StatusInternalServerError, "INTERNAL"
	}
}

// parseBool parses an optional boolean query parameter.
func parseBool(value string) (bool, error) {
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid boolean %q", value)
	}
	return parsed, nil
}

// parseInt64 parses an optional int64 query parameter, returning fallback for
// an empty value.
func parseInt64(value string, fallback int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseInt(value, 10, 64)
}
