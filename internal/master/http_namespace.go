package master

import "net/http"

// mkdir creates one directory or a missing ancestor chain.
func (h *HTTPHandler) mkdir(w http.ResponseWriter, r *http.Request) {
	var req mkdirRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	info, err := h.service.Mkdir(r.Context(), req.Path, req.Recursive)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, info)
}

// createRequest is the JSON body for POST /v1/create.
type createRequest struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode,omitempty"`
}

// create creates an empty regular file.
func (h *HTTPHandler) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	info, err := h.service.CreateFile(r.Context(), req.Path, req.Mode)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, info)
}

// stat returns metadata for one path.
func (h *HTTPHandler) stat(w http.ResponseWriter, r *http.Request) {
	info, err := h.service.Stat(r.Context(), r.URL.Query().Get("path"))
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// list returns direct children or a recursive descendant list.
func (h *HTTPHandler) list(w http.ResponseWriter, r *http.Request) {
	recursive, err := parseBool(r.URL.Query().Get("recursive"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	entries, err := h.service.List(r.Context(), r.URL.Query().Get("path"), recursive)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

// delete removes a file or directory tree.
func (h *HTTPHandler) delete(w http.ResponseWriter, r *http.Request) {
	recursive, err := parseBool(r.URL.Query().Get("recursive"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := h.service.Delete(r.Context(), r.URL.Query().Get("path"), recursive); err != nil {
		writeAPIError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// renameRequest is the JSON body for POST /v1/rename.
type renameRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// rename moves a file or directory subtree.
func (h *HTTPHandler) rename(w http.ResponseWriter, r *http.Request) {
	var req renameRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := h.service.Rename(r.Context(), req.Source, req.Destination); err != nil {
		writeAPIError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// snapshotRequest is the JSON body for POST /v1/snapshot.
type snapshotRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// snapshot creates a copy-on-write namespace snapshot.
func (h *HTTPHandler) snapshot(w http.ResponseWriter, r *http.Request) {
	var req snapshotRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	info, err := h.service.Snapshot(r.Context(), req.Source, req.Destination)
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, info)
}

// status returns cluster capacity and placement statistics.
func (h *HTTPHandler) status(w http.ResponseWriter, r *http.Request) {
	status, err := h.service.GetClusterStatus(r.Context())
	if err != nil {
		writeAPIError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}
