// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// proxyCluster forwards /api/clusters/<namespace>/<name>/<gfs-path> to the
// selected cluster's internal client Service. The cluster lookup prevents the
// UI from proxying to arbitrary URLs.
func (s *Server) proxyCluster(w http.ResponseWriter, r *http.Request) {
	namespace, name, rest, ok := splitClusterPath(r.URL.Path)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "expected /api/clusters/<namespace>/<name>/<path>")
		return
	}
	cluster, err := s.getCluster(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "GFS cluster not found: "+namespace+"/"+name)
		return
	}
	token, err := s.clusterToken(r.Context(), cluster)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "read GFS HTTP token: "+err.Error())
		return
	}
	base, err := s.targetFor(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "resolve GFS endpoint: "+err.Error())
		return
	}
	target, err := url.Parse(base)
	if err != nil || target.Host == "" {
		writeJSONError(w, http.StatusBadGateway, "invalid GFS endpoint")
		return
	}

	proxy := &httputil.ReverseProxy{
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.URL.Path = rest
			pr.Out.URL.RawPath = ""
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			pr.Out.Host = target.Host
			if token != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+token)
			}
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeJSONError(w, http.StatusBadGateway, "GFS proxy error: "+err.Error())
		},
	}
	proxy.ServeHTTP(w, r)
}

// splitClusterPath parses /api/clusters/<namespace>/<name>/<gfs-path>.
func splitClusterPath(requestPath string) (namespace, name, rest string, ok bool) {
	trimmed := strings.TrimPrefix(requestPath, "/api/clusters/")
	if trimmed == requestPath || trimmed == "" {
		return "", "", "", false
	}
	parts := strings.SplitN(trimmed, "/", 3)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", false
	}
	rest = "/"
	if len(parts) == 3 && parts[2] != "" {
		rest = "/" + parts[2]
	}
	return parts[0], parts[1], rest, true
}
