// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testServer builds a UI server with one GFSCluster and a temporary asset dir.
func testServer(t *testing.T) (*Server, string) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := gfsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &gfsv1alpha1.GFSCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Status: gfsv1alpha1.GFSClusterStatus{
			Phase: "Ready", MasterReady: true, ReadyChunkservers: 3, DesiredChunkservers: 3,
			HTTPEndpoint: "http://demo-client.default.svc.cluster.local:8080",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	assets := t.TempDir()
	if err := os.WriteFile(filepath.Join(assets, "index.html"), []byte("<html>GFS UI</html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(assets, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(assets, "assets", "app.js"), []byte("console.log('gfs')"), 0o644); err != nil {
		t.Fatal(err)
	}
	return NewServer(client, Options{AssetsDir: assets}, logr.Discard()), assets
}

// TestListClusters verifies cluster discovery for the UI.
func TestListClusters(t *testing.T) {
	server, _ := testServer(t)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/clusters", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Clusters []clusterSummary `json:"clusters"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Clusters) != 1 || response.Clusters[0].Name != "demo" {
		t.Fatalf("unexpected clusters: %+v", response.Clusters)
	}
	if response.Clusters[0].ProxyPath != "/api/clusters/default/demo" {
		t.Fatalf("unexpected proxy path %q", response.Clusters[0].ProxyPath)
	}
}

// TestProxyCluster verifies path and query rewriting to the GFS API.
func TestProxyCluster(t *testing.T) {
	server, _ := testServer(t)
	var gotPath, gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	server.targetFor = func(_ context.Context, namespace, name string) (string, error) {
		if namespace != "default" || name != "demo" {
			t.Fatalf("unexpected target %s/%s", namespace, name)
		}
		return upstream.URL, nil
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/clusters/default/demo/v1/read?path=%2Fdata&offset=4", nil)
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	if gotPath != "/v1/read" || !strings.Contains(gotQuery, "offset=4") {
		t.Fatalf("unexpected proxy target path=%q query=%q", gotPath, gotQuery)
	}
}

// TestProxyForwardsHTTPToken verifies the cluster bearer token is applied to
// proxied GFS requests.
func TestProxyForwardsHTTPToken(t *testing.T) {
	server, _ := testServer(t)
	cluster := &gfsv1alpha1.GFSCluster{}
	if err := server.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo"}, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.HTTP.TokenSecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "gfs-token"}, Key: "token",
	}
	if err := server.client.Update(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "gfs-token", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("s3cret")},
	}
	if err := server.client.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	var authorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	server.targetFor = func(context.Context, string, string) (string, error) { return upstream.URL, nil }
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/clusters/default/demo/v1/status", nil))
	if authorization != "Bearer s3cret" {
		t.Fatalf("unexpected authorization %q", authorization)
	}
}

// TestProxyMissingCluster verifies that unknown clusters are rejected.
func TestProxyMissingCluster(t *testing.T) {
	server, _ := testServer(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/clusters/default/missing/v1/status", nil)
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}

// TestStaticSPAFallback verifies assets are served and client routes fall back
// to index.html.
func TestStaticSPAFallback(t *testing.T) {
	server, _ := testServer(t)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "gfs") {
		t.Fatalf("asset response %d: %s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/clusters/default/demo", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("SPA fallback status %d", recorder.Code)
	}
	body, _ := io.ReadAll(recorder.Result().Body)
	if !strings.Contains(string(body), "GFS UI") {
		t.Fatalf("unexpected SPA body %q", body)
	}
}
