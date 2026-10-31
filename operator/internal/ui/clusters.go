// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// clusterSummary is the UI's read-only view of one GFSCluster.
type clusterSummary struct {
	Name                string `json:"name"`
	Namespace           string `json:"namespace"`
	Phase               string `json:"phase"`
	MasterReady         bool   `json:"masterReady"`
	ReadyChunkservers   int32  `json:"readyChunkservers"`
	DesiredChunkservers int32  `json:"desiredChunkservers"`
	HTTPEndpoint        string `json:"httpEndpoint"`
	GRPCEndpoint        string `json:"grpcEndpoint"`
	ProxyPath           string `json:"proxyPath"`
}

// listClusters returns every GFSCluster visible to the operator.
func (s *Server) listClusters(w http.ResponseWriter, r *http.Request) {
	list := &gfsv1alpha1.GFSClusterList{}
	if err := s.client.List(r.Context(), list); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list GFS clusters: "+err.Error())
		return
	}
	result := make([]clusterSummary, 0, len(list.Items))
	for i := range list.Items {
		cluster := &list.Items[i]
		result = append(result, clusterSummary{
			Name: cluster.Name, Namespace: cluster.Namespace, Phase: cluster.Status.Phase,
			MasterReady: cluster.Status.MasterReady, ReadyChunkservers: cluster.Status.ReadyChunkservers,
			DesiredChunkservers: cluster.Status.DesiredChunkservers,
			HTTPEndpoint:        cluster.Status.HTTPEndpoint, GRPCEndpoint: cluster.Status.GRPCEndpoint,
			ProxyPath: "/api/clusters/" + cluster.Namespace + "/" + cluster.Name,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace == result[j].Namespace {
			return result[i].Name < result[j].Name
		}
		return result[i].Namespace < result[j].Namespace
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"clusters": result})
}

// getCluster retrieves the cluster named in a proxy URL.
func (s *Server) getCluster(ctx context.Context, namespace, name string) (*gfsv1alpha1.GFSCluster, error) {
	cluster := &gfsv1alpha1.GFSCluster{}
	err := s.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, cluster)
	if apierrors.IsNotFound(err) {
		return nil, err
	}
	return cluster, err
}

// clusterToken returns the optional HTTP bearer token configured on a cluster.
func (s *Server) clusterToken(ctx context.Context, cluster *gfsv1alpha1.GFSCluster) (string, error) {
	ref := cluster.Spec.HTTP.TokenSecretRef
	if ref == nil {
		return "", nil
	}
	if ref.Name == "" || ref.Key == "" {
		return "", fmt.Errorf("tokenSecretRef requires name and key")
	}
	secret := &corev1.Secret{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: ref.Name}, secret); err != nil {
		return "", err
	}
	value, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s has no key %s", ref.Name, ref.Key)
	}
	return string(value), nil
}

// clientServiceTarget resolves the GFS master HTTP Service for a cluster.
func clientServiceTarget(_ context.Context, namespace, name string) (string, error) {
	return "http://" + name + "-client." + namespace + ".svc.cluster.local:8080", nil
}
