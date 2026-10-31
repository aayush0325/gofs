// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// phasePending means no GFS component is ready yet.
	phasePending = "Pending"
	// phaseProgressing means at least one component is ready but not all are.
	phaseProgressing = "Progressing"
	// phaseReady means the master and all desired chunkservers are ready.
	phaseReady = "Ready"
	// phaseError means the operator could not reconcile the desired resources.
	phaseError = "Error"
)

// buildStatus reads StatefulSet status and derives the public cluster state.
func (r *GFSClusterReconciler) buildStatus(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) (gfsv1alpha1.GFSClusterStatus, error) {
	status := gfsv1alpha1.GFSClusterStatus{
		ObservedGeneration:  cluster.Generation,
		DesiredChunkservers: spec.Chunkservers.Replicas,
		HTTPEndpoint:        fmt.Sprintf("http://%s:%d", fqdn(clientServiceName(cluster), cluster.Namespace), masterHTTPPort),
		GRPCEndpoint:        fmt.Sprintf("%s:%d", fqdn(masterServiceName(cluster), cluster.Namespace), masterGRPCPort),
	}

	master := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: masterStatefulSetName(cluster), Namespace: cluster.Namespace}, master)
	if err == nil {
		status.MasterReady = master.Status.ReadyReplicas >= 1
	} else if !apierrors.IsNotFound(err) {
		return status, err
	}

	chunkservers := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: chunkserverStatefulSetName(cluster), Namespace: cluster.Namespace}, chunkservers)
	if err == nil {
		status.ReadyChunkservers = chunkservers.Status.ReadyReplicas
	} else if !apierrors.IsNotFound(err) {
		return status, err
	}

	switch {
	case status.MasterReady && status.ReadyChunkservers >= status.DesiredChunkservers:
		status.Phase = phaseReady
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllComponentsReady",
			Message: "GFS master and all requested chunkservers are ready", ObservedGeneration: cluster.Generation,
		})
	case status.MasterReady || status.ReadyChunkservers > 0:
		status.Phase = phaseProgressing
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: "ComponentsNotReady",
			Message:            fmt.Sprintf("master ready=%t, chunkservers ready=%d/%d", status.MasterReady, status.ReadyChunkservers, status.DesiredChunkservers),
			ObservedGeneration: cluster.Generation,
		})
	default:
		status.Phase = phasePending
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: "NoComponentsReady",
			Message: "waiting for the master and chunkserver StatefulSets to become ready", ObservedGeneration: cluster.Generation,
		})
	}
	return status, nil
}

// updateStatus persists a changed status subresource.
func (r *GFSClusterReconciler) updateStatus(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, status gfsv1alpha1.GFSClusterStatus) error {
	if equality.Semantic.DeepEqual(cluster.Status, status) {
		return nil
	}
	cluster.Status = status
	return r.Status().Update(ctx, cluster)
}
