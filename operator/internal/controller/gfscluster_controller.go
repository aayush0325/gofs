/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// clusterFinalizer cleans up StatefulSet-created PVCs before the CR leaves.
	clusterFinalizer = "gfs.gofs.io/finalizer"
)

// GFSClusterReconciler reconciles a GFSCluster object into Services, Secrets,
// and StatefulSets.
type GFSClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=gfs.gofs.io,resources=gfsclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gfs.gofs.io,resources=gfsclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gfs.gofs.io,resources=gfsclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets;persistentvolumeclaims;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives the Kubernetes resources toward the GFSCluster spec.
func (r *GFSClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	cluster := &gfsv1alpha1.GFSCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !cluster.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, cluster)
	}
	if !controllerutil.ContainsFinalizer(cluster, clusterFinalizer) {
		controllerutil.AddFinalizer(cluster, clusterFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	spec := normalizeSpec(cluster.Spec)
	if err := r.validateTokenSecret(ctx, cluster, spec); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	tlsSecret, certHash, err := r.ensureTLSSecret(ctx, cluster, spec)
	if err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.reconcileMasterService(ctx, cluster); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.reconcileClientService(ctx, cluster, spec); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.reconcileChunkserverService(ctx, cluster); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.reconcileMasterStatefulSet(ctx, cluster, spec, tlsSecret, certHash); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.reconcileChunkserverStatefulSet(ctx, cluster, spec, tlsSecret, certHash); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.ensureMonitoring(ctx, cluster, spec); err != nil {
		return r.reconcileError(ctx, cluster, err)
	}

	status, err := r.buildStatus(ctx, cluster, spec)
	if err != nil {
		return r.reconcileError(ctx, cluster, err)
	}
	if err := r.updateStatus(ctx, cluster, status); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("reconciled GFS cluster", "phase", status.Phase,
		"readyChunkservers", status.ReadyChunkservers, "desired", status.DesiredChunkservers)
	if status.Phase != phaseReady {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	// Periodically re-check drift and optional Prometheus Operator resources.
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// SetupWithManager registers the controller and its owned resource watches.
func (r *GFSClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gfsv1alpha1.GFSCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Named("gfscluster").
		Complete(r)
}

// validateTokenSecret ensures an optional HTTP bearer-token key exists before
// rolling out pods that reference it.
func (r *GFSClusterReconciler) validateTokenSecret(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) error {
	if spec.HTTP.TokenSecretRef == nil {
		return nil
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: spec.HTTP.TokenSecretRef.Name, Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, secret); err != nil {
		return fmt.Errorf("read HTTP token secret %q: %w", spec.HTTP.TokenSecretRef.Name, err)
	}
	if _, ok := secret.Data[spec.HTTP.TokenSecretRef.Key]; !ok {
		return fmt.Errorf("HTTP token secret %q has no key %q", secret.Name, spec.HTTP.TokenSecretRef.Key)
	}
	return nil
}

// finalize removes StatefulSet-created PVCs before letting owner-reference
// garbage collection remove the other managed resources.
func (r *GFSClusterReconciler) finalize(ctx context.Context, cluster *gfsv1alpha1.GFSCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cluster, clusterFinalizer) {
		return ctrl.Result{}, nil
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(cluster.Namespace), client.MatchingLabels{"gfs.gofs.io/cluster": cluster.Name}); err != nil {
		return ctrl.Result{}, err
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete PVC %s: %w", pvc.Name, err)
		}
	}
	controllerutil.RemoveFinalizer(cluster, clusterFinalizer)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileError records an Error status and returns the original error so the
// controller-runtime backoff and event recorder handle retries.
func (r *GFSClusterReconciler) reconcileError(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, cause error) (ctrl.Result, error) {
	status := cluster.Status
	status.ObservedGeneration = cluster.Generation
	status.Phase = phaseError
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "ReconcileError", Message: cause.Error(),
		ObservedGeneration: cluster.Generation,
	})
	_ = r.updateStatus(ctx, cluster, status)
	return ctrl.Result{}, cause
}
