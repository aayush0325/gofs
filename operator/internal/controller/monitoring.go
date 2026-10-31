// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	_ "embed"
	"strings"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed assets/gfs-dashboard.json
var grafanaDashboard string

const (
	// serviceMonitorKind is the Prometheus Operator CRD used for scraping.
	serviceMonitorKind = "ServiceMonitor"
	// serviceMonitorGroup is the Prometheus Operator API group.
	serviceMonitorGroup = "monitoring.coreos.com"
)

// ensureMonitoring creates the Prometheus ServiceMonitor and Grafana dashboard
// ConfigMap. ServiceMonitor creation is skipped when the Prometheus Operator
// CRD is not installed, so GFS can run in a plain cluster.
func (r *GFSClusterReconciler) ensureMonitoring(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) error {
	if spec.Monitoring.Disabled {
		_ = r.deleteIfExists(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: dashboardName(cluster), Namespace: cluster.Namespace}})
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(serviceMonitorGVK())
		obj.SetName(serviceMonitorName(cluster))
		obj.SetNamespace(cluster.Namespace)
		_ = r.deleteIfExists(ctx, obj)
		return nil
	}

	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: dashboardName(cluster), Namespace: cluster.Namespace}}
	if err := r.apply(ctx, cluster, configMap, func() error {
		configMap.Labels = mergeLabels(commonLabels(cluster, "monitoring"),
			map[string]string{"grafana_dashboard": "1"},
			spec.Monitoring.GrafanaDashboardLabels)
		dashboardID := cluster.Namespace + "-" + cluster.Name
		configMap.Data = map[string]string{
			cluster.Name + "-gfs.json": strings.ReplaceAll(grafanaDashboard, "__CLUSTER__", dashboardID),
		}
		return nil
	}); err != nil {
		return err
	}

	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetGroupVersionKind(serviceMonitorGVK())
	serviceMonitor.SetName(serviceMonitorName(cluster))
	serviceMonitor.SetNamespace(cluster.Namespace)
	err := r.apply(ctx, cluster, serviceMonitor, func() error {
		serviceMonitor.SetLabels(mergeLabels(commonLabels(cluster, "monitoring"),
			map[string]string{"release": "kube-prometheus-stack"},
			spec.Monitoring.ServiceMonitorLabels))
		serviceMonitor.Object["spec"] = map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{
				"gfs.gofs.io/scrape":  "true",
				"gfs.gofs.io/cluster": cluster.Name,
			}},
			"namespaceSelector": map[string]any{"matchNames": []any{cluster.Namespace}},
			"jobLabel":          "app.kubernetes.io/component",
			"targetLabels": []any{
				"app.kubernetes.io/name",
				"app.kubernetes.io/instance",
				"app.kubernetes.io/component",
				"gfs.gofs.io/cluster",
			},
			"endpoints": []any{map[string]any{
				"port": "http", "path": "/metrics", "interval": "30s", "scrapeTimeout": "10s",
			}},
		}
		return nil
	})
	if err != nil {
		if apimeta.IsNoMatchError(err) || strings.Contains(err.Error(), "no matches for kind") {
			return nil
		}
		return err
	}
	return nil
}

// deleteIfExists removes an optional monitoring resource and tolerates a
// missing object or an uninstalled ServiceMonitor CRD.
func (r *GFSClusterReconciler) deleteIfExists(ctx context.Context, obj client.Object) error {
	err := r.Delete(ctx, obj)
	if err == nil || apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || strings.Contains(err.Error(), "no matches for kind") {
		return nil
	}
	return err
}

// mergeLabels merges maps from lowest to highest precedence without mutating
// the inputs.
func mergeLabels(base map[string]string, extra ...map[string]string) map[string]string {
	out := make(map[string]string, len(base))
	for key, value := range base {
		out[key] = value
	}
	for _, labels := range extra {
		for key, value := range labels {
			out[key] = value
		}
	}
	return out
}

// serviceMonitorGVK returns the Prometheus Operator ServiceMonitor GVK.
func serviceMonitorGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: serviceMonitorGroup, Version: "v1", Kind: serviceMonitorKind}
}

// serviceMonitorName returns the per-cluster ServiceMonitor name.
func serviceMonitorName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-gfs-monitor"
}

// dashboardName returns the per-cluster Grafana dashboard ConfigMap name.
func dashboardName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-grafana-dashboard"
}
