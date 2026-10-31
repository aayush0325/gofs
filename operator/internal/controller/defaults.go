// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// defaultMasterImage is used when spec.image.master is empty.
	defaultMasterImage = "ghcr.io/aayush0325/gofs-master:latest"
	// defaultChunkserverImage is used when spec.image.chunkserver is empty.
	defaultChunkserverImage = "ghcr.io/aayush0325/gofs-chunkserver:latest"
	// defaultChunkSize is the GFS paper's 64 MiB chunk size.
	defaultChunkSize int64 = 64 << 20
	// defaultReplication is the paper's three-way replication target.
	defaultReplication int32 = 3
	// masterHTTPPort is the client API port.
	masterHTTPPort int32 = 8080
	// masterGRPCPort is the internal metadata RPC port.
	masterGRPCPort int32 = 9090
	// chunkserverPort is the internal chunk RPC port.
	chunkserverPort int32 = 9091
	// chunkserverHTTPPort serves health and metrics.
	chunkserverHTTPPort int32 = 8081
	// tlsMountPath is where all pods mount their mTLS material.
	tlsMountPath = "/certs"
	// dataMountPath is where master and chunkserver volumes are mounted.
	dataMountPath = "/data"
)

// normalizeSpec fills omitted values and clamps replication to the available
// chunkserver count.
func normalizeSpec(spec gfsv1alpha1.GFSClusterSpec) gfsv1alpha1.GFSClusterSpec {
	if spec.Image.Master == "" {
		spec.Image.Master = defaultMasterImage
	}
	if spec.Image.Chunkserver == "" {
		spec.Image.Chunkserver = defaultChunkserverImage
	}
	if spec.Image.PullPolicy == "" {
		spec.Image.PullPolicy = corev1.PullIfNotPresent
	}
	if spec.Chunkservers.Replicas <= 0 {
		spec.Chunkservers.Replicas = 3
	}
	if spec.Master.Storage.Size.Value() <= 0 {
		spec.Master.Storage.Size = resource.MustParse("1Gi")
	}
	if spec.Chunkservers.Storage.Size.Value() <= 0 {
		spec.Chunkservers.Storage.Size = resource.MustParse("10Gi")
	}
	if spec.ChunkSizeBytes <= 0 {
		spec.ChunkSizeBytes = defaultChunkSize
	}
	if spec.ReplicationFactor <= 0 {
		spec.ReplicationFactor = defaultReplication
	}
	if spec.ReplicationFactor > spec.Chunkservers.Replicas {
		spec.ReplicationFactor = spec.Chunkservers.Replicas
	}
	if spec.LeaseDuration.Duration <= 0 {
		spec.LeaseDuration.Duration = 60 * time.Second
	}
	if spec.HeartbeatTimeout.Duration <= 0 {
		spec.HeartbeatTimeout.Duration = 15 * time.Second
	}
	if spec.GCGrace.Duration <= 0 {
		spec.GCGrace.Duration = time.Minute
	}
	if spec.MaintenanceInterval.Duration <= 0 {
		spec.MaintenanceInterval.Duration = 10 * time.Second
	}
	if spec.HTTP.ServiceType == "" {
		spec.HTTP.ServiceType = corev1.ServiceTypeClusterIP
	}
	if spec.Chunkservers.CapacityBytes <= 0 {
		spec.Chunkservers.CapacityBytes = spec.Chunkservers.Storage.Size.Value()
	}
	return spec
}

// masterServiceName returns the internal master Service name.
func masterServiceName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-master"
}

// clientServiceName returns the client-facing HTTP Service name.
func clientServiceName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-client"
}

// chunkserverServiceName returns the headless chunkserver Service name.
func chunkserverServiceName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-chunkservers"
}

// masterStatefulSetName returns the master StatefulSet name.
func masterStatefulSetName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-master"
}

// chunkserverStatefulSetName returns the chunkserver StatefulSet name.
func chunkserverStatefulSetName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-chunkserver"
}

// generatedTLSSecretName returns the operator-owned TLS Secret name.
func generatedTLSSecretName(cluster *gfsv1alpha1.GFSCluster) string {
	return cluster.Name + "-tls"
}

// commonLabels returns stable labels for a GFS component.
func commonLabels(cluster *gfsv1alpha1.GFSCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "gofs",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/managed-by": "gofs-operator",
		"gfs.gofs.io/cluster":          cluster.Name,
	}
}

// selectorLabels returns the immutable pod selector labels for a component.
func selectorLabels(cluster *gfsv1alpha1.GFSCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "gofs",
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": component,
	}
}

// fqdn returns a Kubernetes service DNS name.
func fqdn(name, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", name, namespace)
}

// certificateHosts builds every DNS name and IP SAN needed by master and
// chunkserver pods, including stable StatefulSet pod names.
func certificateHosts(cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) []string {
	namespace := cluster.Namespace
	headless := chunkserverServiceName(cluster)
	hosts := []string{"localhost", "127.0.0.1"}
	services := []string{masterServiceName(cluster), clientServiceName(cluster), headless}
	for _, service := range services {
		hosts = append(hosts,
			service,
			fmt.Sprintf("%s.%s", service, namespace),
			fmt.Sprintf("%s.%s.svc", service, namespace),
			fqdn(service, namespace),
		)
	}
	for i := int32(0); i < spec.Chunkservers.Replicas; i++ {
		pod := fmt.Sprintf("%s-%d", chunkserverStatefulSetName(cluster), i)
		hosts = append(hosts,
			pod,
			fmt.Sprintf("%s.%s", pod, headless),
			fmt.Sprintf("%s.%s.%s", pod, headless, namespace),
			fmt.Sprintf("%s.%s.%s.svc", pod, headless, namespace),
			fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod, headless, namespace),
		)
	}
	sort.Strings(hosts)
	unique := hosts[:0]
	for _, host := range hosts {
		if len(unique) == 0 || unique[len(unique)-1] != host {
			unique = append(unique, host)
		}
	}
	return unique
}

// masterArgs returns the master command-line arguments.
func masterArgs(cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) []string {
	return []string{
		"gfs-master",
		"--grpc-listen=:" + strconv.Itoa(int(masterGRPCPort)),
		"--http-listen=:" + strconv.Itoa(int(masterHTTPPort)),
		"--data-dir=" + dataMountPath,
		"--replication=" + strconv.Itoa(int(spec.ReplicationFactor)),
		"--chunk-size=" + strconv.FormatInt(spec.ChunkSizeBytes, 10),
		"--lease-duration=" + spec.LeaseDuration.Duration.String(),
		"--heartbeat-timeout=" + spec.HeartbeatTimeout.Duration.String(),
		"--gc-grace=" + spec.GCGrace.Duration.String(),
		"--maintenance-interval=" + spec.MaintenanceInterval.Duration.String(),
		"--tls-ca=" + tlsMountPath + "/ca.crt",
		"--tls-cert=" + tlsMountPath + "/server.crt",
		"--tls-key=" + tlsMountPath + "/server.key",
		"--tls-client-cert=" + tlsMountPath + "/client.crt",
		"--tls-client-key=" + tlsMountPath + "/client.key",
	}
}

// chunkserverArgs returns the chunkserver command-line arguments. Identity,
// advertise address, and master address come from environment variables.
func chunkserverArgs(spec gfsv1alpha1.GFSClusterSpec) []string {
	return []string{
		"gfs-chunkserver",
		"--grpc-listen=:" + strconv.Itoa(int(chunkserverPort)),
		"--http-listen=:" + strconv.Itoa(int(chunkserverHTTPPort)),
		"--data-dir=" + dataMountPath,
		"--capacity=" + strconv.FormatInt(spec.Chunkservers.CapacityBytes, 10),
		"--chunk-size=" + strconv.FormatInt(spec.ChunkSizeBytes, 10),
		"--tls-ca=" + tlsMountPath + "/ca.crt",
		"--tls-cert=" + tlsMountPath + "/server.crt",
		"--tls-key=" + tlsMountPath + "/server.key",
		"--tls-client-cert=" + tlsMountPath + "/client.crt",
		"--tls-client-key=" + tlsMountPath + "/client.key",
	}
}
