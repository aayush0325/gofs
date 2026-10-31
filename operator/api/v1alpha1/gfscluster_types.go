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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ImageSpec selects the container images used by the GFS pods.
type ImageSpec struct {
	// Master is the gfs-master image.
	// +kubebuilder:default="ghcr.io/aayush0325/gofs-master:latest"
	Master string `json:"master,omitempty"`
	// Chunkserver is the gfs-chunkserver image.
	// +kubebuilder:default="ghcr.io/aayush0325/gofs-chunkserver:latest"
	Chunkserver string `json:"chunkserver,omitempty"`
	// PullPolicy controls when the images are pulled.
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// StorageSpec configures a persistent volume claim.
type StorageSpec struct {
	// Size is the requested persistent volume size. The controller defaults
	// master storage to 1Gi and chunkserver storage to 10Gi.
	Size resource.Quantity `json:"size,omitempty"`
	// StorageClassName selects a StorageClass. Empty uses the cluster default.
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// WorkloadSpec contains scheduling and resource settings shared by GFS pods.
type WorkloadSpec struct {
	// Resources requests and limits for the container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// NodeSelector constrains pod placement.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations allow scheduling onto tainted nodes.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// MasterSpec configures the single GFS metadata master.
type MasterSpec struct {
	// Storage is the master BoltDB volume.
	Storage StorageSpec `json:"storage,omitempty"`
	// Workload configures resources and scheduling.
	Workload WorkloadSpec `json:"workload,omitempty"`
}

// ChunkserversSpec configures the chunkserver StatefulSet.
type ChunkserversSpec struct {
	// Replicas is the number of chunkserver pods and stable chunk volume claims.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	Replicas int32 `json:"replicas,omitempty"`
	// Storage is the per-chunkserver chunk volume.
	Storage StorageSpec `json:"storage,omitempty"`
	// Workload configures resources and scheduling.
	Workload WorkloadSpec `json:"workload,omitempty"`
	// CapacityBytes is advertised to the master as available chunk capacity.
	// Zero derives capacity from the requested volume size.
	CapacityBytes int64 `json:"capacityBytes,omitempty"`
}

// TLSSpec controls internal mutual TLS credentials.
type TLSSpec struct {
	// SecretName uses an existing secret with ca.crt, server.crt, server.key,
	// client.crt, and client.key. When empty the operator generates and owns one.
	SecretName string `json:"secretName,omitempty"`
}

// HTTPSpec configures the client-facing HTTP service.
type HTTPSpec struct {
	// ServiceType exposes the HTTP API. Internal gRPC remains ClusterIP-only.
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`
	// NodePort pins the client HTTP NodePort when serviceType is NodePort or
	// LoadBalancer. Zero lets Kubernetes allocate one.
	// +kubebuilder:validation:Minimum=30000
	// +kubebuilder:validation:Maximum=32767
	NodePort int32 `json:"nodePort,omitempty"`
	// TokenSecretRef enables bearer-token authentication on /v1 routes.
	TokenSecretRef *corev1.SecretKeySelector `json:"tokenSecretRef,omitempty"`
}

// MonitoringSpec configures Prometheus ServiceMonitors and Grafana dashboards.
type MonitoringSpec struct {
	// Disabled turns off all monitoring resources. Monitoring is enabled by default.
	Disabled bool `json:"disabled,omitempty"`
	// ServiceMonitorLabels are added to the generated ServiceMonitor. Set this
	// to the label selected by your Prometheus Operator, for example
	// release: kube-prometheus-stack.
	ServiceMonitorLabels map[string]string `json:"serviceMonitorLabels,omitempty"`
	// GrafanaDashboardLabels are added to the dashboard ConfigMap. Grafana
	// sidecars commonly select grafana_dashboard: "1".
	GrafanaDashboardLabels map[string]string `json:"grafanaDashboardLabels,omitempty"`
}

// GFSClusterSpec defines the desired state of a GFS cluster.
type GFSClusterSpec struct {
	// Image selects the master and chunkserver images.
	Image ImageSpec `json:"image,omitempty"`
	// Master configures the metadata master.
	Master MasterSpec `json:"master,omitempty"`
	// Chunkservers configures the data plane.
	Chunkservers ChunkserversSpec `json:"chunkservers,omitempty"`
	// ChunkSizeBytes is the maximum chunk size. It must match in master and data pods.
	// +kubebuilder:validation:Minimum=1048576
	// +kubebuilder:validation:Maximum=1073741824
	// +kubebuilder:default=67108864
	ChunkSizeBytes int64 `json:"chunkSizeBytes,omitempty"`
	// ReplicationFactor is the target number of replicas per chunk. Zero uses
	// min(3, chunkservers.replicas).
	// +kubebuilder:validation:Minimum=1
	ReplicationFactor int32 `json:"replicationFactor,omitempty"`
	// LeaseDuration is the primary lease lifetime.
	LeaseDuration metav1.Duration `json:"leaseDuration,omitempty"`
	// HeartbeatTimeout is how long the master waits before declaring a chunkserver dead.
	HeartbeatTimeout metav1.Duration `json:"heartbeatTimeout,omitempty"`
	// GCGrace is the delay before orphaned chunks are physically deleted.
	GCGrace metav1.Duration `json:"gcGrace,omitempty"`
	// MaintenanceInterval controls re-replication and garbage collection passes.
	MaintenanceInterval metav1.Duration `json:"maintenanceInterval,omitempty"`
	// TLS configures internal mutual TLS.
	TLS TLSSpec `json:"tls,omitempty"`
	// HTTP configures the client-facing API service.
	HTTP HTTPSpec `json:"http,omitempty"`
	// Monitoring configures Prometheus scraping and Grafana dashboards.
	Monitoring MonitoringSpec `json:"monitoring,omitempty"`
}

// GFSClusterStatus defines the observed state of a GFSCluster.
type GFSClusterStatus struct {
	// ObservedGeneration is the last reconciled spec generation.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Phase is Pending, Progressing, Ready, or Error.
	// +kubebuilder:validation:Enum=Pending;Progressing;Ready;Error
	Phase string `json:"phase,omitempty"`
	// MasterReady reports whether the master pod is ready.
	MasterReady bool `json:"masterReady,omitempty"`
	// ReadyChunkservers is the number of ready chunkserver pods.
	ReadyChunkservers int32 `json:"readyChunkservers,omitempty"`
	// DesiredChunkservers is the requested chunkserver replica count.
	DesiredChunkservers int32 `json:"desiredChunkservers,omitempty"`
	// HTTPEndpoint is the in-cluster client API endpoint.
	HTTPEndpoint string `json:"httpEndpoint,omitempty"`
	// GRPCEndpoint is the in-cluster master gRPC endpoint.
	GRPCEndpoint string `json:"grpcEndpoint,omitempty"`
	// Conditions describe the current reconciliation state.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=gfs;gfscluster
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Chunkservers",type=integer,JSONPath=`.status.readyChunkservers`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredChunkservers`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GFSCluster is a single-master Google File System deployment.
type GFSCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GFSClusterSpec   `json:"spec,omitempty"`
	Status GFSClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GFSClusterList contains a list of GFSCluster resources.
type GFSClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GFSCluster `json:"items"`
}

// init registers the API types with the operator scheme.
func init() {
	SchemeBuilder.Register(&GFSCluster{}, &GFSClusterList{})
}
