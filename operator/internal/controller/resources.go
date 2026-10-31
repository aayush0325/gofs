// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strconv"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// dataVolumeName is the StatefulSet PVC template and mount name.
	dataVolumeName = "data"
	// certsVolumeName is the mTLS Secret volume name.
	certsVolumeName = "certs"
)

// apply performs a create-or-update and attaches the GFSCluster as controller
// owner so Kubernetes garbage collection removes managed resources.
func (r *GFSClusterReconciler) apply(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, obj client.Object, mutate func() error) error {
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
			return err
		}
		return mutate()
	})
	return err
}

// reconcileMasterService ensures the internal master Service.
func (r *GFSClusterReconciler) reconcileMasterService(ctx context.Context, cluster *gfsv1alpha1.GFSCluster) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: masterServiceName(cluster), Namespace: cluster.Namespace}}
	return r.apply(ctx, cluster, service, func() error {
		service.Labels = commonLabels(cluster, "master")
		service.Labels["gfs.gofs.io/scrape"] = "true"
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Selector = selectorLabels(cluster, "master")
		service.Spec.Ports = []corev1.ServicePort{
			{Name: "http", Port: masterHTTPPort, TargetPort: intstr.FromInt32(masterHTTPPort)},
			{Name: "grpc", Port: masterGRPCPort, TargetPort: intstr.FromInt32(masterGRPCPort)},
		}
		return nil
	})
}

// reconcileClientService ensures the client-facing HTTP Service.
func (r *GFSClusterReconciler) reconcileClientService(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: clientServiceName(cluster), Namespace: cluster.Namespace}}
	return r.apply(ctx, cluster, service, func() error {
		service.Labels = commonLabels(cluster, "client")
		service.Spec.Type = spec.HTTP.ServiceType
		service.Spec.Selector = selectorLabels(cluster, "master")
		httpPort := corev1.ServicePort{Name: "http", Port: masterHTTPPort, TargetPort: intstr.FromInt32(masterHTTPPort)}
		if spec.HTTP.NodePort > 0 && spec.HTTP.ServiceType != corev1.ServiceTypeClusterIP {
			httpPort.NodePort = spec.HTTP.NodePort
		}
		service.Spec.Ports = []corev1.ServicePort{httpPort}
		return nil
	})
}

// reconcileChunkserverService ensures the headless chunkserver Service used for
// stable per-pod DNS names.
func (r *GFSClusterReconciler) reconcileChunkserverService(ctx context.Context, cluster *gfsv1alpha1.GFSCluster) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: chunkserverServiceName(cluster), Namespace: cluster.Namespace}}
	return r.apply(ctx, cluster, service, func() error {
		service.Labels = commonLabels(cluster, "chunkserver")
		service.Labels["gfs.gofs.io/scrape"] = "true"
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.PublishNotReadyAddresses = true
		service.Spec.Selector = selectorLabels(cluster, "chunkserver")
		service.Spec.Ports = []corev1.ServicePort{
			{Name: "grpc", Port: chunkserverPort, TargetPort: intstr.FromInt32(chunkserverPort)},
			{Name: "http", Port: chunkserverHTTPPort, TargetPort: intstr.FromInt32(chunkserverHTTPPort)},
		}
		return nil
	})
}

// reconcileMasterStatefulSet ensures the single master pod and its metadata PVC.
func (r *GFSClusterReconciler) reconcileMasterStatefulSet(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec, tlsSecret, certHash string) error {
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: masterStatefulSetName(cluster), Namespace: cluster.Namespace}}
	return r.apply(ctx, cluster, statefulSet, func() error {
		statefulSet.Labels = commonLabels(cluster, "master")
		statefulSet.Spec.ServiceName = masterServiceName(cluster)
		statefulSet.Spec.Replicas = ptr.To[int32](1)
		statefulSet.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cluster, "master")}
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      commonLabels(cluster, "master"),
				Annotations: map[string]string{tlsCertHashAnnotation: certHash},
			},
			Spec: masterPodSpec(cluster, spec, tlsSecret),
		}
		statefulSet.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvcTemplate(cluster, spec.Master.Storage)}
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}
		return nil
	})
}

// reconcileChunkserverStatefulSet ensures the replicated chunkserver pods and
// their per-pod chunk PVCs.
func (r *GFSClusterReconciler) reconcileChunkserverStatefulSet(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec, tlsSecret, certHash string) error {
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: chunkserverStatefulSetName(cluster), Namespace: cluster.Namespace}}
	return r.apply(ctx, cluster, statefulSet, func() error {
		statefulSet.Labels = commonLabels(cluster, "chunkserver")
		statefulSet.Spec.ServiceName = chunkserverServiceName(cluster)
		statefulSet.Spec.Replicas = ptr.To(spec.Chunkservers.Replicas)
		statefulSet.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulSet.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cluster, "chunkserver")}
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      commonLabels(cluster, "chunkserver"),
				Annotations: map[string]string{tlsCertHashAnnotation: certHash},
			},
			Spec: chunkserverPodSpec(cluster, spec, tlsSecret),
		}
		statefulSet.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvcTemplate(cluster, spec.Chunkservers.Storage)}
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}
		return nil
	})
}

// masterPodSpec builds the master pod.
func masterPodSpec(cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec, tlsSecret string) corev1.PodSpec {
	env := []corev1.EnvVar{}
	if spec.HTTP.TokenSecretRef != nil {
		env = append(env, corev1.EnvVar{Name: "GFS_HTTP_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.HTTP.TokenSecretRef}})
	}
	return corev1.PodSpec{
		ServiceAccountName:            "default",
		SecurityContext:               podSecurityContext(),
		TerminationGracePeriodSeconds: ptr.To[int64](20),
		Containers: []corev1.Container{{
			Name:            "master",
			Image:           spec.Image.Master,
			ImagePullPolicy: spec.Image.PullPolicy,
			Args:            masterArgs(cluster, spec),
			Env:             env,
			Ports: []corev1.ContainerPort{
				{Name: "http", ContainerPort: masterHTTPPort},
				{Name: "grpc", ContainerPort: masterGRPCPort},
			},
			Resources:       spec.Master.Workload.Resources,
			SecurityContext: containerSecurityContext(),
			VolumeMounts: []corev1.VolumeMount{
				{Name: dataVolumeName, MountPath: dataMountPath},
				{Name: certsVolumeName, MountPath: tlsMountPath, ReadOnly: true},
			},
			LivenessProbe:  httpProbe("/healthz", masterHTTPPort, 15, 20),
			ReadinessProbe: httpProbe("/readyz", masterHTTPPort, 5, 10),
			StartupProbe:   startupProbe("/healthz", masterHTTPPort),
		}},
		Volumes:      []corev1.Volume{tlsVolume(tlsSecret)},
		NodeSelector: spec.Master.Workload.NodeSelector,
		Tolerations:  spec.Master.Workload.Tolerations,
	}
}

// chunkserverPodSpec builds the chunkserver pod template with stable identity
// and advertise address derived from the StatefulSet pod name.
func chunkserverPodSpec(cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec, tlsSecret string) corev1.PodSpec {
	headless := chunkserverServiceName(cluster)
	advertise := "$(POD_NAME)." + headless + "." + cluster.Namespace + ".svc.cluster.local:" + strconv.Itoa(int(chunkserverPort))
	masterAddress := fqdn(masterServiceName(cluster), cluster.Namespace) + ":" + strconv.Itoa(int(masterGRPCPort))
	env := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "GFS_CHUNKSERVER_ID", Value: "$(POD_NAME)"},
		{Name: "GFS_CHUNKSERVER_ADVERTISE", Value: advertise},
		{Name: "GFS_MASTER_ADDRESS", Value: masterAddress},
	}
	return corev1.PodSpec{
		ServiceAccountName:            "default",
		SecurityContext:               podSecurityContext(),
		TerminationGracePeriodSeconds: ptr.To[int64](20),
		Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					TopologyKey:   "kubernetes.io/hostname",
					LabelSelector: &metav1.LabelSelector{MatchLabels: selectorLabels(cluster, "chunkserver")},
				},
			}},
		}},
		Containers: []corev1.Container{{
			Name:            "chunkserver",
			Image:           spec.Image.Chunkserver,
			ImagePullPolicy: spec.Image.PullPolicy,
			Args:            chunkserverArgs(spec),
			Env:             env,
			Ports: []corev1.ContainerPort{
				{Name: "grpc", ContainerPort: chunkserverPort},
				{Name: "http", ContainerPort: chunkserverHTTPPort},
			},
			Resources:       spec.Chunkservers.Workload.Resources,
			SecurityContext: containerSecurityContext(),
			VolumeMounts: []corev1.VolumeMount{
				{Name: dataVolumeName, MountPath: dataMountPath},
				{Name: certsVolumeName, MountPath: tlsMountPath, ReadOnly: true},
			},
			LivenessProbe:  httpProbe("/healthz", chunkserverHTTPPort, 15, 20),
			ReadinessProbe: httpProbe("/readyz", chunkserverHTTPPort, 5, 10),
			StartupProbe:   startupProbe("/healthz", chunkserverHTTPPort),
		}},
		Volumes:      []corev1.Volume{tlsVolume(tlsSecret)},
		NodeSelector: spec.Chunkservers.Workload.NodeSelector,
		Tolerations:  spec.Chunkservers.Workload.Tolerations,
	}
}

// pvcTemplate creates the StatefulSet volume claim template.
func pvcTemplate(cluster *gfsv1alpha1.GFSCluster, storage gfsv1alpha1.StorageSpec) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: dataVolumeName, Labels: commonLabels(cluster, "data")},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: storage.StorageClassName,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storage.Size}},
		},
	}
}

// tlsVolume mounts the shared mTLS Secret.
func tlsVolume(secretName string) corev1.Volume {
	return corev1.Volume{
		Name: certsVolumeName,
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName:  secretName,
			DefaultMode: ptr.To[int32](0o440),
		}},
	}
}

// podSecurityContext enforces the restricted Pod Security Standard while
// allowing the mounted PVCs and Secret to be read by the non-root gfs user.
func podSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot:        ptr.To(true),
		RunAsUser:           ptr.To[int64](10001),
		RunAsGroup:          ptr.To[int64](10001),
		FSGroup:             ptr.To[int64](10001),
		FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
		SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// containerSecurityContext applies the restricted container security settings.
func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// httpProbe builds an HTTP liveness/readiness/startup probe.
func httpProbe(path string, port int32, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: path, Port: intstr.FromInt32(port), Scheme: corev1.URISchemeHTTP,
		}},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
		TimeoutSeconds:      3,
		FailureThreshold:    3,
	}
}

// startupProbe allows up to two minutes for the process to begin serving.
func startupProbe(path string, port int32) *corev1.Probe {
	probe := httpProbe(path, port, 2, 3)
	probe.FailureThreshold = 40
	return probe
}
