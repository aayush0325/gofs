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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
)

var _ = Describe("GFSCluster Controller", func() {
	const (
		resourceName = "test-gfs"
		namespace    = "default"
	)
	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: namespace}

	// cleanupCluster removes the finalizer and all resources created by the
	// controller so each spec starts from an empty namespace.
	cleanupCluster := func() {
		cluster := &gfsv1alpha1.GFSCluster{}
		if err := k8sClient.Get(ctx, key, cluster); err == nil {
			controllerutil.RemoveFinalizer(cluster, clusterFinalizer)
			_ = k8sClient.Update(ctx, cluster)
			_ = k8sClient.Delete(ctx, cluster)
		}
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-master", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-client", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-chunkservers", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-tls", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-master", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-chunkserver", Namespace: namespace}})
	}

	BeforeEach(func() {
		cluster := &gfsv1alpha1.GFSCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: namespace},
			Spec: gfsv1alpha1.GFSClusterSpec{
				Chunkservers: gfsv1alpha1.ChunkserversSpec{Replicas: 3},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
	})

	AfterEach(cleanupCluster)

	It("creates services, TLS material, and StatefulSets", func() {
		reconciler := &GFSClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		request := reconcile.Request{NamespacedName: key}
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-tls", Namespace: namespace}, secret)).To(Succeed())
		Expect(secret.Data).To(HaveKey("ca.crt"))
		Expect(secret.Data).To(HaveKey("server.key"))

		master := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-master", Namespace: namespace}, master)).To(Succeed())
		Expect(master.Spec.Template.Spec.Containers).To(HaveLen(1))
		Expect(master.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--data-dir=/data"))
		Expect(master.Spec.Template.Annotations).To(HaveKey(tlsCertHashAnnotation))

		chunkserver := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-chunkserver", Namespace: namespace}, chunkserver)).To(Succeed())
		Expect(*chunkserver.Spec.Replicas).To(Equal(int32(3)))
		env := map[string]string{}
		for _, item := range chunkserver.Spec.Template.Spec.Containers[0].Env {
			env[item.Name] = item.Value
		}
		Expect(env["GFS_CHUNKSERVER_ADVERTISE"]).To(ContainSubstring("$(POD_NAME)." + resourceName + "-chunkservers"))

		clientService := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-client", Namespace: namespace}, clientService)).To(Succeed())
		Expect(clientService.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))

		dashboard := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-grafana-dashboard", Namespace: namespace}, dashboard)).To(Succeed())
		Expect(dashboard.Labels).To(HaveKeyWithValue("grafana_dashboard", "1"))
		Expect(dashboard.Data).To(HaveKey(resourceName + "-gfs.json"))
		Expect(dashboard.Data[resourceName+"-gfs.json"]).To(ContainSubstring(resourceName))
	})

	It("reports Ready when both StatefulSets are ready", func() {
		reconciler := &GFSClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		request := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, request)
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		master := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-master", Namespace: namespace}, master)).To(Succeed())
		master.Status.Replicas = 1
		master.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, master)).To(Succeed())

		chunkserver := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-chunkserver", Namespace: namespace}, chunkserver)).To(Succeed())
		chunkserver.Status.Replicas = 3
		chunkserver.Status.ReadyReplicas = 3
		Expect(k8sClient.Status().Update(ctx, chunkserver)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		cluster := &gfsv1alpha1.GFSCluster{}
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		Expect(cluster.Status.Phase).To(Equal(phaseReady))
		Expect(cluster.Status.ReadyChunkservers).To(Equal(int32(3)))
		Expect(cluster.Status.HTTPEndpoint).To(ContainSubstring(resourceName + "-client.default.svc.cluster.local"))
	})

	It("honors a fixed NodePort for the client service", func() {
		reconciler := &GFSClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		request := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, request)
		cluster := &gfsv1alpha1.GFSCluster{}
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		cluster.Spec.HTTP.ServiceType = corev1.ServiceTypeNodePort
		cluster.Spec.HTTP.NodePort = 30080
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		service := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-client", Namespace: namespace}, service)).To(Succeed())
		Expect(service.Spec.Type).To(Equal(corev1.ServiceTypeNodePort))
		Expect(service.Spec.Ports[0].NodePort).To(Equal(int32(30080)))
	})

	It("removes monitoring resources when disabled", func() {
		reconciler := &GFSClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		request := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, request)
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		cluster := &gfsv1alpha1.GFSCluster{}
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		cluster.Spec.Monitoring.Disabled = true
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-grafana-dashboard", Namespace: namespace}, &corev1.ConfigMap{})
		Expect(err).To(HaveOccurred())
	})

	It("ignores deleted clusters", func() {
		reconciler := &GFSClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		cluster := &gfsv1alpha1.GFSCluster{}
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		controllerutil.RemoveFinalizer(cluster, clusterFinalizer)
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	})
})
