// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestGenerateTLSMaterial verifies the generated CA, SANs, and extended key
// usages used by the mutual TLS data plane.
func TestGenerateTLSMaterial(t *testing.T) {
	hosts := []string{"localhost", "127.0.0.1", "gofs-master.default.svc.cluster.local"}
	material, hash, err := generateTLSMaterial(hosts)
	if err != nil {
		t.Fatal(err)
	}
	if hash == "" || len(hash) != 64 {
		t.Fatalf("unexpected hash %q", hash)
	}
	for _, key := range tlsSecretKeys {
		if len(material[key]) == 0 {
			t.Fatalf("missing key %s", key)
		}
	}
	ca := parseCertificate(t, material["ca.crt"])
	server := parseCertificate(t, material["server.crt"])
	client := parseCertificate(t, material["client.crt"])
	if !ca.IsCA {
		t.Fatal("CA certificate is not marked IsCA")
	}
	if err := server.CheckSignatureFrom(ca); err != nil {
		t.Fatalf("server certificate is not signed by CA: %v", err)
	}
	if err := client.CheckSignatureFrom(ca); err != nil {
		t.Fatalf("client certificate is not signed by CA: %v", err)
	}
	if err := server.VerifyHostname("gofs-master.default.svc.cluster.local"); err != nil {
		t.Fatalf("server SAN missing service DNS: %v", err)
	}
	if err := server.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("server SAN missing loopback IP: %v", err)
	}
	if !hasUsage(server.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		t.Fatal("server certificate lacks serverAuth")
	}
	if !hasUsage(client.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		t.Fatal("client certificate lacks clientAuth")
	}
	if client.NotAfter.Before(time.Now().Add(24 * time.Hour)) {
		t.Fatal("client certificate expires too soon")
	}
}

// TestCertificateHosts verifies StatefulSet pod DNS names are included for each
// requested chunkserver replica.
func TestCertificateHosts(t *testing.T) {
	cluster := &gfsv1alpha1.GFSCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "gfs"}}
	spec := normalizeSpec(gfsv1alpha1.GFSClusterSpec{Chunkservers: gfsv1alpha1.ChunkserversSpec{Replicas: 2}})
	hosts := certificateHosts(cluster, spec)
	for _, want := range []string{
		"demo-master.gfs.svc.cluster.local",
		"demo-chunkservers.gfs.svc.cluster.local",
		"demo-chunkserver-0.demo-chunkservers.gfs.svc.cluster.local",
		"demo-chunkserver-1.demo-chunkservers.gfs.svc.cluster.local",
	} {
		if !containsString(hosts, want) {
			t.Fatalf("certificate hosts missing %s: %v", want, hosts)
		}
	}
	if len(hosts) != len(uniqueStrings(hosts)) {
		t.Fatal("certificate hosts contain duplicates")
	}
}

// TestNormalizeSpec verifies defaults and replication clamping.
func TestNormalizeSpec(t *testing.T) {
	spec := normalizeSpec(gfsv1alpha1.GFSClusterSpec{Chunkservers: gfsv1alpha1.ChunkserversSpec{Replicas: 2}})
	if spec.ReplicationFactor != 2 {
		t.Fatalf("replication should clamp to 2, got %d", spec.ReplicationFactor)
	}
	if spec.ChunkSizeBytes != 64<<20 {
		t.Fatalf("unexpected chunk size %d", spec.ChunkSizeBytes)
	}
	if spec.Image.Master == "" || spec.Image.Chunkserver == "" {
		t.Fatal("default images were not populated")
	}
	if spec.Chunkservers.CapacityBytes <= 0 {
		t.Fatal("capacity was not derived")
	}
}

// parseCertificate decodes one PEM certificate.
func parseCertificate(t *testing.T, data []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatal("invalid PEM certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// hasUsage reports whether an extended key usage is present.
func hasUsage(usages []x509.ExtKeyUsage, want x509.ExtKeyUsage) bool {
	for _, usage := range usages {
		if usage == want {
			return true
		}
	}
	return false
}

// containsString reports whether values contains want.
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// uniqueStrings removes duplicates to validate SAN normalization.
func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}
