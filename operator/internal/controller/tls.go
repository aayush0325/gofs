// Copyright 2026 The gofs Authors.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sort"
	"time"

	gfsv1alpha1 "github.com/aayush0325/gofs/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// tlsHostsHashAnnotation tracks the SAN set used by a generated secret.
	tlsHostsHashAnnotation = "gfs.gofs.io/tls-hosts-hash"
	// tlsCertHashAnnotation tracks the certificate material for pod rollouts.
	tlsCertHashAnnotation = "gfs.gofs.io/tls-cert-hash"
)

// tlsSecretKeys are the files expected by the master and chunkserver binaries.
var tlsSecretKeys = []string{"ca.crt", "server.crt", "server.key", "client.crt", "client.key"}

// ensureTLSSecret either validates a user-provided secret or creates and owns a
// generated CA, server certificate, and client certificate. It returns the
// secret name and a hash suitable for a pod-template annotation.
func (r *GFSClusterReconciler) ensureTLSSecret(ctx context.Context, cluster *gfsv1alpha1.GFSCluster, spec gfsv1alpha1.GFSClusterSpec) (string, string, error) {
	if spec.TLS.SecretName != "" {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Name: spec.TLS.SecretName, Namespace: cluster.Namespace}, secret); err != nil {
			return "", "", fmt.Errorf("read TLS secret %q: %w", spec.TLS.SecretName, err)
		}
		if err := validateTLSSecret(secret); err != nil {
			return "", "", err
		}
		return spec.TLS.SecretName, hashSecretData(secret.Data), nil
	}

	name := generatedTLSSecretName(cluster)
	hosts := certificateHosts(cluster, spec)
	hostsHash := hashStrings(hosts)
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)
	if err == nil && secret.Annotations[tlsHostsHashAnnotation] == hostsHash && hasTLSKeys(secret) {
		if secret.Annotations[tlsCertHashAnnotation] != "" {
			return name, secret.Annotations[tlsCertHashAnnotation], nil
		}
		return name, hashSecretData(secret.Data), nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return "", "", err
	}
	notFound := apierrors.IsNotFound(err)

	material, certHash, err := generateTLSMaterial(hosts)
	if err != nil {
		return "", "", err
	}
	if notFound {
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	}
	secret.Data = material
	secret.Type = corev1.SecretTypeOpaque
	if secret.Annotations == nil {
		secret.Annotations = map[string]string{}
	}
	secret.Annotations[tlsHostsHashAnnotation] = hostsHash
	secret.Annotations[tlsCertHashAnnotation] = certHash
	if err := controllerutil.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return "", "", fmt.Errorf("own generated TLS secret: %w", err)
	}
	if secret.ResourceVersion == "" {
		if err := r.Create(ctx, secret); err != nil {
			return "", "", fmt.Errorf("create TLS secret: %w", err)
		}
	} else if err := r.Update(ctx, secret); err != nil {
		return "", "", fmt.Errorf("update TLS secret: %w", err)
	}
	return name, certHash, nil
}

// validateTLSSecret verifies that a user secret contains all required files.
func validateTLSSecret(secret *corev1.Secret) error {
	for _, key := range tlsSecretKeys {
		if len(secret.Data[key]) == 0 {
			return fmt.Errorf("TLS secret %q is missing %s", secret.Name, key)
		}
	}
	return nil
}

// hasTLSKeys reports whether all required keys are present.
func hasTLSKeys(secret *corev1.Secret) bool {
	return validateTLSSecret(secret) == nil
}

// generateTLSMaterial creates a CA, shared server certificate, and client
// certificate for internal mTLS.
func generateTLSMaterial(hosts []string) (map[string][]byte, string, error) {
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", fmt.Errorf("generate CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "gofs-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, "", fmt.Errorf("create CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, "", fmt.Errorf("parse CA certificate: %w", err)
	}

	dnsNames, ipAddresses := splitHosts(hosts)
	serverKey, serverDER, err := signedCertificate(caCert, caKey, "gfs-server", dnsNames, ipAddresses, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now)
	if err != nil {
		return nil, "", err
	}
	clientKey, clientDER, err := signedCertificate(caCert, caKey, "gfs-client", []string{"gfs-client", "localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, now)
	if err != nil {
		return nil, "", err
	}

	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return nil, "", fmt.Errorf("marshal server key: %w", err)
	}
	clientKeyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		return nil, "", fmt.Errorf("marshal client key: %w", err)
	}
	material := map[string][]byte{
		"ca.crt":     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		"server.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		"server.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}),
		"client.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		"client.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyDER}),
	}
	return material, hashSecretData(material), nil
}

// signedCertificate signs one leaf certificate with the generated CA.
func signedCertificate(ca *x509.Certificate, caKey *ecdsa.PrivateKey, commonName string, dns []string, ips []net.IP, usages []x509.ExtKeyUsage, now time.Time) (*ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate %s key: %w", commonName, err)
	}
	template := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           usages,
		BasicConstraintsValid: true,
		DNSNames:              dns,
		IPAddresses:           ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create %s certificate: %w", commonName, err)
	}
	return key, der, nil
}

// splitHosts separates DNS SANs from IP SANs and drops empty values.
func splitHosts(hosts []string) ([]string, []net.IP) {
	var dns []string
	var ips []net.IP
	for _, host := range hosts {
		if host == "" {
			continue
		}
		if ip := net.ParseIP(host); ip != nil {
			ips = append(ips, ip)
		} else {
			dns = append(dns, host)
		}
	}
	return dns, ips
}

// randomSerial returns a random 128-bit certificate serial number.
func randomSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return serial
}

// hashStrings returns a deterministic SHA-256 over sorted values.
func hashStrings(values []string) string {
	copied := append([]string(nil), values...)
	sort.Strings(copied)
	hash := sha256.New()
	for _, value := range copied {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// hashSecretData returns a deterministic SHA-256 over TLS secret contents.
func hashSecretData(data map[string][]byte) string {
	hash := sha256.New()
	for _, key := range tlsSecretKeys {
		_, _ = hash.Write([]byte(key))
		_, _ = hash.Write(data[key])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
