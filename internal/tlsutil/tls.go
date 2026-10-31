// Package tlsutil loads the CA and certificates used for mutual TLS.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerConfig returns a TLS configuration that requires and verifies a client
// certificate signed by the configured CA.
func ServerConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	pool, err := loadCAPool(caFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		NextProtos:   []string{"h2"},
	}, nil
}

// ClientConfig returns a TLS configuration with a client certificate. When
// serverName is non-empty it overrides the host name used for verification;
// otherwise the dial target's host is used.
func ClientConfig(caFile, certFile, keyFile, serverName string) (*tls.Config, error) {
	pool, err := loadCAPool(caFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client certificate: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		NextProtos:   []string{"h2"},
	}, nil
}

// loadCAPool reads a PEM CA bundle.
func loadCAPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file %s contains no certificates", caFile)
	}
	return pool, nil
}
