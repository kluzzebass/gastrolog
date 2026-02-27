// Package tlsutil provides certificate and token generation for cluster mTLS.
//
// All functions are pure crypto utilities with no state. The generated
// certificates use ECDSA P-256 with 10-year validity.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"time"
)

// CAKeyPair holds a self-signed CA certificate and its private key as PEM.
type CAKeyPair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// ClusterKeyPair holds a cluster certificate and its private key as PEM.
type ClusterKeyPair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// GenerateCA creates a self-signed ECDSA P-256 CA certificate with 10-year validity.
func GenerateCA() (CAKeyPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return CAKeyPair{}, fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return CAKeyPair{}, err
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "gastrolog-cluster-ca",
			Organization: []string{"gastrolog"},
		},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return CAKeyPair{}, fmt.Errorf("create CA certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return CAKeyPair{}, fmt.Errorf("marshal CA key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return CAKeyPair{CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// GenerateClusterCert creates an ECDSA P-256 certificate signed by the given CA.
// The certificate has both ServerAuth and ClientAuth ExtKeyUsage (shared cert model).
// SANs include localhost, 127.0.0.1, and any additional SANs provided.
func GenerateClusterCert(caCertPEM, caKeyPEM []byte, extraSANs []string) (ClusterKeyPair, error) {
	caCert, caKey, err := parseCA(caCertPEM, caKeyPEM)
	if err != nil {
		return ClusterKeyPair{}, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return ClusterKeyPair{}, fmt.Errorf("generate cluster key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return ClusterKeyPair{}, err
	}

	// Build SANs: always include localhost, 127.0.0.1, and ::1.
	dnsNames := []string{"localhost"}
	ipAddrs := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}

	for _, san := range extraSANs {
		if ip := net.ParseIP(san); ip != nil {
			ipAddrs = append(ipAddrs, ip)
		} else {
			dnsNames = append(dnsNames, san)
		}
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "gastrolog-cluster",
			Organization: []string{"gastrolog"},
		},
		NotBefore:   now,
		NotAfter:    now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    dnsNames,
		IPAddresses: ipAddrs,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return ClusterKeyPair{}, fmt.Errorf("create cluster certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return ClusterKeyPair{}, fmt.Errorf("marshal cluster key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return ClusterKeyPair{CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// GenerateJoinToken creates a join token in the format "<hex-secret>:<hex-sha256(CA DER)>".
// The secret is 32 bytes of random data. The CA hash enables trust-on-first-use (TOFU).
func GenerateJoinToken(caCertPEM []byte) (string, error) {
	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return "", errors.New("decode CA PEM: no PEM block found")
	}

	caHash := sha256.Sum256(block.Bytes)

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", fmt.Errorf("generate token secret: %w", err)
	}

	return hex.EncodeToString(secret) + ":" + hex.EncodeToString(caHash[:]), nil
}

// ParseJoinToken splits a join token into its secret and CA hash components.
func ParseJoinToken(token string) (secret, caHash string, err error) {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid join token format: expected <secret>:<ca-hash>")
	}
	secret, caHash = parts[0], parts[1]

	if _, err := hex.DecodeString(secret); err != nil {
		return "", "", fmt.Errorf("invalid join token secret: %w", err)
	}
	if _, err := hex.DecodeString(caHash); err != nil {
		return "", "", fmt.Errorf("invalid join token CA hash: %w", err)
	}
	return secret, caHash, nil
}

// VerifyCAFingerprint checks whether the given CA certificate PEM matches
// the expected SHA-256 hash (hex-encoded).
func VerifyCAFingerprint(caCertPEM []byte, expectedHash string) (bool, error) {
	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return false, errors.New("decode CA PEM: no PEM block found")
	}

	actual := sha256.Sum256(block.Bytes)
	return hex.EncodeToString(actual[:]) == expectedHash, nil
}

// parseCA decodes PEM-encoded CA certificate and private key.
func parseCA(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, nil, errors.New("decode CA cert PEM: no PEM block found")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, errors.New("decode CA key PEM: no PEM block found")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA private key: %w", err)
	}

	return cert, key, nil
}

func randomSerial() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}
	return serial, nil
}
