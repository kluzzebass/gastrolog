// Package tlsutil provides certificate and token generation for cluster mTLS.
//
// All functions are pure crypto utilities with no state. The generated
// certificates use ECDSA P-256 with 10-year validity.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
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

// GenerateNodeCert creates an ECDSA P-256 certificate signed by the given CA,
// naming nodeID as its subject. Both key and certificate are produced here,
// which is what the bootstrap node does for itself: it generated the CA a
// moment ago and has no one to ask. Every other node arrives through
// SignCSR, so its key never leaves it.
//
// The certificate has both ServerAuth and ClientAuth ExtKeyUsage: a node is a
// server to its peers and a client to them in the same breath. SANs include
// localhost, 127.0.0.1, and any additional SANs provided.
func GenerateNodeCert(caCertPEM, caKeyPEM []byte, nodeID string, extraSANs []string) (ClusterKeyPair, error) {
	caCert, caKey, err := parseCA(caCertPEM, caKeyPEM)
	if err != nil {
		return ClusterKeyPair{}, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return ClusterKeyPair{}, fmt.Errorf("generate node key: %w", err)
	}

	certPEM, err := issueNodeCert(caCert, caKey, &key.PublicKey, nodeID, extraSANs)
	if err != nil {
		return ClusterKeyPair{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return ClusterKeyPair{}, fmt.Errorf("marshal node key: %w", err)
	}
	return ClusterKeyPair{
		CertPEM: certPEM,
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// issueNodeCert signs a certificate for pub naming nodeID.
func issueNodeCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, pub *ecdsa.PublicKey, nodeID string, extraSANs []string) ([]byte, error) {
	if nodeID == "" {
		return nil, errors.New("node certificate needs a node ID: an unnamed certificate identifies nothing")
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
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
		// The node ID is the subject, and NodeIDFromCert reads it back. It is
		// what lets a peer say which node it is talking to instead of only
		// that the caller holds something the cluster signed.
		Subject: pkix.Name{
			CommonName:   nodeID,
			Organization: []string{"gastrolog"},
		},
		NotBefore:   now,
		NotAfter:    now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    dnsNames,
		IPAddresses: ipAddrs,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, pub, caKey)
	if err != nil {
		return nil, fmt.Errorf("create node certificate: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), nil
}

// NodeIDFromCert returns the node ID a certificate names, which is its
// subject common name. An empty result means the certificate names no node
// and cannot be checked against the cluster's membership.
func NodeIDFromCert(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	return cert.Subject.CommonName
}

// GenerateCSR creates a key pair and a certificate signing request naming
// nodeID, returning the request and the private key.
//
// The key is the point: it is generated by the node that will use it and
// never travels. A signed certificate crossing the wire is a statement about
// a key; the key itself crossing the wire would make the certificate a
// statement about nobody.
func GenerateCSR(nodeID string) (csrPEM, keyPEM []byte, err error) {
	if nodeID == "" {
		return nil, nil, errors.New("certificate request needs a node ID")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate node key: %w", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: nodeID, Organization: []string{"gastrolog"}},
	}, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate request: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal node key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

// SignCSR issues a certificate for the public key in csrPEM, naming nodeID.
//
// nodeID comes from the caller, not from the request: the subject a joiner
// asks for is a request, and honouring it would let anyone holding the join
// token name themselves whatever they liked. Everything the certificate
// asserts is decided here; the request contributes only a public key, and
// only after proving the sender holds the matching private one.
func SignCSR(caCertPEM, caKeyPEM, csrPEM []byte, nodeID string, extraSANs []string) ([]byte, error) {
	caCert, caKey, err := parseCA(caCertPEM, caKeyPEM)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		return nil, errors.New("decode certificate request PEM: no PEM block found")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate request: %w", err)
	}
	// Without this the request carries an unproven public key: anyone could
	// replay someone else's and be issued a certificate for a key they do
	// not hold.
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("certificate request is not signed by the key it presents: %w", err)
	}
	pub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("certificate request must present an ECDSA public key")
	}
	return issueNodeCert(caCert, caKey, pub, nodeID, extraSANs)
}

// DefaultJoinTokenTTL is how long a minted join token stays usable.
//
// A token is minted when someone asks for one — a joiner fetching it at its
// own startup, or an operator about to paste it into a terminal — so the
// window only has to cover the gap between asking and joining, which is
// seconds in the automatic case and minutes in the attended one. An hour is
// generous for both and short enough that a token found later is dead.
const DefaultJoinTokenTTL = time.Hour

// ErrJoinTokenExpired reports a token whose validity window has passed. It is
// distinguished from a wrong token so an operator reading a log can tell
// "mint a new one" apart from "this one was never valid".
var ErrJoinTokenExpired = errors.New("join token expired")

// GenerateJoinTokenKey creates the secret a cluster mints join tokens with.
// It is the durable credential and never leaves the cluster: what an operator
// copies is a token minted from it, which expires.
func GenerateJoinTokenKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate join token key: %w", err)
	}
	return key, nil
}

// MintJoinToken issues a join token valid for ttl from now, in the format
// "<expiry-unix>.<hmac>:<hex-sha256(CA DER)>".
//
// The two halves do different jobs. Before the colon is the credential: an
// expiry the cluster can check and an HMAC over it that only a holder of the
// key could have produced, so nothing has to be stored per token and a token
// cannot be extended by editing its expiry. After the colon is the CA
// fingerprint, which is public and lets a joiner verify who it is talking to
// before it sends the credential (trust-on-first-use).
func MintJoinToken(key, caCertPEM []byte, ttl time.Duration) (string, error) {
	if len(key) == 0 {
		return "", errors.New("mint join token: no key")
	}
	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return "", errors.New("decode CA PEM: no PEM block found")
	}
	caHash := sha256.Sum256(block.Bytes)
	expiry := strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)
	return expiry + "." + hex.EncodeToString(signJoinToken(key, expiry)) +
		":" + hex.EncodeToString(caHash[:]), nil
}

// VerifyJoinToken checks the credential half of a join token against the
// cluster's key, and that its window has not passed.
//
// The HMAC is checked before the expiry: a caller that can learn "expired"
// from an unsigned string learns that the string was otherwise well-formed,
// and there is nothing to be gained by telling it so.
func VerifyJoinToken(key []byte, credential string, now time.Time) error {
	if len(key) == 0 {
		return errors.New("verify join token: no key")
	}
	expiry, mac, ok := strings.Cut(credential, ".")
	if !ok {
		return errors.New("invalid join token: expected <expiry>.<hmac>")
	}
	presented, err := hex.DecodeString(mac)
	if err != nil {
		return fmt.Errorf("invalid join token signature: %w", err)
	}
	if subtle.ConstantTimeCompare(presented, signJoinToken(key, expiry)) != 1 {
		return errors.New("invalid join token")
	}
	seconds, err := strconv.ParseInt(expiry, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid join token expiry: %w", err)
	}
	if now.After(time.Unix(seconds, 0)) {
		return ErrJoinTokenExpired
	}
	return nil
}

// signJoinToken is the HMAC binding an expiry to the cluster that issued it.
func signJoinToken(key []byte, expiry string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(expiry))
	return mac.Sum(nil)
}

// ParseJoinToken splits a join token into its credential and CA hash halves.
// The credential is opaque to the joiner, which only forwards it; the CA hash
// is what the joiner itself uses, to verify the node answering.
func ParseJoinToken(token string) (credential, caHash string, err error) {
	credential, caHash, ok := strings.Cut(token, ":")
	if !ok {
		return "", "", errors.New("invalid join token format: expected <credential>:<ca-hash>")
	}
	if credential == "" {
		return "", "", errors.New("invalid join token: empty credential")
	}
	if _, err := hex.DecodeString(caHash); err != nil {
		return "", "", fmt.Errorf("invalid join token CA hash: %w", err)
	}
	return credential, caHash, nil
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
