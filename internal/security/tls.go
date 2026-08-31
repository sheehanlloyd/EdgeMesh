// Package security builds the TLS material and admin authentication used by
// EdgeMesh's internal surfaces.
//
// All three internal protocols (Raft, edge-control, and peer-cache) use mutual
// TLS. Mutual rather than server-only authentication is the point: these are
// peer protocols where a client that can reach the port is otherwise trusted to
// replicate configuration or read cached objects, so the server must verify who
// is calling, not only the other way round.
package security

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// minTLSVersion is the floor for every EdgeMesh TLS surface. TLS 1.2 and below
// carry cipher suites and renegotiation behaviour that a greenfield internal
// protocol has no reason to accept.
const minTLSVersion = tls.VersionTLS13

// ServerTLS builds a server-side mTLS configuration.
//
// It returns nil when TLS is disabled, which the caller treats as "serve
// plaintext". Configuration validation has already refused that combination in
// production mode, so a nil here can only occur in development.
func ServerTLS(c config.TLS) (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cert, pool, err := loadMaterial(c)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		// RequireAndVerifyClientCert is what makes this mutual TLS. Anything
		// weaker would let any client that can reach the port speak Raft.
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: minTLSVersion,
	}, nil
}

// ClientTLS builds a client-side mTLS configuration.
func ClientTLS(c config.TLS) (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cert, pool, err := loadMaterial(c)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   c.ServerName,
		//nolint:gosec // Guarded: config validation refuses skip_verify in production mode.
		InsecureSkipVerify: c.SkipVerify,
		MinVersion:         minTLSVersion,
	}, nil
}

// loadMaterial reads the key pair and CA bundle.
func loadMaterial(c config.TLS) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, errs.Wrap(errs.ClassValidation, err,
			"load key pair (cert=%q key=%q)", c.CertFile, c.KeyFile)
	}
	pem, err := os.ReadFile(c.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, errs.Wrap(errs.ClassValidation, err, "read CA bundle %q", c.CAFile)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, errs.New(errs.ClassValidation,
			"CA bundle %q contains no usable certificates", c.CAFile)
	}
	return cert, pool, nil
}

// GRPCServerCredentials returns the server option for an internal gRPC surface.
func GRPCServerCredentials(c config.TLS) (grpc.ServerOption, error) {
	cfg, err := ServerTLS(c)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return grpc.Creds(insecure.NewCredentials()), nil
	}
	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// GRPCClientCredentials returns the dial option for an internal gRPC client.
func GRPCClientCredentials(c config.TLS) (grpc.DialOption, error) {
	cfg, err := ClientTLS(c)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(cfg)), nil
}

// ---------------------------------------------------------------------------
// Admin authentication
// ---------------------------------------------------------------------------

// TokenAuthenticator validates an admin bearer token.
type TokenAuthenticator struct {
	enabled bool
	token   []byte
}

// NewTokenAuthenticator resolves the configured token.
//
// The token comes from an environment variable or a file, never from the
// configuration document itself, so configuration stays safe to commit and a
// rotated secret does not require a config change.
func NewTokenAuthenticator(a config.AdminAuth) (*TokenAuthenticator, error) {
	if !a.Enabled {
		return &TokenAuthenticator{enabled: false}, nil
	}
	token, err := a.ResolveAdminToken()
	if err != nil {
		return nil, err
	}
	if len(token) < 16 {
		return nil, errs.New(errs.ClassValidation,
			"admin token must be at least 16 characters; got %d", len(token))
	}
	return &TokenAuthenticator{enabled: true, token: []byte(token)}, nil
}

// Enabled reports whether authentication is active.
func (t *TokenAuthenticator) Enabled() bool { return t != nil && t.enabled }

// ErrUnauthorized is returned for a missing or invalid credential.
var ErrUnauthorized = errs.New(errs.ClassUnauthorized, "missing or invalid admin credentials")

// Authenticate validates an Authorization header value.
//
// Comparison is constant-time: a byte-by-byte comparison that returns early
// leaks the token's prefix through response timing, which is enough to recover
// it one byte at a time.
func (t *TokenAuthenticator) Authenticate(authorization string) error {
	if !t.Enabled() {
		return nil
	}
	const prefix = "Bearer "
	if len(authorization) <= len(prefix) || !strings.EqualFold(authorization[:len(prefix)], prefix) {
		return ErrUnauthorized
	}
	got := []byte(strings.TrimSpace(authorization[len(prefix):]))
	if subtle.ConstantTimeCompare(got, t.token) != 1 {
		return ErrUnauthorized
	}
	return nil
}

// Middleware rejects unauthenticated requests before any expensive parsing.
//
// Authentication runs first so an unauthenticated caller cannot make the
// process decode a large JSON body, which would otherwise be free work an
// attacker can request.
func (t *TokenAuthenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := t.Authenticate(r.Header.Get("Authorization")); err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="edgemesh"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
