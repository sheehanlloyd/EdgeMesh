package security

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

func TestDisabledAuthAllowsEverything(t *testing.T) {
	a, err := NewTokenAuthenticator(config.AdminAuth{})
	if err != nil {
		t.Fatal(err)
	}
	if a.Enabled() {
		t.Fatal("auth must be disabled")
	}
	if err := a.Authenticate(""); err != nil {
		t.Fatalf("disabled auth rejected a request: %v", err)
	}
}

func TestTokenAuthentication(t *testing.T) {
	t.Setenv("EDGEMESH_TEST_ADMIN_TOKEN", "correct-horse-battery-staple")
	a, err := NewTokenAuthenticator(config.AdminAuth{Enabled: true, TokenEnv: "EDGEMESH_TEST_ADMIN_TOKEN"})
	if err != nil {
		t.Fatal(err)
	}

	if err := a.Authenticate("Bearer correct-horse-battery-staple"); err != nil {
		t.Fatalf("the correct token was rejected: %v", err)
	}
	// The scheme is case-insensitive per RFC 9110.
	if err := a.Authenticate("bearer correct-horse-battery-staple"); err != nil {
		t.Fatalf("a lowercase scheme was rejected: %v", err)
	}

	for _, bad := range []string{
		"",
		"Bearer",
		"Bearer ",
		"Bearer wrong-token-value-here",
		"correct-horse-battery-staple", // no scheme
		"Basic correct-horse-battery-staple",
		// A prefix of the real token must not pass, which is what constant-time
		// comparison guarantees.
		"Bearer correct-horse-battery-stapl",
		"Bearer correct-horse-battery-staplee",
	} {
		if err := a.Authenticate(bad); err == nil {
			t.Errorf("credential %q was accepted", bad)
		} else if !errs.IsClass(err, errs.ClassUnauthorized) {
			t.Errorf("credential %q produced class %q, want unauthorized", bad, errs.ClassOf(err))
		}
	}
}

// A short token is refused at construction rather than silently accepted: a
// four-character admin token is not meaningfully different from no auth.
func TestShortTokenIsRejected(t *testing.T) {
	t.Setenv("EDGEMESH_TEST_SHORT", "short")
	if _, err := NewTokenAuthenticator(config.AdminAuth{Enabled: true, TokenEnv: "EDGEMESH_TEST_SHORT"}); err == nil {
		t.Fatal("a short admin token must be rejected")
	}
}

func TestMissingTokenSourceIsRejected(t *testing.T) {
	if _, err := NewTokenAuthenticator(config.AdminAuth{Enabled: true, TokenEnv: "EDGEMESH_DOES_NOT_EXIST"}); err == nil {
		t.Fatal("enabled auth with no resolvable token must fail at startup")
	}
}

func TestMiddlewareRejectsBeforeTheHandlerRuns(t *testing.T) {
	t.Setenv("EDGEMESH_TEST_ADMIN_TOKEN", "correct-horse-battery-staple")
	a, err := NewTokenAuthenticator(config.AdminAuth{Enabled: true, TokenEnv: "EDGEMESH_TEST_ADMIN_TOKEN"})
	if err != nil {
		t.Fatal(err)
	}

	reached := false
	h := a.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/routes", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	// The handler must not run: rejecting first is what stops an
	// unauthenticated caller from making the process parse a large body.
	if reached {
		t.Fatal("the protected handler ran for an unauthenticated request")
	}
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Error("a 401 must carry WWW-Authenticate")
	}

	w = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/routes", nil)
	req.Header.Set("Authorization", "Bearer correct-horse-battery-staple")
	h.ServeHTTP(w, req)
	if !reached {
		t.Fatal("an authenticated request did not reach the handler")
	}
}

func TestTLSDisabledYieldsNoConfig(t *testing.T) {
	cfg, err := ServerTLS(config.TLS{Enabled: false})
	if err != nil || cfg != nil {
		t.Fatalf("disabled TLS: %v %v", cfg, err)
	}
	cfg, err = ClientTLS(config.TLS{Enabled: false})
	if err != nil || cfg != nil {
		t.Fatalf("disabled client TLS: %v %v", cfg, err)
	}
}

func TestTLSMissingMaterialIsAnError(t *testing.T) {
	_, err := ServerTLS(config.TLS{Enabled: true, CAFile: "/nope", CertFile: "/nope", KeyFile: "/nope"})
	if err == nil {
		t.Fatal("missing TLS material must be an error, not a silent fallback to plaintext")
	}
	if !errs.IsClass(err, errs.ClassValidation) {
		t.Fatalf("error class = %q, want validation", errs.ClassOf(err))
	}
}

func FuzzAuthenticate(f *testing.F) {
	const token = "correct-horse-battery-staple"
	f.Add("Bearer " + token)
	f.Add("bEaReR " + token)
	f.Add("Bearer  " + token + "  ")
	f.Add("Bearer " + token + " extra")
	f.Add("Bearerx " + token)
	f.Add("Bearer\t" + token)
	f.Add("")
	f.Add("Bearer ")
	f.Fuzz(func(t *testing.T, header string) {
		t.Setenv("EDGEMESH_FUZZ_TOKEN", token)
		a, err := NewTokenAuthenticator(config.AdminAuth{Enabled: true, TokenEnv: "EDGEMESH_FUZZ_TOKEN"})
		if err != nil {
			t.Skip()
		}

		// State the property rather than listing the inputs that satisfy it.
		// An earlier version of this check enumerated three spellings of the
		// scheme, which quietly asserted that "beArer" must be rejected. RFC
		// 7235 makes the auth-scheme case-insensitive, so that spelling is
		// valid and the implementation was right to take it.
		//
		// The property: accept exactly when the header is the scheme "Bearer",
		// in any casing, followed by a single space and then the token, with
		// surrounding whitespace ignored.
		scheme, rest, hasSpace := strings.Cut(header, " ")
		want := hasSpace && strings.EqualFold(scheme, "Bearer") && strings.TrimSpace(rest) == token

		got := a.Authenticate(header) == nil
		if got != want {
			verb := "rejected"
			if got {
				verb = "accepted"
			}
			t.Fatalf("credential %q was %s; want accepted=%v", header, verb, want)
		}
	})
}
