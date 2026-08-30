package health

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestLivenessIsIndependentOfChecks(t *testing.T) {
	r := NewRegistry()
	// A failing readiness check must never affect liveness: an orchestrator
	// that restarts a process because a dependency is down turns a partial
	// outage into a total one.
	r.Register("dependency", func() error { return errors.New("dependency is down") })

	w := httptest.NewRecorder()
	r.LivenessHandler()(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("liveness = %d, want 200 despite a failing readiness check", w.Code)
	}

	w = httptest.NewRecorder()
	r.ReadinessHandler()(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness = %d, want 503", w.Code)
	}
}

func TestReadinessAggregatesChecks(t *testing.T) {
	r := NewRegistry()
	r.Register("a", func() error { return nil })
	r.Register("b", func() error { return nil })

	rep := r.Ready()
	if rep.Status != "ready" || len(rep.Checks) != 2 {
		t.Fatalf("report = %+v", rep)
	}
	// Checks are sorted so the response is stable for humans and tests alike.
	if rep.Checks[0].Name != "a" || rep.Checks[1].Name != "b" {
		t.Fatalf("checks are not sorted: %+v", rep.Checks)
	}

	r.Register("b", func() error { return errors.New("boom") })
	rep = r.Ready()
	if rep.Status != "not_ready" {
		t.Fatal("one failing check must make the whole report not ready")
	}
	for _, c := range rep.Checks {
		if c.Name == "b" && (c.Status != "not_ready" || c.Error == "") {
			t.Fatalf("failing check not reported: %+v", c)
		}
		if c.Name == "a" && c.Status != "ready" {
			t.Fatal("a passing check was marked not ready")
		}
	}
}

func TestSetLiveDrivesShutdownResponse(t *testing.T) {
	r := NewRegistry()
	r.SetLive(false)
	w := httptest.NewRecorder()
	r.LivenessHandler()(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("liveness during shutdown = %d, want 503", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "shutting_down" {
		t.Fatalf("body = %v", body)
	}
}

func TestDeregister(t *testing.T) {
	r := NewRegistry()
	r.Register("temp", func() error { return errors.New("failing") })
	if r.Ready().Status != "not_ready" {
		t.Fatal("expected not ready")
	}
	r.Deregister("temp")
	if r.Ready().Status != "ready" {
		t.Fatal("deregistering a failing check must restore readiness")
	}
}

func TestEmptyRegistryIsReady(t *testing.T) {
	// A process with no registered dependencies is ready by definition.
	if NewRegistry().Ready().Status != "ready" {
		t.Fatal("an empty registry must report ready")
	}
}

func TestHandlersSetNoStore(t *testing.T) {
	r := NewRegistry()
	for name, h := range map[string]http.HandlerFunc{
		"healthz": r.LivenessHandler(),
		"readyz":  r.ReadinessHandler(),
	} {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodGet, "/"+name, nil))
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s must not be cacheable", name)
		}
		if w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("%s content type = %q", name, w.Header().Get("Content-Type"))
		}
	}
}

// Run with -race.
func TestRegistryConcurrency(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				name := string(rune('a' + i%5))
				switch i % 4 {
				case 0:
					r.Register(name, func() error { return nil })
				case 1:
					r.Deregister(name)
				case 2:
					r.Ready()
				case 3:
					r.SetLive(i%2 == 0)
					r.Live()
				}
			}
		}(w)
	}
	wg.Wait()
}
