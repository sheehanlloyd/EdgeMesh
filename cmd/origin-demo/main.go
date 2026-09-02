// Command origin-demo is EdgeMesh's deterministic demo and test origin.
//
// It exists so the cache, retry, timeout, and failure behaviour of the proxy
// can be demonstrated and tested without any external dependency. Every
// endpoint is deterministic: the same request produces the same bytes, which is
// what makes a cache hit provable rather than merely plausible.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	var (
		addr     = flag.String("addr", ":9000", "listen address")
		id       = flag.String("id", "", "origin instance id (defaults to the hostname)")
		maxDelay = flag.Duration("max-delay", 30*time.Second, "upper bound accepted by /delay")
		maxBytes = flag.Int("max-bytes", 32<<20, "upper bound accepted by /bytes")
		logLevel = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()

	instanceID := *id
	if instanceID == "" {
		if h, err := os.Hostname(); err == nil {
			instanceID = h
		} else {
			instanceID = "origin"
		}
	}

	level := slog.LevelInfo
	switch strings.ToLower(*logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).
		With(slog.String("service", "origin-demo"), slog.String("instance", instanceID))

	srv := newServer(instanceID, *maxDelay, *maxBytes, log)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("demo origin listening", slog.String("address", *addr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		log.Error("listener failed", slog.String("error", err.Error()))
		os.Exit(1)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

// server implements the demo origin's endpoints.
type server struct {
	id       string
	maxDelay time.Duration
	maxBytes int
	log      *slog.Logger
	mux      *http.ServeMux

	// counters back /counter/{id}, which is how a demo proves the origin was
	// not contacted on a cache hit.
	mu       sync.Mutex
	counters map[string]int64
	requests atomic.Int64
}

func newServer(id string, maxDelay time.Duration, maxBytes int, log *slog.Logger) *server {
	s := &server{
		id: id, maxDelay: maxDelay, maxBytes: maxBytes, log: log,
		mux:      http.NewServeMux(),
		counters: make(map[string]int64),
	}
	s.mux.HandleFunc("GET /static/{id}", s.handleStatic)
	s.mux.HandleFunc("GET /dynamic/{id}", s.handleDynamic)
	s.mux.HandleFunc("GET /delay/{ms}", s.handleDelay)
	s.mux.HandleFunc("GET /status/{code}", s.handleStatus)
	s.mux.HandleFunc("GET /bytes/{n}", s.handleBytes)
	s.mux.HandleFunc("GET /counter/{id}", s.handleCounter)
	s.mux.HandleFunc("POST /counter/{id}/reset", s.handleCounterReset)
	s.mux.HandleFunc("GET /vary", s.handleVary)
	s.mux.HandleFunc("GET /private", s.handlePrivate)
	s.mux.HandleFunc("GET /setcookie", s.handleSetCookie)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /stats", s.handleStats)
	s.mux.HandleFunc("/", s.handleRoot)
	return s
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	// Every response identifies its instance so load balancing across origins
	// is visible in a demo without reading logs.
	w.Header().Set("X-Origin-Instance", s.id)
	w.Header().Set("X-Origin-Time", time.Now().UTC().Format(time.RFC3339Nano))
	s.mux.ServeHTTP(w, r)
}

// handleStatic serves a cacheable, byte-stable body.
func (s *server) handleStatic(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	maxAge := 60
	if v := r.URL.Query().Get("max-age"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxAge = n
		}
	}
	body := deterministicBody(id)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(maxAge))
	w.Header().Set("ETag", `"`+id+`"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write([]byte(body))
}

// handleDynamic serves an explicitly uncacheable response.
func (s *server) handleDynamic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(w, "dynamic %s at %s from %s\n",
		r.PathValue("id"), time.Now().UTC().Format(time.RFC3339Nano), s.id)
}

// handleDelay sleeps before responding, for timeout and coalescing demos.
func (s *server) handleDelay(w http.ResponseWriter, r *http.Request) {
	ms, err := strconv.Atoi(r.PathValue("ms"))
	if err != nil || ms < 0 {
		http.Error(w, "delay must be a non-negative integer of milliseconds", http.StatusBadRequest)
		return
	}
	d := time.Duration(ms) * time.Millisecond
	if d > s.maxDelay {
		d = s.maxDelay
	}
	// The sleep respects cancellation so an abandoned request frees its
	// goroutine immediately rather than after the full delay.
	select {
	case <-time.After(d):
	case <-r.Context().Done():
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	fmt.Fprintf(w, "delayed %s from %s\n", d, s.id)
}

// handleStatus returns a requested status code.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	code, err := strconv.Atoi(r.PathValue("code"))
	if err != nil || code < 100 || code > 599 {
		http.Error(w, "status must be within [100,599]", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	fmt.Fprintf(w, "status %d from %s\n", code, s.id)
}

// handleBytes returns a deterministic body of the requested size.
func (s *server) handleBytes(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil || n < 0 {
		http.Error(w, "size must be a non-negative integer", http.StatusBadRequest)
		return
	}
	if n > s.maxBytes {
		http.Error(w, "size exceeds this origin's limit", http.StatusRequestEntityTooLarge)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("Content-Length", strconv.Itoa(n))

	// Written in bounded chunks so a large request does not allocate the whole
	// body at once.
	const chunk = 64 << 10
	buf := make([]byte, chunk)
	for i := range buf {
		buf[i] = byte('A' + i%26)
	}
	for written := 0; written < n; {
		size := chunk
		if remaining := n - written; remaining < size {
			size = remaining
		}
		m, err := w.Write(buf[:size])
		if err != nil {
			return
		}
		written += m
	}
}

// handleCounter increments and reports a counter.
//
// This is the endpoint that makes a cache hit provable: if the counter does not
// advance, the origin was genuinely not contacted.
func (s *server) handleCounter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	maxAge := 60
	if v := r.URL.Query().Get("max-age"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxAge = n
		}
	}
	s.mu.Lock()
	s.counters[id]++
	n := s.counters[id]
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(maxAge))
	w.Header().Set("X-Origin-Hits", strconv.FormatInt(n, 10))
	fmt.Fprintf(w, `{"counter":%q,"origin_hits":%d,"instance":%q}`+"\n", id, n, s.id)
}

func (s *server) handleCounterReset(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	delete(s.counters, id)
	s.mu.Unlock()
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(w, `{"counter":%q,"reset":true}`+"\n", id)
}

// handleVary exercises Vary-aware cache keying.
func (s *server) handleVary(w http.ResponseWriter, r *http.Request) {
	enc := r.Header.Get("Accept-Encoding")
	w.Header().Set("Vary", "Accept-Encoding")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "variant for accept-encoding %q from %s\n", enc, s.id)
}

// handlePrivate returns a response no shared cache may store.
func (s *server) handlePrivate(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "private, max-age=300")
	fmt.Fprintf(w, "private response from %s\n", s.id)
}

// handleSetCookie returns a response carrying Set-Cookie, which must not be
// cached by default.
func (s *server) handleSetCookie(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Set-Cookie", "session=demo; Path=/; HttpOnly")
	w.Header().Set("Cache-Control", "public, max-age=300")
	fmt.Fprintf(w, "response with a session cookie from %s\n", s.id)
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","instance":%q}`+"\n", s.id)
}

func (s *server) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	counters := make(map[string]int64, len(s.counters))
	for k, v := range s.counters {
		counters[k] = v
	}
	s.mu.Unlock()

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"instance":%q,"requests":%d,"counters":{`, s.id, s.requests.Load())
	first := true
	for k, v := range counters {
		if !first {
			fmt.Fprint(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q:%d", k, v)
	}
	fmt.Fprint(w, "}}\n")
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(w, `EdgeMesh demo origin %q

  GET  /static/{id}            cacheable, byte-stable body (?max-age=N)
  GET  /dynamic/{id}           no-store
  GET  /delay/{ms}             intentional latency
  GET  /status/{code}          intentional status
  GET  /bytes/{n}              deterministic body of n bytes
  GET  /counter/{id}           increments an origin-hit counter (?max-age=N)
  POST /counter/{id}/reset     resets that counter
  GET  /vary                   Vary: Accept-Encoding
  GET  /private                Cache-Control: private
  GET  /setcookie              carries Set-Cookie
  GET  /healthz                health check
  GET  /stats                  request and counter totals
`, s.id)
}

// deterministicBody builds a stable body for an id. The same id always yields
// the same bytes, which is what lets a test assert a cache hit by comparing
// response bodies.
func deterministicBody(id string) string {
	var b strings.Builder
	b.WriteString("edgemesh-static-object:")
	b.WriteString(id)
	b.WriteString("\n")
	for i := 0; i < 8; i++ {
		fmt.Fprintf(&b, "line-%d:%s\n", i, id)
	}
	return b.String()
}
