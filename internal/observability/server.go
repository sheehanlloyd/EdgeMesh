package observability

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/health"
)

// TelemetryServer hosts /metrics, /healthz, /readyz and, in development mode,
// the pprof endpoints.
//
// It is a separate listener from both the public proxy and the admin API. That
// separation is what lets an operator expose telemetry to a scrape network
// without exposing it to the internet, and it keeps a saturated public listener
// from making the process look dead to its liveness probe.
type TelemetryServer struct {
	srv    *http.Server
	ln     net.Listener
	log    *slog.Logger
	errCh  chan error
	closed bool
}

// TelemetryOptions configures the telemetry listener.
type TelemetryOptions struct {
	Address string
	Metrics *Metrics
	Health  *health.Registry
	Logger  *slog.Logger
	Pprof   bool
	// BuildInfo is served at /version for demo and support purposes.
	BuildInfo map[string]string
}

// NewTelemetryServer binds the telemetry listener.
//
// Binding happens here rather than in Start so a port conflict is reported
// during startup, while the process can still exit cleanly, instead of
// surfacing asynchronously after the process claims to be running.
func NewTelemetryServer(o TelemetryOptions) (*TelemetryServer, error) {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	mux := http.NewServeMux()

	if o.Metrics != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(o.Metrics.Registry(), promhttp.HandlerOpts{
			// A broken collector must not take down the scrape endpoint; report
			// the error to the scraper and keep serving the rest.
			ErrorHandling: promhttp.ContinueOnError,
		}))
	}
	if o.Health != nil {
		mux.HandleFunc("/healthz", o.Health.LivenessHandler())
		mux.HandleFunc("/readyz", o.Health.ReadinessHandler())
	}
	if len(o.BuildInfo) > 0 {
		info := o.BuildInfo
		mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = writeJSONMap(w, info)
		})
	}
	if o.Pprof {
		// pprof is gated by configuration and refused outright in production
		// mode: these handlers expose heap contents and allow CPU-consuming
		// profiles to be triggered by anyone who can reach the port.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	ln, err := net.Listen("tcp", o.Address)
	if err != nil {
		return nil, errs.Wrap(errs.ClassUnavailable, err, "bind telemetry listener %q", o.Address)
	}

	return &TelemetryServer{
		srv: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      60 * time.Second, // pprof profiles run for 30s by default
			IdleTimeout:       60 * time.Second,
		},
		ln:    ln,
		log:   o.Logger,
		errCh: make(chan error, 1),
	}, nil
}

// Addr reports the bound address, which is useful when the configuration asked
// for port 0.
func (s *TelemetryServer) Addr() string { return s.ln.Addr().String() }

// Start serves in the background.
func (s *TelemetryServer) Start() {
	go func() {
		err := s.srv.Serve(s.ln)
		if err != nil && err != http.ErrServerClosed {
			s.errCh <- err
			s.log.Error("telemetry listener failed", slog.String("error", err.Error()))
			return
		}
		s.errCh <- nil
	}()
	s.log.Info("telemetry listener started", slog.String("address", s.Addr()))
}

// Shutdown stops the listener, waiting up to ctx's deadline for in-flight
// scrapes.
func (s *TelemetryServer) Shutdown(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.srv.Shutdown(ctx); err != nil {
		return errs.Wrap(errs.ClassTimeout, err, "shut down telemetry listener")
	}
	return nil
}

func writeJSONMap(w http.ResponseWriter, m map[string]string) error {
	// Hand-rolled to avoid pulling encoding/json into this file's hot path;
	// the map is small, fixed, and free of characters needing escapes.
	if _, err := w.Write([]byte("{")); err != nil {
		return err
	}
	first := true
	for k, v := range m {
		if !first {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false
		if _, err := w.Write([]byte(`"` + k + `":"` + v + `"`)); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte("}"))
	return err
}
