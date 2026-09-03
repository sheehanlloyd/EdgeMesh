// Command edge runs an EdgeMesh edge node.
//
// An edge owns the eventually consistent half of the system: it proxies client
// traffic, caches responses in two tiers, participates in the consistent hash
// ring, and follows configuration published by the control plane.
//
// The defining property of this process is that it keeps serving when the
// control plane is unreachable. It holds the last valid configuration snapshot
// in memory and stays *ready* while it can serve traffic; only the ability to
// receive new configuration is lost. See docs/architecture.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/breaker"
	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l2"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/health"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/peer"
	"github.com/sheehanlloyd/edgemesh/internal/proxy"
	"github.com/sheehanlloyd/edgemesh/internal/raft/transport"
	"github.com/sheehanlloyd/edgemesh/internal/ring"
	"github.com/sheehanlloyd/edgemesh/internal/security"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "edgemesh-edge: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to the edge configuration file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("edgemesh-edge", version)
		return nil
	}
	if *configPath == "" {
		return errs.New(errs.ClassValidation, "--config is required")
	}

	cfg, err := config.LoadEdge(*configPath)
	if err != nil {
		return err
	}

	log := observability.NewLogger(observability.LoggerOptions{
		Level: cfg.Telemetry.LogLevel, Format: cfg.Telemetry.LogFormat,
		Service: "edgemesh-edge", NodeID: cfg.Node.ID,
		Region: cfg.Node.Region, Zone: cfg.Node.Zone,
	})
	log.Info("starting edgemesh edge node",
		slog.String("version", version),
		slog.String("mode", string(cfg.Mode)),
		slog.String("public_address", cfg.Node.PublicAddress),
		slog.String("peer_address", cfg.Node.PeerAddress),
		slog.String("advertise_peer_address", cfg.Node.AdvertisePeerAddress))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Background work that must outlive the shutdown *signal* runs under its own
	// context, canceled explicitly after the drain rather than by SIGTERM.
	//
	// The cache coordinator is the reason. Its coalescer derives every origin
	// fill from this context; if it were the signal context, SIGTERM would
	// cancel fills that draining requests are still waiting on, and those
	// requests would fail with a gateway timeout. That would defeat the whole
	// point of a graceful drain.
	backgroundCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()

	metrics := observability.NewMetrics()
	healthReg := health.NewRegistry()

	tracing, err := observability.InitTracing(ctx, observability.TracingOptions{
		ServiceName: "edgemesh-edge", ServiceVersion: version,
		Namespace: cfg.Telemetry.ServiceNamespace, NodeID: cfg.Node.ID,
		Region: cfg.Node.Region, Zone: cfg.Node.Zone,
		OTLPEndpoint: cfg.Telemetry.OTLPEndpoint, Insecure: cfg.Telemetry.OTLPInsecure,
		SampleRatio: cfg.Telemetry.TraceSampleRatio,
	})
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracing.Shutdown(sctx); err != nil {
			log.Warn("telemetry shutdown failed", slog.String("error", err.Error()))
		}
	}()

	// --- caches -----------------------------------------------------------
	l1Cache, err := newTier(cfg, cfg.Cache.L1MaxBytes, cache.TierL1, metrics)
	if err != nil {
		return err
	}
	defer closeTier(l1Cache, cache.TierL1, log)

	l2Tier, err := newTier(cfg, cfg.Cache.L2MaxBytes, cache.TierL2, metrics)
	if err != nil {
		return err
	}
	defer closeTier(l2Tier, cache.TierL2, log)

	ringHolder := ring.NewHolder()
	registry := newConfigRegistry(cfg, log, metrics)

	// --- origin transport --------------------------------------------------
	guard, err := origin.NewGuard(cfg.OriginSecurity)
	if err != nil {
		return err
	}
	registry.guard = guard

	originTransport := proxy.NewOriginTransport(cfg.Proxy, guard)
	breakers := breaker.NewGroup(breaker.Options{
		FailureThreshold: 5,
		FailureRatio:     0.5,
		MinimumRequests:  10,
		Window:           10 * time.Second,
		Cooldown:         5 * time.Second,
		HalfOpenProbes:   2,
		SuccessesToClose: 2,
	})
	registry.breakers = breakers
	breakers.OnTransition(func(originID string, _, to breaker.State) {
		metrics.CircuitTransitions.WithLabelValues(originID, string(to)).Inc()
		log.Warn("circuit breaker state changed",
			slog.String(observability.FieldOriginID, originID),
			slog.String("to_state", string(to)))
	})
	originClient := proxy.NewOriginClient(originTransport, breakers)
	originClient.OnAttempt = func(r proxy.AttemptResult) {
		result := "ok"
		if r.Err != nil {
			result = string(errs.ClassOf(r.Err))
		} else if r.Status >= 500 {
			result = "5xx"
		}
		metrics.OriginRequests.WithLabelValues(r.OriginID, result).Inc()
		if r.Duration > 0 {
			metrics.OriginDuration.WithLabelValues(r.OriginID).Observe(r.Duration.Seconds())
		}
		if r.Retryable && r.RetryReason != "" {
			metrics.OriginRetries.WithLabelValues(r.OriginID, r.RetryReason).Inc()
		}
	}

	trusted, err := proxy.NewTrustedProxies(cfg.Proxy.TrustedProxyCIDRs)
	if err != nil {
		return errs.Wrap(errs.ClassValidation, err, "parse proxy.trusted_proxy_cidrs")
	}

	fetcher, err := proxy.NewFetcher(proxy.FetcherOptions{
		NodeID: cfg.Node.ID, Routes: registry.Routes(), Pools: registry,
		Origin: originClient, Trusted: trusted, Logger: log,
		MaxObjectBytes: cfg.Cache.MaxObjectBytes,
		OnAdmission: func(tier cache.Tier, reason string) {
			metrics.CacheAdmissions.WithLabelValues(string(tier), reason).Inc()
		},
	})
	if err != nil {
		return err
	}

	// --- peer cache --------------------------------------------------------
	peerCreds, err := security.GRPCClientCredentials(cfg.Security)
	if err != nil {
		return err
	}
	peerClient, err := peer.NewClient(peer.ClientOptions{
		Dialer: func(_ context.Context, target string) (*grpc.ClientConn, error) {
			return grpc.NewClient(target, transport.DefaultDialOptions(peerCreds)...)
		},
		Timeout: cfg.PeerRPCTimeout,
		Logger:  log,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := peerClient.Close(); err != nil {
			log.Warn("failed to close peer connections", slog.String("error", err.Error()))
		}
	}()
	peerClient.OnResult = func(op, result string, d time.Duration) {
		metrics.PeerRequests.WithLabelValues(op, result).Inc()
		metrics.PeerDuration.WithLabelValues(op).Observe(d.Seconds())
	}

	coordinator, err := l2.New(backgroundCtx, l2.Options{
		NodeID: cfg.Node.ID, Local: l2Tier, FrontTier: l1Cache, Ring: ringHolder,
		Peers: peerClient, Origin: fetcher, Logger: log,
		ReplicationFactor:  cfg.Cache.ReplicationFactor,
		ReplicationQueue:   cfg.Cache.ReplicationQueue,
		ReplicationWorkers: cfg.Cache.ReplicationWorkers,
		ReplicationTimeout: cfg.Cache.ReplicationTimeout,
		MaxObjectBytes:     cfg.Cache.MaxObjectBytes,
		Metrics: l2.Metrics{
			CacheRequest: func(tier cache.Tier, outcome cache.Outcome) {
				metrics.CacheRequests.WithLabelValues(string(tier), string(outcome)).Inc()
			},
			ReplicationQueued:  func(d int) { metrics.ReplicationQueued.Set(float64(d)) },
			ReplicationDropped: func(r string) { metrics.ReplicationDropped.WithLabelValues(r).Inc() },
			CoalescedWaiters:   func(n int64) { metrics.CoalescedWaiters.Set(float64(n)) },
			CoalescedFill:      func() { metrics.CoalescedFills.Inc() },
			FillDuration:       func(d time.Duration) { metrics.CacheFillDuration.Observe(d.Seconds()) },
		},
	})
	if err != nil {
		return err
	}
	// A safety net for the error paths below; the ordered shutdown closes the
	// coordinator explicitly after the drain.
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Close(cctx); err != nil {
			log.Warn("cache coordinator shutdown failed", slog.String("error", err.Error()))
		}
	}()
	registry.coordinator = coordinator

	// --- peer gRPC server ---------------------------------------------------
	peerServerCreds, err := security.GRPCServerCredentials(cfg.Security)
	if err != nil {
		return err
	}
	peerSrv := grpc.NewServer(transport.DefaultServerOptions(peerServerCreds)...)
	peerService := peer.NewServer(coordinator, log)
	peerService.OnRequest = func(op, result string, d time.Duration) {
		metrics.PeerRequests.WithLabelValues("inbound_"+op, result).Inc()
		metrics.PeerDuration.WithLabelValues("inbound_" + op).Observe(d.Seconds())
	}
	edgemeshv1.RegisterPeerCacheServer(peerSrv, peerService)

	peerLn, err := net.Listen("tcp", cfg.Node.PeerAddress)
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "bind peer listener %q", cfg.Node.PeerAddress)
	}
	go func() {
		if err := peerSrv.Serve(peerLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("peer listener failed", slog.String("error", err.Error()))
		}
	}()
	log.Info("peer cache listening", slog.String("address", peerLn.Addr().String()))

	// --- public proxy -------------------------------------------------------
	handler, err := proxy.NewHandler(proxy.Options{
		NodeID: cfg.Node.ID, Routes: registry.Routes(), Pools: registry,
		Cache:  &tieredCache{l1: l1Cache, l2: coordinator, metrics: metrics},
		Origin: originClient, Limiters: registry, Trusted: trusted,
		Logger: log, Tracer: tracing.Tracer(),
		MaxObjectBytes: cfg.Cache.MaxObjectBytes,
		MaxConcurrent:  cfg.Proxy.MaxConcurrentRequests,
		RequestTimeout: cfg.Proxy.RequestTimeout,
		PropagateTrace: cfg.Proxy.PropagateTraceToOrigin,
		Metrics: proxy.Metrics{
			Request: func(route, method, class string, d time.Duration) {
				metrics.HTTPRequests.WithLabelValues(route, method, class).Inc()
				metrics.HTTPDuration.WithLabelValues(route).Observe(d.Seconds())
			},
			InflightDelta: func(route string, delta float64) {
				metrics.HTTPInflight.WithLabelValues(route).Add(delta)
			},
			BodyBytes: func(route, dir string, n float64) {
				metrics.HTTPBodyBytes.WithLabelValues(route, dir).Observe(n)
			},
			CacheRequest: func(tier cache.Tier, outcome cache.Outcome) {
				metrics.CacheRequests.WithLabelValues(string(tier), string(outcome)).Inc()
			},
			RateLimited: func(route string) { metrics.RateLimitDenied.WithLabelValues(route).Inc() },
		},
	})
	if err != nil {
		return err
	}

	publicSrv := &http.Server{
		Addr:              cfg.Node.PublicAddress,
		Handler:           handler,
		ReadHeaderTimeout: cfg.Proxy.ReadHeaderTimeout,
		IdleTimeout:       cfg.Proxy.IdleTimeout,
		WriteTimeout:      cfg.Proxy.WriteTimeout,
		MaxHeaderBytes:    cfg.Proxy.MaxHeaderBytes,
		ErrorLog:          nil,
	}
	publicLn, err := net.Listen("tcp", cfg.Node.PublicAddress)
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "bind public listener %q", cfg.Node.PublicAddress)
	}
	go func() {
		var serveErr error
		if cfg.Proxy.TLS.Enabled {
			serveErr = publicSrv.ServeTLS(publicLn, cfg.Proxy.TLS.CertFile, cfg.Proxy.TLS.KeyFile)
		} else {
			serveErr = publicSrv.Serve(publicLn)
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("public listener failed", slog.String("error", serveErr.Error()))
		}
	}()
	log.Info("public proxy listening", slog.String("address", publicLn.Addr().String()))

	// --- control-plane client -----------------------------------------------
	controlClient, err := newControlClient(controlClientOptions{
		Config: cfg, Registry: registry, Ring: ringHolder,
		Coordinator: coordinator, Logger: log, Metrics: metrics,
		Creds: peerCreds,
		// The leader shows these in `edgemeshctl nodes`, which is how an
		// operator sees load and cache occupancy across the fleet without
		// opening Prometheus.
		Inflight: handler.Inflight,
		Requests: handler.Requests,
	})
	if err != nil {
		return err
	}
	go controlClient.Run(ctx)

	// --- health checks ------------------------------------------------------
	healthChecker := origin.NewChecker(origin.CheckerOptions{
		Client: &http.Client{Transport: originTransport}, Logger: log, MaxConcurrent: 16,
	})
	healthChecker.OnTransition = func(poolID string, e *origin.Endpoint, _, to origin.Health) {
		v := 0.0
		if to == origin.HealthHealthy {
			v = 1
		}
		metrics.OriginHealth.WithLabelValues(poolID, e.ID).Set(v)
	}
	registry.healthChecker = healthChecker
	// Health checks run under the background context so origin selection stays
	// correct for requests that are still draining.
	healthChecker.Start(backgroundCtx, time.Second)
	defer healthChecker.Stop()

	// Readiness: an edge is ready once it can serve traffic. Control-plane
	// connectivity is deliberately *not* part of this: an edge with a valid
	// configuration snapshot can serve every request it could a moment ago, and
	// marking it unready would turn a control-plane outage into a data-plane
	// outage, which is exactly the coupling this architecture avoids.
	healthReg.Register("routing_snapshot", func() error {
		if registry.Routes().Load().Version() == 0 && registry.Routes().Load().Len() == 0 {
			return errs.New(errs.ClassUnavailable, "no configuration snapshot has been received yet")
		}
		return nil
	})

	telemetry, err := observability.NewTelemetryServer(observability.TelemetryOptions{
		Address: cfg.Telemetry.Address, Metrics: metrics, Health: healthReg,
		Logger: log, Pprof: cfg.Telemetry.EnablePprof,
		BuildInfo: map[string]string{
			"service": "edgemesh-edge", "version": version,
			"node_id": cfg.Node.ID, "region": cfg.Node.Region,
		},
	})
	if err != nil {
		return err
	}
	telemetry.Start()

	go runCacheMetrics(backgroundCtx, l1Cache, l2Tier, coordinator, ringHolder, metrics)
	go runBreakerMetrics(backgroundCtx, breakers, metrics)

	log.Info("edgemesh edge node ready", slog.String("node_id", cfg.Node.ID))

	<-ctx.Done()
	log.Info("shutdown signal received; draining")

	// --- graceful shutdown ---------------------------------------------------
	//
	// The order matters: stop admitting work, give the load balancer a moment
	// to notice, then drain in flight requests before tearing down the
	// dependencies those requests are still using.
	healthReg.SetLive(false)
	healthReg.Register("shutdown", func() error {
		return errs.New(errs.ClassUnavailable, "node is shutting down")
	})
	handler.Drain()

	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.Proxy.DrainTimeout)
	defer cancel()

	// Drain the public listener first: in-flight requests still need the cache,
	// the peer connections, and the origin transport, all of which are torn down
	// below.
	if err := publicSrv.Shutdown(drainCtx); err != nil {
		log.Warn("public listener drain timed out",
			slog.String("error", err.Error()),
			slog.Int64("inflight_requests", handler.Inflight()))
	} else {
		log.Info("public listener drained")
	}

	// Peers may still be mid-request against this node's L2 share, so its
	// listener stops only after local traffic has finished.
	peerSrv.GracefulStop()

	// Only now is it safe to cancel background work: nothing is waiting on it.
	stopBackground()
	if err := coordinator.Close(drainCtx); err != nil {
		log.Warn("cache coordinator shutdown failed", slog.String("error", err.Error()))
	}

	if err := telemetry.Shutdown(drainCtx); err != nil {
		log.Warn("telemetry shutdown failed", slog.String("error", err.Error()))
	}

	log.Info("edgemesh edge node stopped")
	return nil
}
