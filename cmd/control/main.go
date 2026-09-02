// Command control runs an EdgeMesh control-plane node.
//
// A control node owns the strongly consistent half of the system: it
// participates in Raft, applies configuration commands to the replicated state
// machine, serves the admin API, and streams configuration to edge nodes. It is
// deliberately not on the request hot path. See docs/architecture.md.
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
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/control/api"
	"github.com/sheehanlloyd/edgemesh/internal/control/configstream"
	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/health"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
	raftnode "github.com/sheehanlloyd/edgemesh/internal/raft/node"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
	"github.com/sheehanlloyd/edgemesh/internal/raft/storage"
	"github.com/sheehanlloyd/edgemesh/internal/raft/transport"
	"github.com/sheehanlloyd/edgemesh/internal/security"
)

// version is stamped at build time with -ldflags.
var version = "dev"

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet at this point, so a plain write to
		// stderr is the only reliable way to report a startup failure.
		fmt.Fprintf(os.Stderr, "edgemesh-control: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to the control-plane configuration file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("edgemesh-control", version)
		return nil
	}
	if *configPath == "" {
		return errs.New(errs.ClassValidation, "--config is required")
	}

	cfg, err := config.LoadControl(*configPath)
	if err != nil {
		return err
	}

	log := observability.NewLogger(observability.LoggerOptions{
		Level: cfg.Telemetry.LogLevel, Format: cfg.Telemetry.LogFormat,
		Service: "edgemesh-control", NodeID: cfg.Node.ID,
	})
	log.Info("starting edgemesh control node",
		slog.String("version", version),
		slog.String("mode", string(cfg.Mode)),
		slog.String("raft_address", cfg.Node.RaftAddress),
		slog.String("admin_address", cfg.Node.AdminAddress),
		slog.String("edge_address", cfg.Node.EdgeAddress))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetrics()
	healthReg := health.NewRegistry()

	tracing, err := observability.InitTracing(ctx, observability.TracingOptions{
		ServiceName: "edgemesh-control", ServiceVersion: version,
		Namespace: cfg.Telemetry.ServiceNamespace, NodeID: cfg.Node.ID,
		OTLPEndpoint: cfg.Telemetry.OTLPEndpoint, Insecure: cfg.Telemetry.OTLPInsecure,
		SampleRatio: cfg.Telemetry.TraceSampleRatio,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracing.Shutdown(shutdownCtx); err != nil {
			log.Warn("telemetry shutdown failed", slog.String("error", err.Error()))
		}
	}()

	// --- durable state and consensus -------------------------------------
	store, err := storage.Open(storage.Options{Dir: cfg.Raft.DataDir})
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("failed to close the raft store", slog.String("error", err.Error()))
		}
	}()
	log.Info("raft store opened", slog.String("path", store.Path()))

	sm := statemachine.New()

	clientCreds, err := security.GRPCClientCredentials(cfg.Security)
	if err != nil {
		return err
	}
	peerAddrs := make(map[string]string, len(cfg.Raft.Peers))
	peerIDs := make([]string, 0, len(cfg.Raft.Peers))
	for _, p := range cfg.Raft.Peers {
		peerAddrs[p.ID] = p.Address
		peerIDs = append(peerIDs, p.ID)
	}

	rpcTransport, err := transport.New(transport.Options{
		Peers: peerAddrs,
		Dialer: func(_ context.Context, target string) (*grpc.ClientConn, error) {
			return grpc.NewClient(target, transport.DefaultDialOptions(clientCreds)...)
		},
		Logger:             log,
		SnapshotChunkBytes: cfg.Raft.SnapshotChunkBytes,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := rpcTransport.Close(); err != nil {
			log.Warn("failed to close the raft transport", slog.String("error", err.Error()))
		}
	}()

	// The membership tracker and config stream reference each other, so the
	// tracker's change callback is wired after both exist.
	var streamServer *configstream.Server

	members := membership.New(membership.Options{
		HeartbeatInterval:  cfg.Membership.HeartbeatInterval,
		SuspectAfterMissed: cfg.Membership.SuspectAfterMissed,
		DeadAfterMissed:    cfg.Membership.DeadAfterMissed,
		Logger:             log,
		OnChange: func(m *edgemeshv1.Membership) {
			if streamServer != nil {
				streamServer.BroadcastMembership(m)
			}
		},
	})

	obs := &controlObserver{
		log: log, metrics: metrics, members: members,
		stream: func() *configstream.Server { return streamServer },
		nodeID: cfg.Node.ID,
	}

	node, err := raftnode.New(raftnode.Config{
		ID:                  cfg.Node.ID,
		Peers:               peerIDs,
		ElectionTimeoutMin:  cfg.Raft.ElectionTimeoutMin,
		ElectionTimeoutMax:  cfg.Raft.ElectionTimeoutMax,
		HeartbeatInterval:   cfg.Raft.HeartbeatInterval,
		SnapshotEntries:     cfg.Raft.SnapshotEntries,
		SnapshotBytes:       cfg.Raft.SnapshotBytes,
		MaxEntriesPerAppend: cfg.Raft.MaxEntriesPerAppend,
		RPCTimeout:          cfg.Raft.RPCTimeout,
		PreVote:             cfg.Raft.PreVote,
		Store:               store,
		Transport:           rpcTransport,
		StateMachine:        sm,
		Logger:              log,
		Observer:            obs,
	})
	if err != nil {
		return err
	}

	leaderResolver := func(leaderID string) string {
		// The admin address is what a CLI client needs; the Raft address is
		// internal and would be useless to redirect a client to.
		for _, p := range cfg.Raft.Peers {
			if p.ID == leaderID {
				// Peer addresses are Raft ports; derive the admin port from the
				// local configuration's offset convention.
				return adminAddressFor(p.Address, cfg)
			}
		}
		return ""
	}

	// --- edge-facing config stream ---------------------------------------
	streamServer, err = configstream.NewServer(configstream.Options{
		StateMachine: sm,
		Membership:   members,
		Leader: &leaderInfoProvider{
			node: node, nodeID: cfg.Node.ID,
			edgeAddressFor: func(id string) string { return edgeAddressFor(id, cfg) },
		},
		Logger: log,
	})
	if err != nil {
		return err
	}

	// --- gRPC server (raft + edge control) --------------------------------
	serverCreds, err := security.GRPCServerCredentials(cfg.Security)
	if err != nil {
		return err
	}
	raftSrv := grpc.NewServer(transport.DefaultServerOptions(serverCreds)...)
	edgemeshv1.RegisterRaftTransportServer(raftSrv,
		transport.NewServer(node, &forwarder{node: node, sm: sm}, log, 256<<20))

	raftLn, err := net.Listen("tcp", cfg.Node.RaftAddress)
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "bind raft listener %q", cfg.Node.RaftAddress)
	}
	go func() {
		if err := raftSrv.Serve(raftLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("raft listener failed", slog.String("error", err.Error()))
		}
	}()
	log.Info("raft transport listening", slog.String("address", raftLn.Addr().String()))

	edgeSrv := grpc.NewServer(transport.DefaultServerOptions(serverCreds)...)
	edgemeshv1.RegisterEdgeControlServer(edgeSrv, streamServer)
	edgeLn, err := net.Listen("tcp", cfg.Node.EdgeAddress)
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "bind edge control listener %q", cfg.Node.EdgeAddress)
	}
	go func() {
		if err := edgeSrv.Serve(edgeLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("edge control listener failed", slog.String("error", err.Error()))
		}
	}()
	log.Info("edge control listening", slog.String("address", edgeLn.Addr().String()))

	// --- admin API --------------------------------------------------------
	auth, err := security.NewTokenAuthenticator(cfg.AdminAuth)
	if err != nil {
		return err
	}
	if !auth.Enabled() {
		log.Warn("admin API authentication is disabled; this is only appropriate in development mode")
	}

	adminAPI, err := api.New(api.Options{
		Node: node, StateMachine: sm, Membership: members, Auth: auth,
		Logger: log, NodeID: cfg.Node.ID, Version: version,
		LeaderAddress: leaderResolver,
		OnWrite: func(res statemachine.Result, cmd *statemachine.Command) {
			broadcastWrite(streamServer, res, cmd)
		},
		RecordRequest: func(endpoint, result string) {
			metrics.AdminRequests.WithLabelValues(endpoint, result).Inc()
		},
	})
	if err != nil {
		return err
	}

	adminSrv := &http.Server{
		Addr:              cfg.Node.AdminAddress,
		Handler:           adminAPI.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	adminLn, err := net.Listen("tcp", cfg.Node.AdminAddress)
	if err != nil {
		return errs.Wrap(errs.ClassUnavailable, err, "bind admin listener %q", cfg.Node.AdminAddress)
	}
	go func() {
		if err := adminSrv.Serve(adminLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin listener failed", slog.String("error", err.Error()))
		}
	}()
	log.Info("admin api listening", slog.String("address", adminLn.Addr().String()))

	// --- readiness --------------------------------------------------------
	//
	// A control node is ready once its storage, consensus loop, and transports
	// are running. Readiness deliberately does not require leadership: a
	// follower is a fully functional cluster member.
	healthReg.Register("raft", func() error {
		if node.Term() == 0 && node.LeaderID() == "" {
			return errs.New(errs.ClassUnavailable, "raft has not yet joined a term")
		}
		return nil
	})
	healthReg.Register("storage", func() error { return store.Sync() })

	telemetry, err := observability.NewTelemetryServer(observability.TelemetryOptions{
		Address: cfg.Telemetry.Address, Metrics: metrics, Health: healthReg,
		Logger: log, Pprof: cfg.Telemetry.EnablePprof,
		BuildInfo: map[string]string{
			"service": "edgemesh-control", "version": version, "node_id": cfg.Node.ID,
		},
	})
	if err != nil {
		return err
	}
	telemetry.Start()

	if err := node.Start(ctx); err != nil {
		return err
	}

	// --- background loops --------------------------------------------------
	go runMembershipSweep(ctx, cfg, members, node, metrics)
	go runMetricsRefresh(ctx, node, sm, streamServer, metrics)

	log.Info("edgemesh control node ready", slog.String("node_id", cfg.Node.ID))

	<-ctx.Done()
	log.Info("shutdown signal received; draining")

	// --- graceful shutdown -------------------------------------------------
	//
	// Readiness is dropped first so an orchestrator stops routing to this node
	// before its listeners begin refusing work.
	healthReg.SetLive(false)
	healthReg.Register("shutdown", func() error {
		return errs.New(errs.ClassUnavailable, "node is shutting down")
	})

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := adminSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("admin listener shutdown failed", slog.String("error", err.Error()))
	}
	streamServer.DisconnectAll()
	edgeSrv.GracefulStop()
	raftSrv.GracefulStop()
	node.Stop()
	if err := telemetry.Shutdown(shutdownCtx); err != nil {
		log.Warn("telemetry shutdown failed", slog.String("error", err.Error()))
	}

	log.Info("edgemesh control node stopped")
	return nil
}
