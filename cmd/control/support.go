package main

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"time"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/control/configstream"
	"github.com/sheehanlloyd/edgemesh/internal/control/membership"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
	raftnode "github.com/sheehanlloyd/edgemesh/internal/raft/node"
	"github.com/sheehanlloyd/edgemesh/internal/raft/statemachine"
)

// controlObserver reacts to Raft state changes.
//
// It is the seam between consensus and everything that must respond to a
// leadership change: liveness tracking moves to the new leader, config streams
// are re-pointed, and metrics follow the role.
type controlObserver struct {
	log     *slog.Logger
	metrics *observability.Metrics
	members *membership.Tracker
	stream  func() *configstream.Server
	nodeID  string
}

func (o *controlObserver) OnRoleChange(from, to raftnode.Role, term uint64) {
	// Exactly one role gauge is 1 at a time, so a dashboard can graph role
	// without the series overlapping.
	for _, r := range []raftnode.Role{raftnode.RoleFollower, raftnode.RoleCandidate, raftnode.RoleLeader} {
		v := 0.0
		if r == to {
			v = 1
		}
		o.metrics.RaftRole.WithLabelValues(string(r)).Set(v)
	}
	o.metrics.RaftTerm.Set(float64(term))
	if to == raftnode.RoleCandidate {
		o.metrics.RaftElections.Inc()
	}
	if from == raftnode.RoleLeader || to == raftnode.RoleLeader {
		o.metrics.RaftLeadershipChanges.Inc()
	}

	isLeader := to == raftnode.RoleLeader
	o.members.SetLeader(isLeader)

	if s := o.stream(); s != nil {
		if isLeader {
			// A new leader has no liveness history, so edges must reconnect and
			// re-register. Pushing a snapshot immediately gets them current
			// configuration without waiting for their reconnect backoff.
			s.BroadcastSnapshot()
		} else if from == raftnode.RoleLeader {
			// Dropping the streams sends edges to the new leader immediately
			// rather than leaving them attached to a node that can no longer
			// answer authoritatively.
			s.DisconnectAll()
		}
	}

	o.log.Info("raft role changed",
		slog.String(observability.FieldNodeID, o.nodeID),
		slog.String("from", string(from)),
		slog.String("to", string(to)),
		slog.Uint64(observability.FieldRaftTerm, term))
}

func (o *controlObserver) OnCommit(index uint64) {
	o.metrics.RaftCommitIndex.Set(float64(index))
}

func (o *controlObserver) OnApply(index uint64, cmd *statemachine.Command, res statemachine.Result, d time.Duration, err error) {
	o.metrics.RaftLastApplied.Set(float64(index))
	// Observed for both outcomes: a rejected command still costs apply time,
	// and excluding failures would quietly bias the histogram.
	o.metrics.RaftApplyDuration.Observe(d.Seconds())
	if err != nil {
		o.log.Warn("state machine rejected a committed command",
			slog.Uint64(observability.FieldRaftIndex, index),
			slog.String("command", cmd.Type.String()),
			slog.String(observability.FieldErrorClass, string(errs.ClassOf(err))),
			slog.String("error", err.Error()))
		return
	}
	o.metrics.ConfigVersion.Set(float64(res.ConfigVersion))
}

func (o *controlObserver) OnLeaderChange(leaderID string, term uint64) {
	o.log.Info("raft leader changed",
		slog.String("leader_id", leaderID),
		slog.Uint64(observability.FieldRaftTerm, term))
}

// leaderInfoProvider tells the config stream who leads.
type leaderInfoProvider struct {
	node           *raftnode.Node
	nodeID         string
	edgeAddressFor func(nodeID string) string
}

func (p *leaderInfoProvider) LeaderInfo() configstream.LeaderInfo {
	leader := p.node.LeaderID()
	return configstream.LeaderInfo{
		LeaderID:      leader,
		EdgeAddress:   p.edgeAddressFor(leader),
		IsLocalLeader: p.node.IsLeader(),
	}
}

// forwarder relays a follower's admin write to the leader.
type forwarder struct {
	node *raftnode.Node
	sm   *statemachine.StateMachine
}

func (f *forwarder) ForwardCommand(ctx context.Context, encoded []byte) (uint64, uint64, []byte, error) {
	cmd, err := statemachine.Decode(encoded)
	if err != nil {
		return 0, 0, nil, err
	}
	if !f.node.IsLeader() {
		return 0, 0, nil, errs.New(errs.ClassNotLeader,
			"this node is not the leader; leader is %q", f.node.LeaderID())
	}
	res, err := f.node.Propose(ctx, cmd)
	if err != nil {
		return 0, 0, nil, err
	}
	return res.ConfigVersion, f.node.LastApplied(), nil, nil
}

// broadcastWrite pushes a committed configuration change to connected edges.
//
// Broadcasting the specific change rather than a whole snapshot keeps the
// common case small; a client that falls behind gets a snapshot instead, which
// the stream server handles.
func broadcastWrite(s *configstream.Server, res statemachine.Result, cmd *statemachine.Command) {
	if s == nil {
		return
	}
	switch cmd.Type {
	case statemachine.CommandCreateRoute, statemachine.CommandUpdateRoute:
		s.BroadcastRoute(res.Route, res.ConfigVersion)
	case statemachine.CommandDeleteRoute:
		s.BroadcastRouteDeleted(cmd.ID, res.ConfigVersion)
	case statemachine.CommandCreateOriginPool, statemachine.CommandUpdateOriginPool:
		s.BroadcastOriginPool(res.OriginPool, res.ConfigVersion)
	case statemachine.CommandDeleteOriginPool:
		s.BroadcastOriginPoolDeleted(cmd.ID, res.ConfigVersion)
	case statemachine.CommandSetGlobalSettings:
		s.BroadcastSettings(res.Settings, res.ConfigVersion)
	case statemachine.CommandPurgeCache:
		s.BroadcastPurge(res.Purge)
	}
}

// runMembershipSweep re-evaluates edge liveness on a timer.
//
// It takes no config stream and no logger: the tracker already logs every state
// transition and already broadcasts through its OnChange hook, so doing either
// here would duplicate both.
func runMembershipSweep(ctx context.Context, cfg *config.ControlConfig,
	members *membership.Tracker, node *raftnode.Node, metrics *observability.Metrics) {

	t := time.NewTicker(cfg.Membership.SweepInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			states := []edgemeshv1.NodeState{
				edgemeshv1.NodeState_NODE_STATE_HEALTHY,
				edgemeshv1.NodeState_NODE_STATE_SUSPECT,
				edgemeshv1.NodeState_NODE_STATE_DEAD,
			}
			if !node.IsLeader() {
				// Only the leader tracks liveness. A node that has just lost
				// leadership must zero these gauges rather than leave them at
				// their last leading value, which would report a fleet this
				// node no longer observes for as long as it stays a follower.
				for _, st := range states {
					metrics.EdgeNodes.WithLabelValues(st.String()).Set(0)
				}
				continue
			}
			members.Sweep()
			counts := members.Counts()
			for _, st := range states {
				metrics.EdgeNodes.WithLabelValues(st.String()).Set(float64(counts[st]))
			}
		}
	}
}

// runMetricsRefresh publishes gauges that are cheaper to sample than to observe
// on every state change.
func runMetricsRefresh(ctx context.Context, node *raftnode.Node,
	sm *statemachine.StateMachine, stream *configstream.Server,
	metrics *observability.Metrics) {

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st := node.Status()
			metrics.RaftTerm.Set(float64(st.Term))
			metrics.RaftCommitIndex.Set(float64(st.CommitIndex))
			metrics.RaftLastApplied.Set(float64(st.LastApplied))
			metrics.RaftLogEntries.Set(float64(st.LogEntries))
			metrics.RaftSnapshotIndex.Set(float64(st.SnapshotIndex))
			metrics.ConfigVersion.Set(float64(sm.ConfigVersion()))
			if stream != nil {
				metrics.ConfigStreamClients.Set(float64(stream.ClientCount()))
			}
			for peer, lag := range node.ReplicationLag() {
				metrics.RaftReplicationLag.WithLabelValues(peer).Set(float64(lag))
			}
		}
	}
}

// adminAddressFor derives a peer's admin address from its Raft address.
//
// The reference deployment offsets the admin port from the Raft port by a fixed
// amount, which keeps configuration to one address per peer. A deployment that
// does not follow the convention can set advertise addresses explicitly.
func adminAddressFor(raftAddress string, cfg *config.ControlConfig) string {
	return offsetPort(raftAddress, cfg.Node.RaftAddress, cfg.Node.AdminAddress)
}

// edgeAddressFor derives a peer's edge-facing address the same way.
func edgeAddressFor(nodeID string, cfg *config.ControlConfig) string {
	if nodeID == cfg.Node.ID {
		return cfg.Node.AdvertiseEdgeAddress
	}
	for _, p := range cfg.Raft.Peers {
		if p.ID == nodeID {
			return offsetPort(p.Address, cfg.Node.RaftAddress, cfg.Node.EdgeAddress)
		}
	}
	return ""
}

// offsetPort maps peerAddress's port by the difference between localFrom's and
// localTo's ports, keeping peerAddress's host.
func offsetPort(peerAddress, localFrom, localTo string) string {
	peerHost, peerPortStr, err := net.SplitHostPort(peerAddress)
	if err != nil {
		return ""
	}
	_, fromPortStr, err := net.SplitHostPort(localFrom)
	if err != nil {
		return ""
	}
	_, toPortStr, err := net.SplitHostPort(localTo)
	if err != nil {
		return ""
	}
	peerPort, err1 := strconv.Atoi(peerPortStr)
	fromPort, err2 := strconv.Atoi(fromPortStr)
	toPort, err3 := strconv.Atoi(toPortStr)
	if err1 != nil || err2 != nil || err3 != nil {
		return ""
	}
	target := peerPort + (toPort - fromPort)
	if target < 1 || target > 65535 {
		return ""
	}
	return net.JoinHostPort(peerHost, strconv.Itoa(target))
}
