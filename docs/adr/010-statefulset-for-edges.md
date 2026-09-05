# ADR-010: StatefulSet rather than Deployment for edge nodes

**Status:** Accepted

## Context

Edge nodes are stateless in the usual sense: they hold no durable data, and any
cached object can be refetched from the origin. The obvious Kubernetes primitive
is a Deployment.

But edges are not interchangeable. Each one occupies a position on the
consistent hash ring derived from its node ID, and peers reach it by dialing its
advertised address directly. A peer-cache request must reach *one specific
node*, not any node behind a service.

## Decision

Deploy edges as a **StatefulSet** with a headless service for per-pod DNS,
alongside the load-balanced service that carries public traffic.

## Alternatives considered

**Deployment with a headless service.** Deployment pods get random name
suffixes, so a rolling update replaces `edge-7f8b9c` with `edge-2a4d1e`. Every
replacement is a new ring identity, which remaps that node's share of the
keyspace, on every roll, for every pod. A three-pod rolling update would churn
the entire ring three times.

**Deployment with ownership by IP.** Would avoid the naming problem and
introduce a worse one: a pod's IP changes on every reschedule, so ring
membership would churn on events that have nothing to do with capacity.

## Consequences

**Good.** A pod that restarts returns with the same identity, the same ring
position, and the same peer address, so its share of the keyspace comes back to
it rather than being redistributed twice. Peers can address a specific node.
Scaling changes ring membership by exactly the pods added or removed.

**Bad.** StatefulSets are slower to roll than Deployments, and `podManagementPolicy: Parallel`
is required so edges start together rather than waiting for each predecessor to
become ready. Scaling down leaves the ordinal free rather than reusing an
arbitrary name.

**Accepted.** The HPA is configured with a long scale-down stabilization window
for the same underlying reason: adding or removing an edge remaps part of the
keyspace, so flapping replica counts would depress the hit ratio far more than
the saved capacity is worth.

The **control plane** is a StatefulSet for a different and stronger reason: its
Raft peer addresses are static configuration, so a member must come back with
the same DNS name or the cluster cannot reach it at all.
