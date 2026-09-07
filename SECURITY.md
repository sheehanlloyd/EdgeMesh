# Security Policy

## What EdgeMesh is

EdgeMesh is **production-inspired educational infrastructure**. It has not been
security-audited and has not been operated in production. It is published as a
portfolio and learning project.

Do not deploy it in front of production traffic without your own review.

## Reporting a vulnerability

Open a GitHub security advisory, or email the address on the repository owner's
GitHub profile.

Please include what the issue is, how to reproduce it, and what an attacker
could achieve. A response should come within a week; because this is a personal
project, there is no guaranteed remediation timeline.

Please do not open a public issue for something exploitable until it is fixed.

## Security posture

What EdgeMesh does defend against, in detail:
[docs/threat-model.md](docs/threat-model.md).

Summary:

- **mTLS** on all three internal protocols (Raft, edge-control, peer-cache) with
  `RequireAndVerifyClientCert` and TLS 1.3 minimum
- **Constant-time** admin token comparison, authenticated before any parsing
- **SSRF guards** that always deny cloud instance-metadata ranges and re-check
  at dial time to close the DNS-rebinding window
- **Conservative cache eligibility**: authenticated requests, cookie-bearing
  requests, `Set-Cookie` responses, and `Vary: *` are never cached by default
- **Hop-by-hop header stripping**, including headers nominated by `Connection`
- **Forwarding headers replaced**, not appended, unless the peer is an explicitly
  trusted proxy
- **Bounded everything**: in-flight requests, cache bytes, object size, retries,
  rate-limiter keys, replication queue, peer transfers

## Development mode is insecure by design

`mode: development` runs internal RPCs in plaintext and leaves the admin API
unauthenticated. That is what it is for.

`mode: production` refuses to start without mTLS and admin authentication,
refuses `skip_verify`, refuses pprof, and requires explicit CIDRs before
permitting private-network origins. The Helm chart enforces the same rules at
install time.

## Known accepted risks

Enumerated in [docs/threat-model.md](docs/threat-model.md#accepted-residual-risks).
The most significant: a compromised control-plane node can rewrite all routing
(Raft assumes crash-stop, not Byzantine, peers), rate limits are node-local, and
there is no certificate rotation or audit log.

## Secret hygiene

No secret is committed. `.gitignore` excludes `certs/`, `*.tfvars`, `.env`, and
Terraform state. `.env.example` lists names only. Development certificates are
generated at runtime into a git-ignored directory. The Helm chart reads the
admin token from a Secret and never templates it into a ConfigMap.
