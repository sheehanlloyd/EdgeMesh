# EdgeMesh Threat Model

EdgeMesh is **production-inspired educational infrastructure**. It has not been
audited or run in production. This document states what it defends against, what
it does not, and which risks are knowingly accepted.

## Assets

| Asset | Why it matters |
|---|---|
| Cached response bodies | May contain data intended for one client only |
| Routing configuration | Controls where every request goes |
| Admin credentials | Grant the ability to rewrite all routing |
| mTLS private keys | Grant the ability to impersonate a cluster node |
| Raft log and snapshots | The durable record of configuration |
| Origin reachability | The proxy can reach networks its clients cannot |

The last one is easy to overlook and is the most dangerous: a proxy is, by
construction, a machine that makes requests on someone else's behalf from inside
a trusted network.

## Trust boundaries

```text
   UNTRUSTED                    │  SEMI-TRUSTED      │  TRUSTED
                                │                    │
   internet clients ──HTTP────► │   edge nodes ──────┼──► origins
                                │        ▲           │
                                │        │ mTLS      │
   operators ───HTTPS+token───► │   control plane    │
                                │        ▲           │
                                │        │ mTLS      │
                                │   peer edges       │
```

1. **Client → edge.** Fully untrusted. Everything from a client is hostile until
   validated.
2. **Operator → admin API.** Authenticated by bearer token; an authenticated
   operator is fully trusted.
3. **Node ↔ node.** Mutually authenticated by mTLS. A node holding a valid
   certificate is trusted to replicate configuration and serve cached objects.
4. **Edge → origin.** The origin is trusted to return correct content; EdgeMesh
   is responsible for not *reaching* origins it should not.

## Threats and mitigations

### T1: Cache poisoning through key manipulation

*A client crafts a request that stores content under a key another client will
read.*

- The route ID, not the raw `Host` header, is the first key component: untrusted
  host bytes never reach the key.
- Components are joined with `0x1f`, which cannot appear in any of them, so the
  encoding is injective and no boundary-shifting collision exists.
- The path is keyed verbatim, with no `//` collapsing or `..` resolution, so one
  stored object cannot be addressed by several paths.
- `Vary: *` responses are never admitted.
- Fuzz tested: [`FuzzBuildIsInjective`](../internal/cache/key/key_test.go).

### T2: Serving one user's response to another

*An authenticated or personalized response is cached and served to a different
client.*

- Requests carrying `Authorization` are not cached.
- Requests carrying any cookie are not cached unless the route explicitly
  allowlists the cookie names.
- Responses carrying `Set-Cookie` are not stored unless explicitly permitted.
- `Cache-Control: private` and `no-store` are honoured.

Every ambiguous case resolves to "do not cache".

### T3: Server-side request forgery

*A misconfigured or malicious route points EdgeMesh at an internal address.*

- Only `http` and `https` schemes; no `file://`, Unix sockets, or arbitrary
  schemes.
- Cloud instance-metadata ranges (`169.254.169.254`, `169.254.170.2`,
  `fd00:ec2::254`) are **always denied**, even when private networks are
  permitted. This is the credential-theft path that matters most.
- Link-local, multicast, and unspecified addresses are always denied.
- Loopback and private ranges are denied by default and must be opened
  explicitly; production mode additionally demands the exact CIDRs.
- **The check runs twice**: at configuration time and again in the transport's
  dial hook. The second check is what closes the DNS-rebinding window: a
  hostname that resolved to a public address during validation may resolve to
  loopback by the time a connection is made.

### T4: Request smuggling and response splitting

*A client uses header manipulation to desynchronize the proxy from the origin.*

- Hop-by-hop headers are stripped, **including headers nominated by
  `Connection`**. A client controls that list, and forwarding it is a smuggling
  primitive.
- Go's `net/http` rejects header values containing CR or LF, which prevents
  response splitting through the standard API.
- Header size is bounded by `max_header_bytes`.

### T5: Forged client identity

*A client sets `X-Forwarded-For` to evade rate limiting or poison logs.*

- Forwarding headers are **replaced**, not appended to, unless the peer is in
  `trusted_proxy_cidrs`, which defaults to empty, trusting nobody.
- From a trusted proxy the chain is extended and the **last** entry is used as
  the client identity; earlier entries came from further upstream and are
  forgeable.
- Request IDs supplied by clients are length-bounded and stripped of anything
  outside `[A-Za-z0-9_-]`, so a client cannot forge log lines with newlines.

### T6: Unauthorized configuration change

*Someone rewrites routing to redirect traffic.*

- Bearer-token authentication on the admin API, compared in constant time. A
  byte-by-byte comparison that returns early leaks the token's prefix through
  response timing.
- Authentication runs **before** routing and body parsing, so an unauthenticated
  caller cannot make the process do work.
- Tokens come from an environment variable or a file, never from the
  configuration document, so configuration stays safe to commit.
- Tokens shorter than 16 characters are refused at startup.
- Production mode refuses to start with authentication disabled.

### T7: Unauthorized cluster membership

*An attacker connects to the Raft or peer-cache port.*

- All three internal protocols use **mutual** TLS with
  `RequireAndVerifyClientCert`. Server-only TLS would let anyone who can reach
  the port speak Raft.
- TLS 1.3 minimum.
- Production mode refuses to start without mTLS, and refuses `skip_verify`.

### T8: Resource exhaustion

*An attacker drives the proxy out of memory or CPU.*

Every unbounded path has an explicit limit. See the backpressure table in
[architecture.md](architecture.md#backpressure). Specifically: bounded in-flight
requests, byte-budgeted caches, per-object size caps, a bounded and droppable
replication queue, LRU-capped rate-limiter keys with idle expiry, bounded
retries, and chunked peer transfers.

### T9: Information disclosure through diagnostics

*Internal topology leaks to clients.*

- `X-EdgeMesh-*` headers are emitted only when a route enables them.
- Error bodies carry a classified code and a request ID, never a stack trace or
  internal address.
- pprof is opt-in and **refused in production mode**: the telemetry listener is
  unauthenticated, and pprof exposes heap contents and allows CPU-consuming
  profiles to be triggered by anyone who can reach the port.
- Metric labels never carry raw URLs, cache keys, request IDs, or
  client-supplied hostnames, which is both a cardinality and a disclosure
  concern.
- Authorization headers, cookies, and request bodies are never logged.

## Accepted residual risks

Stated plainly rather than buried:

1. **A compromised control node can rewrite all routing.** Raft assumes
   crash-stop, not Byzantine, peers. Mitigation is operational: protect the
   nodes and their certificates.
2. **Rate limits are node-local.** With N edges, a configured rate of R is an
   effective ceiling of up to N×R cluster-wide. Distributed exact limiting is a
   documented non-goal for V1 because it would put a coordination round trip on
   the hot path.
3. **No TLS certificate rotation.** Certificates are loaded at startup; rotation
   requires a restart.
4. **No admin API rate limiting.** An authenticated operator can propose writes
   as fast as Raft accepts them.
5. **No audit log.** Configuration changes are visible in the Raft log and in
   application logs, but there is no separate tamper-evident audit trail.
6. **Cache TTLs depend on clock accuracy.** Badly skewed clocks across edges
   produce inconsistent expiry.
7. **The demo origin is not hardened.** It exists for testing and should never
   be exposed.
8. **Development mode is genuinely insecure.** Plaintext internal RPC and an
   unauthenticated admin API. It is called "development mode" for that reason,
   and production mode refuses every one of those combinations.

## Secret hygiene

- No secret is committed. `.gitignore` excludes `certs/`, `*.tfvars`, `.env`,
  and Terraform state.
- `.env.example` carries names only, never values.
- Development certificates are generated at runtime by
  `scripts/generate-dev-certs.sh` into a git-ignored directory.
- Terraform variables take secrets from the environment or a secret manager.
- The Helm chart reads the admin token from a Secret and never templates it into
  a ConfigMap.

## Reporting

See [SECURITY.md](../SECURITY.md).
