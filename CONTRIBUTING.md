# Contributing to EdgeMesh

## Getting set up

```bash
make bootstrap    # reports missing prerequisites; installs nothing
make build
make test
```

`make bootstrap` deliberately does not install anything. It tells you what is
missing and how to get it, so you stay in control of what lands on your machine.

## Before opening a pull request

```bash
make ci   # fmt-check, vet, test-race, test-integration, helm-lint
```

## Standards

**Correctness before cleverness.** Failure behaviour must be explicit and
tested. If a code path can fail, something should assert what happens when it
does.

**Comments explain why, not what.** `// increment i` is noise. `// The vote must
be durable before the response is sent, or a crash in that window lets this node
vote twice in one term` is the kind of comment this codebase wants. If a
decision looks arbitrary, the comment should say why it is not.

**Errors are classified, not string-matched.** Use `internal/errs` and wrap with
`%w` so classification survives.

**Everything is bounded.** Any queue, buffer, map keyed by client input, retry
loop, or goroutine fan-out needs an explicit limit and a defined behaviour at
that limit.

**No `time.Sleep` for synchronization in tests.** Inject a clock
(`internal/clock`) or poll a condition. A sleeping test is a slow test that is
also flaky.

**A flaky test is a bug report.** Do not add a retry or extend a timeout to
silence one. Two real bugs in this repository were found by flaky tests; both
would have been hidden by a longer timeout. Diagnose first.

**No fabricated numbers.** Never add a performance claim without the hardware,
command, and conditions that produced it. See
[docs/benchmarks.md](docs/benchmarks.md).

## Testing expectations

| Change | Expected tests |
|---|---|
| New policy logic | Table-driven unit tests including the rejection cases |
| Concurrency | A test that fails under `-race` without the fix |
| A parser or decoder | A fuzz target |
| Cross-component behaviour | An integration test |
| A bug fix | A regression test that fails without the fix |

## Making a change

1. **New subsystem?** Add metrics, logs, and spans alongside the code, not
   afterwards.
2. **Architectural decision?** Add an ADR in `docs/adr/`: the decision, the
   alternatives, and the consequences accepted.
3. **Protobuf change?** Run `make proto`. Never reuse a field number; reserve
   removed ones.
4. **New failure mode?** Add a row to `docs/failure-model.md` with a way to
   reproduce and a way to observe it.

## Repository layout

```text
cmd/         binaries: edge, control, edgemeshctl, origin-demo
api/proto/   protobuf definitions (api/gen is generated)
internal/    the implementation
test/        integration, e2e, and deployment tests
deploy/      compose, kind, helm, terraform
docs/        architecture, raft, cache, failure model, threat model, ADRs
scripts/     demo, chaos, load test, certificates
```

Package names reflect domain ownership. There is no `utils` package and there
should not be one.

## Source control

This repository's development convention is that automated tooling never mutates
Git state, so no `add`, `commit`, `push`, or branch manipulation. Changes are left
unstaged for a human to review and commit.
