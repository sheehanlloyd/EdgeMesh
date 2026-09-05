# ADR-008: No automatic `terraform apply`

**Status:** Accepted

## Context

The repository ships Terraform for an EKS cluster. Applying it creates billable
AWS resources, roughly $0.20/hour, about $140/month if left running.

## Decision

**Nothing in this repository ever runs `terraform apply`.** Not a Make target,
not a script, not a CI workflow. `fmt`, `init -backend=false`, `validate`, and
`plan` are the only automated Terraform operations.

## Consequences

**Good.** Nobody can spend money by running `make` or opening a pull request. CI
still catches syntax errors, type errors, and formatting drift, so the code is
verified without being executed. The cost is impossible to incur accidentally.

**Bad.** Deploying requires reading the README and running the command by hand.
There is no one-command cloud demo.

**Accepted.** That friction is the feature. A `make deploy` target that quietly
creates an EKS cluster is a bug in a portfolio repository, where the most likely
person to run it is someone exploring it for the first time.

Supporting guardrails: `enable_nat_gateway` defaults to `false` (a NAT gateway
is ~$32/month and the most common surprise on an AWS bill), `single_nat_gateway`
is true when NAT is enabled, every resource is tagged
`Warning=billable-demo-cluster-destroy-when-done`, and a `destroy_reminder`
output prints the teardown sequence, uninstalling the Helm release first, so
load balancers it created are not orphaned and left billing after the cluster is
gone.
