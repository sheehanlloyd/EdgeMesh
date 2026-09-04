# EdgeMesh developer task runner.
#
# No target here mutates Git state, installs system packages with elevated
# privileges, or applies cloud infrastructure. `terraform apply` is deliberately
# absent: provisioning is a human-invoked action, and a Makefile target that
# spends money by accident is a bug.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE       := github.com/sheehanlloyd/edgemesh
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS      := -s -w -X main.version=$(VERSION)
BIN_DIR      := bin
GO           ?= go
# Per-target fuzz window for `make fuzz-smoke`. Longer is better locally.
FUZZTIME     ?= 10s
GOFLAGS      ?=
DOCKER       ?= docker
COMPOSE_FILE := deploy/compose/docker-compose.yaml
KIND_CLUSTER := edgemesh

# Packages whose tests are fast and deterministic enough to run everywhere.
UNIT_PKGS := ./internal/... ./cmd/...

.PHONY: help
help: ## Show this help
	@echo "EdgeMesh make targets:"
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

.PHONY: bootstrap
bootstrap: ## Report missing prerequisites and how to install them
	@echo "Checking EdgeMesh prerequisites..."
	@missing=0; \
	check() { \
	  if command -v $$1 >/dev/null 2>&1; then \
	    printf "  \033[32mok\033[0m       %-16s %s\n" "$$1" "$$($$1 $$2 2>&1 | head -1)"; \
	  else \
	    printf "  \033[31mmissing\033[0m  %-16s %s\n" "$$1" "$$3"; \
	    missing=1; \
	  fi; \
	}; \
	check go version "install from https://go.dev/dl/ (1.25+ required)"; \
	check protoc --version "brew install protobuf | apt-get install protobuf-compiler"; \
	check docker --version "install Docker Desktop or the docker engine"; \
	check kind --version "go install sigs.k8s.io/kind@latest"; \
	check helm version "brew install helm | https://helm.sh/docs/intro/install/"; \
	check terraform version "brew install terraform | https://developer.hashicorp.com/terraform/install"; \
	check golangci-lint --version "brew install golangci-lint"; \
	check k6 version "brew install k6 | https://k6.io/docs/get-started/installation/"; \
	echo; \
	echo "Go plugins used by 'make proto' are installed into \$$(go env GOPATH)/bin by:"; \
	echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; \
	echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; \
	if [ $$missing -ne 0 ]; then \
	  echo; \
	  echo "Some prerequisites are missing. Nothing was installed: install them yourself"; \
	  echo "so you stay in control of what lands on your machine."; \
	fi

# ---------------------------------------------------------------------------
# Code generation
# ---------------------------------------------------------------------------

.PHONY: proto
proto: ## Generate protobuf and gRPC code
	@command -v protoc >/dev/null || { echo "protoc is required; run 'make bootstrap'"; exit 1; }
	@mkdir -p api/gen
	protoc --proto_path=api/proto \
		--go_out=api/gen --go_opt=module=$(MODULE)/api/gen \
		--go-grpc_out=api/gen --go-grpc_opt=module=$(MODULE)/api/gen \
		api/proto/edgemesh/v1/*.proto
	@echo "generated code is in api/gen (left unstaged for review)"

.PHONY: proto-check
proto-check: ## Fail if generated code is stale relative to the .proto files
	@tmp=$$(mktemp -d); \
	cp -R api/gen "$$tmp/before"; \
	$(MAKE) --no-print-directory proto >/dev/null; \
	if ! diff -r "$$tmp/before" api/gen >/dev/null 2>&1; then \
	  echo "generated protobuf code is out of date; run 'make proto'"; \
	  diff -rq "$$tmp/before" api/gen || true; \
	  rm -rf "$$tmp"; exit 1; \
	fi; \
	rm -rf "$$tmp"; \
	echo "generated protobuf code is up to date"

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -w $$(find . -name '*.go' -not -path './api/gen/*')

.PHONY: fmt-check
fmt-check: ## Fail if any Go source is unformatted
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './api/gen/*')); \
	if [ -n "$$out" ]; then echo "unformatted files:"; echo "$$out"; exit 1; fi; \
	echo "all Go source is gofmt-clean"

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null || { echo "golangci-lint is required; run 'make bootstrap'"; exit 1; }
	golangci-lint run ./...

.PHONY: vulncheck
vulncheck: ## Run govulncheck against the module
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: no-dead-assignments
no-dead-assignments: ## Fail on `_ = someVar`, the pattern that hides unfinished work
	@# No linter catches assigning a *variable* to the blank identifier: it is
	@# not a wasted assignment and not an unchecked error, so it passes every
	@# check while silencing the compiler. It is how a measurement that is never
	@# recorded, or a parameter that is never used, survives review. Discarding a
	@# call's result (`_ = f()`, `_ = c.Close()`) is legitimate and not matched:
	@# the pattern here is a bare identifier with no call and no selector.
	@hits=$$(grep -rnE '^[[:space:]]*_ = [a-z][A-Za-z0-9_]*$$' \
	  --include='*.go' internal cmd 2>/dev/null | grep -v '_test\.go' || true); \
	if [ -n "$$hits" ]; then \
	  echo "dead assignments found; use the value or delete it:"; \
	  echo "$$hits"; \
	  exit 1; \
	fi; \
	echo "no dead assignments"

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests
	$(GO) test $(GOFLAGS) $(UNIT_PKGS)

.PHONY: test-race
test-race: ## Run unit tests under the race detector
	$(GO) test -race -timeout 900s $(UNIT_PKGS)

.PHONY: test-integration
test-integration: ## Run the in-process multi-node integration suite
	$(GO) test -timeout 900s ./test/integration/...

.PHONY: test-e2e
test-e2e: build ## Run end-to-end tests against real processes
	EDGEMESH_BIN_DIR=$(PWD)/$(BIN_DIR) $(GO) test -timeout 900s -tags e2e ./test/e2e/...

.PHONY: test-all
test-all: test-race test-integration ## Run every automated suite

.PHONY: fuzz-smoke
fuzz-smoke: ## Run every fuzz target for a short window
	@set -e; \
	found=0; \
	for pkg in $$($(GO) list ./...); do \
	  for fn in $$($(GO) test -list 'Fuzz.*' $$pkg 2>/dev/null | grep '^Fuzz' || true); do \
	    found=$$((found+1)); \
	    echo "fuzzing $$pkg $$fn"; \
	    $(GO) test $$pkg -run "^$$fn$$" -fuzz "^$$fn$$" -fuzztime $(FUZZTIME); \
	  done; \
	done; \
	echo "ran $$found fuzz targets"; \
	if [ "$$found" -eq 0 ]; then echo "no fuzz targets were discovered"; exit 1; fi

.PHONY: benchmark
benchmark: ## Run Go microbenchmarks
	$(GO) test -run '^$$' -bench . -benchmem ./internal/...

.PHONY: cover
cover: ## Produce a coverage profile at coverage.out
	$(GO) test -coverprofile=coverage.out -covermode=atomic $(UNIT_PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

.PHONY: build
build: ## Build every binary into ./bin
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/ ./cmd/...
	@ls -1 $(BIN_DIR)

.PHONY: images
images: ## Build container images for every service
	$(DOCKER) build -f Dockerfile.control -t edgemesh/control:$(VERSION) --build-arg VERSION=$(VERSION) .
	$(DOCKER) build -f Dockerfile.edge    -t edgemesh/edge:$(VERSION)    --build-arg VERSION=$(VERSION) .
	$(DOCKER) build -f Dockerfile.origin  -t edgemesh/origin:$(VERSION)  --build-arg VERSION=$(VERSION) .
	@echo "built edgemesh/{control,edge,origin}:$(VERSION)"

.PHONY: clean
clean: ## Remove build output and local state
	rm -rf $(BIN_DIR) coverage.out .data

# ---------------------------------------------------------------------------
# Local environments
# ---------------------------------------------------------------------------

.PHONY: dev-certs
dev-certs: ## Generate a local development CA and per-node certificates
	./scripts/generate-dev-certs.sh

.PHONY: run-local
run-local: build ## Run the whole topology as local processes (no Docker)
	./scripts/run-local.sh start

.PHONY: stop-local
stop-local: ## Stop the local process topology
	./scripts/run-local.sh stop

.PHONY: compose-up
compose-up: ## Start the full local topology in Docker Compose
	$(DOCKER) compose -f $(COMPOSE_FILE) up -d --build
	@echo
	@echo "EdgeMesh is starting. Ports:"
	@echo "  edge-1 proxy      http://localhost:8081"
	@echo "  edge-2 proxy      http://localhost:8082"
	@echo "  edge-3 proxy      http://localhost:8083"
	@echo "  admin API         http://localhost:7101 (cp-1), 7102 (cp-2), 7103 (cp-3)"
	@echo "  Prometheus        http://localhost:9090"
	@echo "  Grafana           http://localhost:3000 (admin/admin)"
	@echo "  Jaeger traces     http://localhost:16686"

.PHONY: compose-down
compose-down: ## Stop the Docker Compose topology and remove volumes
	$(DOCKER) compose -f $(COMPOSE_FILE) down -v

.PHONY: compose-logs
compose-logs: ## Follow Compose logs
	$(DOCKER) compose -f $(COMPOSE_FILE) logs -f

.PHONY: kind-up
kind-up: ## Create a kind cluster and deploy EdgeMesh into it
	./deploy/kind/up.sh $(KIND_CLUSTER)

.PHONY: kind-down
kind-down: ## Delete the kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: demo
demo: ## Run the scripted end-to-end demo against a running topology
	./scripts/demo.sh

.PHONY: chaos-leader
chaos-leader: ## Kill the current Raft leader and watch failover
	./scripts/chaos/kill-leader.sh

.PHONY: chaos-edge
chaos-edge: ## Kill an edge node and watch the ring converge
	./scripts/chaos/kill-edge.sh

.PHONY: chaos-origin
chaos-origin: ## Make an origin unhealthy and watch exclusion
	./scripts/chaos/fail-origin.sh

.PHONY: loadtest
loadtest: ## Run the k6 load test against edge-1
	./scripts/loadtest/run.sh

.PHONY: dashboards
dashboards: ## Validate the provisioned Grafana dashboards
	@for f in deploy/compose/grafana/dashboards/*.json; do \
	  python3 -c "import json,sys; json.load(open('$$f'))" && echo "  ok $$f"; \
	done

# ---------------------------------------------------------------------------
# Deployment artifacts
# ---------------------------------------------------------------------------

.PHONY: helm-lint
helm-lint: ## Lint the Helm chart
	helm lint deploy/helm/edgemesh

.PHONY: helm-template
helm-template: ## Render the Helm chart to stdout
	helm template edgemesh deploy/helm/edgemesh

.PHONY: terraform-fmt
terraform-fmt: ## Check Terraform formatting
	terraform -chdir=deploy/terraform/aws fmt -check -recursive

.PHONY: terraform-validate
terraform-validate: ## Validate the Terraform configuration (never applies)
	terraform -chdir=deploy/terraform/aws init -backend=false -input=false
	terraform -chdir=deploy/terraform/aws validate

.PHONY: ci
ci: fmt-check vet no-dead-assignments test-race test-integration helm-lint ## Run what CI runs
	@echo "CI checks passed"
