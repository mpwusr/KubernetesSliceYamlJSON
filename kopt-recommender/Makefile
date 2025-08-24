# -------- Config --------
MODULE        := github.com/yourorg/kopt-recommender
IMG           := ghcr.io/yourorg/kopt-recommender
TAG           := dev
IMAGE         := $(IMG):$(TAG)

GO            := go
GOFLAGS       :=
LD_FLAGS      := -s -w
PKG_ALL       := ./...

# Path to kubeconfig for out-of-cluster runs (override as needed)
KUBECONFIG   ?= $(HOME)/.kube/config

# golangci-lint path (Homebrew default on Apple Silicon)
GOLANGCI_LINT ?= /opt/homebrew/bin/golangci-lint

# Webhook TLS cert location expected by controller-runtime
CERT_DIR      := /tmp/k8s-webhook-server/serving-certs
CERT_CRT      := $(CERT_DIR)/tls.crt
CERT_KEY      := $(CERT_DIR)/tls.key

# controller-gen (for deepcopy/CRDs/RBAC)
GOBIN                 ?= $(shell go env GOPATH)/bin
CONTROLLER_GEN        ?= $(shell command -v controller-gen 2>/dev/null || echo $(GOBIN)/controller-gen)
CONTROLLER_GEN_VERSION ?= v0.14.0

# -------- Help --------
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nTargets:\n"} /^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# -------- Go basics --------
.PHONY: tidy
tidy: ## go mod tidy & download
	$(GO) mod tidy
	$(GO) mod download

.PHONY: fmt
fmt: ## Format source (go fmt)
	$(GO) fmt $(PKG_ALL)

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKG_ALL)

.PHONY: test
test: ## Run unit tests
	$(GO) test $(GOFLAGS) -count=1 -race $(PKG_ALL)

# -------- Lint --------
.PHONY: lint
lint: ## Run golangci-lint (needs golangci-lint installed)
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 || { echo "golangci-lint not found at '$(GOLANGCI_LINT)'. Set GOLANGCI_LINT or install via Homebrew."; exit 1; }
	$(GOLANGCI_LINT) run

# -------- Codegen (deepcopy/CRDs/RBAC) --------
.PHONY: controller-gen-install
controller-gen-install: ## Install controller-gen (once)
	@if [ ! -x "$(CONTROLLER_GEN)" ]; then \
		echo "Installing controller-gen $(CONTROLLER_GEN_VERSION) into $(GOBIN)"; \
		GO111MODULE=on $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION); \
	else \
		echo "controller-gen found at $(CONTROLLER_GEN)"; \
	fi

.PHONY: generate
generate: controller-gen-install ## Generate deepcopy (and later CRDs/RBAC)
	@[ -f hack/boilerplate.go.txt ] || { mkdir -p hack; echo "/*\n// SPDX-License-Identifier: Apache-2.0\n*/" > hack/boilerplate.go.txt; }
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt" paths="./..."

# -------- Build --------
BIN := bin/kopt-recommender

.PHONY: build
build: generate ## Build local binary (bin/kopt-recommender)
	@mkdir -p bin
	$(GO) build $(GOFLAGS) -ldflags '$(LD_FLAGS)' -o $(BIN) ./main.go

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin

# -------- Run locally --------
.PHONY: run
run: build ## Run controller locally (webhook disabled)
	@echo "Running with DISABLE_WEBHOOK=1 against KUBECONFIG=$(KUBECONFIG)"
	KUBECONFIG=$(KUBECONFIG) DISABLE_WEBHOOK=1 ./$(BIN) \
	  --metrics-bind-address=:8080 \
	  --health-probe-bind-address=:8081 \
	  --leader-elect=false \
	  --webhook-port=9443

# TLS for local webhook testing
$(CERT_CRT) $(CERT_KEY):
	@echo "Generating self-signed webhook certs in $(CERT_DIR)"
	@mkdir -p $(CERT_DIR)
	@openssl req -x509 -newkey rsa:2048 -nodes \
	  -keyout $(CERT_KEY) \
	  -out $(CERT_CRT) \
	  -days 365 \
	  -subj "/CN=localhost" >/dev/null 2>&1
	@echo "Created: $(CERT_CRT), $(CERT_KEY)"

.PHONY: run-webhook
run-webhook: build $(CERT_CRT) $(CERT_KEY) ## Run locally with webhook enabled (uses self-signed certs)
	@echo "Running webhook on :9443 with certs in $(CERT_DIR)"
	KUBECONFIG=$(KUBECONFIG) ./$(BIN) \
	  --metrics-bind-address=:8080 \
	  --health-probe-bind-address=:8081 \
	  --leader-elect=false \
	  --webhook-port=9443

# -------- Container build/push --------
.PHONY: docker-build
docker-build: ## Build container image
	docker build -t $(IMAGE) .

.PHONY: docker-push
docker-push: ## Push container image
	docker push $(IMAGE)

# For kind users: load the image directly into the cluster
KIND_CLUSTER ?= kopt
.PHONY: kind-load
kind-load: ## Load image into kind cluster (KIND_CLUSTER=$(KIND_CLUSTER))
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)

# -------- Manifests (placeholders) --------
.PHONY: manifests
manifests: ## Generate or apply manifests (placeholder - integrate kustomize/helm)
	@echo "TODO: add kustomize or helm generation/apply here."

# -------- CI convenience --------
.PHONY: check
check: tidy fmt vet lint test ## Run all local checks

