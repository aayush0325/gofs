SHELL := /bin/sh
GO ?= go
PROTOC ?= protoc
BIN := bin
COMPOSE_FILE ?= deploy/demo/docker-compose.yml
OPERATOR_IMG ?= gofs-operator:local
KIND_CLUSTER ?= gofs-demo

.PHONY: all build proto certs test vet fmt tidy paper demo compose-up compose-down docker-build operator operator-web operator-test operator-manifests operator-docker operator-install operator-deploy operator-installer kind-demo kind-down kind-manifests clean

all: build

## Build all three binaries into ./bin.
build: $(BIN)/gfs-master $(BIN)/gfs-chunkserver $(BIN)/gfs-client

$(BIN)/gfs-master: FORCE
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $@ ./cmd/gfs-master

$(BIN)/gfs-chunkserver: FORCE
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $@ ./cmd/gfs-chunkserver

$(BIN)/gfs-client: FORCE
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $@ ./cmd/gfs-client

## Regenerate gRPC/protobuf code (generated files are committed).
proto:
	$(PROTOC) -I proto --go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/gfs/v1/*.proto

## Generate the local CA, server certificate, and client certificate.
certs:
	chmod +x scripts/gen-certs.sh
	./scripts/gen-certs.sh ./certs

## Run all unit and integration tests.
test:
	$(GO) test ./...

## Run static analysis.
vet:
	$(GO) vet ./...

## Format all Go sources.
fmt:
	gofmt -w cmd internal operator/cmd operator/api operator/internal
	@if command -v goimports >/dev/null 2>&1; then goimports -w cmd internal operator/cmd operator/api operator/internal; fi

## Resolve module dependencies.
tidy:
	$(GO) mod tidy

## Download the GFS SOSP 2003 paper if it is missing.
paper:
	@test -f docs/gfs-sosp2003.pdf || curl -L --fail --silent --show-error \
		"https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf" \
		-o docs/gfs-sosp2003.pdf

## Start the full three-chunkserver demo with Docker Compose.
demo: compose-up

compose-up:
	docker compose -f $(COMPOSE_FILE) --profile tools build
	docker compose -f $(COMPOSE_FILE) up --build

compose-down:
	docker compose -f $(COMPOSE_FILE) down

## Build all three Docker images.
docker-build:
	docker build -f Dockerfile.master -t gofs-master:local .
	docker build -f Dockerfile.chunkserver -t gofs-chunkserver:local .
	docker build -f Dockerfile.client -t gofs-client:local .

## Build the Operator SDK project.
operator:
	cd operator && make build

## Build the React web console.
operator-web:
	cd operator && make web-build

## Run operator unit/envtest tests.
operator-test:
	cd operator && make test

## Regenerate operator CRDs, RBAC, and deepcopy code.
operator-manifests:
	cd operator && make manifests generate

## Build the operator image.
operator-docker:
	cd operator && make docker-build IMG=$(OPERATOR_IMG)

## Install the GFSCluster CRD into the current kubeconfig.
operator-install:
	cd operator && make install

## Deploy the operator to the current kubeconfig (including CRDs).
operator-deploy:
	cd operator && make deploy IMG=$(OPERATOR_IMG)

## Create a single installer manifest at operator/dist/install.yaml.
operator-installer:
	cd operator && make build-installer IMG=$(OPERATOR_IMG)

## Create a Kind cluster, deploy the operator, and run the in-cluster demo.
kind-demo:
	KIND_CLUSTER=$(KIND_CLUSTER) ./deploy/kind/demo.sh

## Delete the Kind demo cluster.
kind-down:
	KIND_CLUSTER=$(KIND_CLUSTER) ./deploy/kind/cleanup.sh

## Refresh deploy/kind/operator.yaml from the operator kustomization.
kind-manifests:
	cd operator && make build-installer IMG=$(OPERATOR_IMG)
	cp operator/dist/install.yaml deploy/kind/operator.yaml

clean:
	rm -rf $(BIN)

FORCE:
