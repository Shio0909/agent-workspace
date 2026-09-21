.PHONY: test build vet images e2e
KIND_CLUSTER ?= agent-workspace

test:
	go test -race ./...
vet:
	go vet ./...
build:
	mkdir -p bin
	go build -o bin/agent-workspace ./cmd/agent-workspace
	go build -o bin/demo-agent ./cmd/demo-agent
images:
	docker build --target controller -t agent-workspace:local .
	docker build --target demo -t agent-workspace-demo:local .
e2e:
	make images
	kind load docker-image agent-workspace:local agent-workspace-demo:local --name $(KIND_CLUSTER)
	./scripts/kind-smoke.sh
