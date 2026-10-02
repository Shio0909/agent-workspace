.PHONY: test build vet images agent-images e2e e2e-continuity
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
agent-images:
	docker build --target controller -t agent-workspace:local .
	docker build --target agent --build-arg AGENT_VERSION=v1 -t agent-workspace-agent:local .
	docker build --target agent --build-arg AGENT_VERSION=v2 -t agent-workspace-agent:v2 .
	printf 'FROM agent-workspace-agent:v2\nENTRYPOINT ["false"]\n' | docker build -t agent-workspace-agent:bad -
	docker build --target fakellm -t agent-workspace-fakellm:local .
e2e-continuity:
	make agent-images
	kind load docker-image agent-workspace:local agent-workspace-agent:local agent-workspace-agent:v2 agent-workspace-agent:bad agent-workspace-fakellm:local --name $(KIND_CLUSTER)
	KIND_CLUSTER=$(KIND_CLUSTER) ./scripts/kind-continuity.sh
