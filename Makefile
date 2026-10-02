.PHONY: test build vet images agent-images e2e e2e-continuity e2e-eino-agent e2e-eino-agent-faults e2e-isolation
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
# Needs eino-agent-workspace:local (docker build -f docker/Dockerfile.workspace in
# the eino_agent repository) and LLM_BASE_URL, LLM_MODEL, LLM_KEY_FILE.
e2e-eino-agent:
	docker build --target controller -t agent-workspace:local .
	printf 'FROM eino-agent-workspace:local\nENV AGENT_VERSION=v2\n' | docker build -t eino-agent-workspace:v2 -
	printf 'FROM eino-agent-workspace:v2\nENTRYPOINT ["false"]\n' | docker build -t eino-agent-workspace:bad -
	kind load docker-image agent-workspace:local eino-agent-workspace:local eino-agent-workspace:v2 eino-agent-workspace:bad --name $(KIND_CLUSTER)
	KIND_CLUSTER=$(KIND_CLUSTER) ./scripts/kind-eino-agent.sh

# Failure and load checks for the same image: PostgreSQL killed, pod killed,
# concurrent turns, knowledge base, a second workspace. Same inputs and needs
# eino-agent-workspace:local, and it builds the controller image.
e2e-eino-agent-faults:
	docker build --target controller -t agent-workspace:local .
	kind load docker-image agent-workspace:local eino-agent-workspace:local --name $(KIND_CLUSTER)
	KIND_CLUSTER=$(KIND_CLUSTER) ./scripts/kind-eino-agent-faults.sh

# Pod Security "restricted" enforced on the namespace, and workspace pods
# reachable only from the controller. Needs a CNI that enforces NetworkPolicy.
e2e-isolation:
	make images
	kind load docker-image agent-workspace:local agent-workspace-demo:local --name $(KIND_CLUSTER)
	KIND_CLUSTER=$(KIND_CLUSTER) ./scripts/kind-isolation.sh
