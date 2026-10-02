# Security Policy

## Scope

`agent-workspace` is a single-controller reference implementation. It is not
hardened for multi-tenant or untrusted-code workloads.

## Known Boundaries

- Authentication is a shared control token that can do everything, plus
  optional scoped tokens (`-tokens`) limited to some workspace ids or
  profiles. Scoped tokens are authorization, not tenant isolation: workspaces
  share a cluster, a namespace and a node pool (network isolation between them
  depends on the NetworkPolicy below being enforced), and tokens have no expiry
  (revoking one means editing the file and restarting). See
  [docs/control-plane.md](docs/control-plane.md), section 11.
- The controller is designed for one replica. It does not implement leader
  election or high availability.
- Agent containers run with a dedicated service account and automatic token
  mounting disabled, but this is not a sandbox guarantee.
- `deploy/controller.yaml` includes a NetworkPolicy that lets workspace pods
  accept traffic only from the controller, so one workspace, or any other pod in
  the cluster, cannot call a workspace directly. It does nothing on a CNI that
  does not enforce NetworkPolicy, and says nothing about it. Egress is not
  restricted: agents need their model endpoint, and a compromised workspace can
  reach anything the node can.
- The controller, the fake LLM and every workspace pod set `runAsNonRoot`, the
  `RuntimeDefault` seccomp profile, no privilege escalation and no capabilities,
  which is what the "restricted" Pod Security profile asks for. Nothing enforces
  it for you: label the namespace `pod-security.kubernetes.io/enforce=restricted`
  (`scripts/kind-isolation.sh` does, and checks that it holds). `runAsNonRoot`
  makes the kubelet refuse a profile image whose `USER` is root or not numeric.
  This is a hardening of the pod, not a sandbox: the container shares the node's
  kernel. Earlier controllers built pods without these fields. The pod template
  is part of the spec hash, so after upgrading the controller each existing
  workspace is rolled once the next time it is reconciled while running (read
  from the code, not covered by an upgrade test).
- State is stored in a local bbolt database with a process lock. It is not a
  multi-controller database.
- Credentials written through the API are stored in Kubernetes Secrets, so
  their at-rest protection is whatever the cluster provides (base64 by default).
  The controller itself keeps only version numbers and key names. The shared
  control token can write any workspace's credentials; a scoped token only
  those of its own workspaces.
- TLS termination and rate limiting are expected to be provided by the
  surrounding deployment.

## Reporting

Please report vulnerabilities privately through GitHub Security Advisories
instead of a public issue. Include reproduction steps and the affected commit.
