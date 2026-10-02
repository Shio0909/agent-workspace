# Security Policy

## Scope

`agent-workspace` is a single-controller reference implementation. It is not
hardened for multi-tenant or untrusted-code workloads.

## Known Boundaries

- Authentication is a shared control token that can do everything, plus
  optional scoped tokens (`-tokens`) limited to some workspace ids or
  profiles. Scoped tokens are authorization, not tenant isolation: there is no
  network isolation between workspaces, and tokens have no expiry (revoking one
  means editing the file and restarting). See
  [docs/control-plane.md](docs/control-plane.md), section 11.
- The controller is designed for one replica. It does not implement leader
  election or high availability.
- Agent containers run with a dedicated service account and automatic token
  mounting disabled, but this is not a sandbox guarantee.
- There is no NetworkPolicy, Pod Security Admission, or egress restriction
  built in.
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
