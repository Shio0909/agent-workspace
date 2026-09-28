# Security Policy

## Scope

`agent-workspace` is a single-controller reference implementation. It is not
hardened for multi-tenant or untrusted-code workloads.

## Known Boundaries

- Authentication is a single shared control token. There is no per-user
  authorization or tenant isolation.
- The controller is designed for one replica. It does not implement leader
  election or high availability.
- Agent containers run with a dedicated service account and automatic token
  mounting disabled, but this is not a sandbox guarantee.
- There is no NetworkPolicy, Pod Security Admission, or egress restriction
  built in.
- State is stored in a local bbolt database with a process lock. It is not a
  multi-controller database.
- TLS termination and rate limiting are expected to be provided by the
  surrounding deployment.

## Reporting

Please report vulnerabilities privately through GitHub Security Advisories
instead of a public issue. Include reproduction steps and the affected commit.
