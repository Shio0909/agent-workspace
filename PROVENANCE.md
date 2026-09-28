# Provenance

`agent-workspace` is an independent Kubernetes controller implementation.

The project was informed by general controller design patterns and by an
earlier local project, `myclaw`, which the author also wrote. The shared ideas
are architectural rather than source-level: durable desired state, staged
reclamation, idempotent lifecycle requests, and reconciliation.

The implementations are separate. `myclaw` uses go-zero, MySQL, Redis-based
locking, YAML templates, and Kubernetes dynamic clients. `agent-workspace` uses
a pure Go control package, client-go typed APIs and informers, bbolt durable
state, and a single-controller process lock.

The `nc-` Kubernetes resource prefix is historical. It is retained for
compatibility with existing resources; changing it would require an explicit
migration and is not performed implicitly.
