# Provenance

`agent-workspace` is an independent Kubernetes controller implementation. It
uses a pure Go control package, client-go typed APIs and informers, bbolt
durable state, and a single-controller process lock.

The `nc-` Kubernetes resource prefix is historical. It is retained for
compatibility with existing resources; changing it would require an explicit
migration and is not performed implicitly.
