# Documentation

The design documents describe the requested direct-call solution. They cover
Core NATS-style messaging only.

- [Core broker from scratch](core-broker-from-scratch.md) defines the minimum
  behavior, public Go API, internal components, process boundary, routing
  protocol, failure behavior, and implementation stages.
- [Existing alternatives](alternatives.md) compares maintained in-process and
  local multi-process options.
- [nats-server direct API proposal](nats-server-direct-api-proposal.md)
  records related upstream work, current internal extension points, the likely
  review concerns, and copy-paste issue and pull request plans.
- [Current demos](tutorial.md) documents the code currently in this
  repository. The demos connect the official `nats.go` client to an embedded
  server through `Server.InProcessConn()`, so no client TCP connection exists.
  This is not the direct-call design.
