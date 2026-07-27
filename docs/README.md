# Documentation

- [Tutorial](tutorial.md) documents the demos in this repository. The demos
  connect the official `nats.go` client to an embedded server through
  `Server.InProcessConn()`, so no client TCP connection exists.
- [Existing alternatives](alternatives.md) compares maintained in-process and
  local multi-process messaging options with the embedded-NATS approach used
  here.
- [Core broker from scratch](core-broker-from-scratch.md) is a design study of
  a direct-call, protocol-free Core NATS-style broker: minimum behavior,
  public Go API, internal components, process boundary, routing protocol,
  failure behavior, and implementation stages.
- [nats-server direct API proposal](nats-server-direct-api-proposal.md)
  records related upstream work, current internal extension points, the likely
  review concerns, and copy-paste issue and pull request plans for a
  protocol-free embedded API in `nats-server`.

The two design studies cover Core NATS-style messaging only.
