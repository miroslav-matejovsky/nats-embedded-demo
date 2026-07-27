# Embedded NATS feasibility demo

This repository demonstrates message passing through an embedded NATS server
without using a NATS client library in application code.

It contains two local-only applications:

- `cmd/nats`: core NATS publish and subscribe.
- `cmd/jetstream`: JetStream stream creation, durable pull consumption,
  publishing, and explicit acknowledgement.

Each process embeds `github.com/nats-io/nats-server/v2/server`. Its application
code obtains a `net.Conn` from `Server.InProcessConn()` and writes the NATS text
protocol directly. Multiple processes exchange messages through NATS routes
bound to loopback addresses.

## Feasibility result

The approach is possible, with an important boundary:

- The server offers `Server.InProcessConn()` as the exported in-process entry
  point.
- Publishing and subscribing still use the NATS wire protocol.
- JetStream operations still use its subject-based JSON API.
- The lower-level server client, subscription, stream, and consumer
  implementations are unexported.

No project Go source imports `github.com/nats-io/nats.go`. That module can still
appear as an indirect module dependency of `nats-server`, but it is absent from
the application build dependency graph.

This is a feasibility demo, not a production client implementation. It omits
reconnects, authentication, TLS, concurrent dispatch, and other client-library
features.

## Quick start

Run a standalone core NATS round trip:

```powershell
go run ./cmd/nats --client-port=-1 --cluster-port=-1
```

Run a standalone JetStream round trip:

```powershell
go run ./cmd/jetstream --client-port=-1 --cluster-port=-1 `
  --store-dir=.test-results/quickstart-js
```

Use double-dash application flags with `go run` so the Go command does not
consume similarly named flags.

See [docs/tutorial.md](docs/tutorial.md) for separate-process cluster examples,
protocol details, and implementation notes.

## Architecture

```text
process A                                      process B
+-------------------------------+              +-------------------------------+
| demo application              |              | demo application              |
|   NATS text protocol          |              |   NATS text protocol          |
|          |                    |              |          |                    |
|   Server.InProcessConn()      |              |   Server.InProcessConn()      |
|          | net.Pipe           |              |          | net.Pipe           |
|   embedded NATS server        |<------------>|   embedded NATS server        |
|                               | local route  |                               |
+-------------------------------+     TCP      +-------------------------------+
```

Shared code is organized as follows:

- `pkg/embeddedserver`: reusable local server configuration and startup.
- `pkg/natswire`: reusable minimal synchronous NATS protocol implementation.
- `pkg/jetstream`: reusable JetStream JSON API calls over `natswire`.
- `internal/demo`: shared command behavior types.

## Verification

```powershell
task all
```
