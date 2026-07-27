# Embedded NATS tutorial without a client library

## 1. What this demo proves

An application can embed a NATS server and exchange messages without importing
`nats.go`.

The public server API does not expose application-level `Publish` and
`Subscribe` methods. Instead, `Server.InProcessConn()` returns a `net.Conn`
connected to a server-side client through `net.Pipe`. The application writes
normal NATS protocol commands to that connection.

This is direct use of the embedded server's supported in-process connection.
It is not direct access to the server's message-matching or JetStream storage
objects. Those implementation types and methods are intentionally unexported.

## 2. Prerequisites

- Go version from `go.mod`.
- Task, if running the full project checks.
- Two PowerShell terminals for the multi-process examples.

No external `nats-server` executable is needed. No NATS client library is used
by project source code.

All listeners and routes bind or connect only to loopback addresses. The demo
rejects a route whose host is not `localhost` or a loopback IP address.

## 3. Standalone core NATS

Run:

```powershell
go run ./cmd/nats --client-port=-1 --cluster-port=-1
```

With the default `--mode=both`, the process:

1. Starts one embedded server.
2. Calls `Server.InProcessConn()`.
3. Completes the NATS `INFO` and `CONNECT` handshake.
4. sends `SUB demo.core 1`.
5. sends `PUB demo.core <size>` and the payload.
6. reads the resulting `MSG` frame.
7. shuts down.

Ports set to `-1` use random local ports. With no `--routes`, clustering is
disabled.

## 4. Core NATS across two processes

Each process owns its own embedded server and its own in-process application
connection. The servers connect through loopback route ports.

Terminal A:

```powershell
go run ./cmd/nats `
  --name=core-a `
  --client-port=4222 `
  --cluster-port=6222 `
  --routes=nats://127.0.0.1:6223 `
  --mode=subscriber `
  --timeout=2m
```

Terminal B:

```powershell
go run ./cmd/nats `
  --name=core-b `
  --client-port=4223 `
  --cluster-port=6223 `
  --routes=nats://127.0.0.1:6222 `
  --mode=publisher `
  --message="hello across embedded servers" `
  --timeout=2m
```

Start Terminal A first. Its route connection retries until Terminal B starts.
Terminal B waits until routed subscription interest for `demo.core` is visible,
then publishes. Terminal A receives one message and exits.

Core NATS messages are transient. If no subscriber interest exists when a
message is published, the message is not retained.

## 5. Standalone JetStream

Run:

```powershell
go run ./cmd/jetstream `
  --client-port=-1 `
  --cluster-port=-1 `
  --store-dir=.test-results/standalone-js
```

The process:

1. Starts an embedded server with JetStream enabled.
2. waits for `$JS.API.INFO` to respond.
3. requests stream creation on `$JS.API.STREAM.CREATE.DEMO`.
4. requests durable pull-consumer creation through the JetStream API.
5. publishes to `demo.jetstream` with an inbox reply subject.
6. reads the JSON publish acknowledgement and stream sequence.
7. requests the next consumer message.
8. publishes an acknowledgement to the delivery reply subject.

The default file-backed stream survives process restarts when the same store
directory is reused. Use a new directory for a clean run.

## 6. JetStream across two processes

JetStream clustering requires configured routes. Each node below has a route to
the other node, including the node that starts first.

Terminal A:

```powershell
go run ./cmd/jetstream `
  --name=js-a `
  --client-port=4322 `
  --cluster-port=6322 `
  --routes=nats://127.0.0.1:6323 `
  --store-dir=.test-results/js-a `
  --mode=subscriber `
  --timeout=2m
```

Terminal B:

```powershell
go run ./cmd/jetstream `
  --name=js-b `
  --client-port=4323 `
  --cluster-port=6323 `
  --routes=nats://127.0.0.1:6322 `
  --store-dir=.test-results/js-b `
  --mode=publisher `
  --message="persistent hello across embedded servers" `
  --timeout=2m
```

The default stream replica count is one. This keeps the two-process example
simple, but the stream data exists on only one JetStream node. A production
cluster normally uses three or more nodes and an appropriate replica count.

The durable consumer is named `demo-consumer`. A subscriber can restart with
the same store and consumer name and continue from its acknowledged position.

## 7. NATS protocol used by the demo

The `pkg/natswire` package implements only the frames needed here.

| Direction | Frame | Purpose |
| --- | --- | --- |
| Server to app | `INFO {...}` | Announces server capabilities. |
| App to server | `CONNECT {...}` | Enables headers and no-responder status messages. |
| App to server | `SUB <subject> <sid>` | Registers interest. |
| App to server | `UNSUB <sid> [max]` | Removes or limits interest. |
| App to server | `PUB <subject> [reply] <size>` | Publishes a payload. |
| Server to app | `MSG <subject> <sid> [reply] <size>` | Delivers a payload. |
| Server to app | `HMSG ...` | Delivers headers or a status response. |
| Either direction | `PING` / `PONG` | Flushes commands and keeps the connection alive. |

A request is built from protocol primitives:

1. create a unique `_INBOX` subject;
2. subscribe to it;
3. set `UNSUB <sid> 1`;
4. publish with the inbox as the reply subject;
5. read one response.

JetStream uses the same request/reply mechanism. Its control plane is JSON sent
to subjects under `$JS.API`. A persisted publish acknowledgement is also a JSON
reply. A consumed message contains an acknowledgement subject in its `reply`
field.

## 8. Server internals and process boundaries

`Server.InProcessConn()` creates a `net.Pipe`. The server registers its end by
calling its internal client creation path. From that point, parsing, permission
checks, subject matching, route forwarding, and JetStream handling use the same
server machinery as a TCP client.

The in-process connection does not cross process boundaries. Cross-process
traffic follows this path:

```text
application A
  -> net.Pipe
  -> embedded server A
  -> loopback NATS route
  -> embedded server B
  -> net.Pipe
  -> application B
```

This preserves normal NATS routing behavior while avoiding a local TCP client
connection inside each process.

## 9. Limits of the experiment

The minimal protocol code is deliberately synchronous and incomplete. It does
not implement:

- automatic reconnect and subscription replay;
- authentication, TLS, credentials, or account selection;
- concurrent callback dispatch;
- client-side buffering and slow-consumer policies;
- queue subscriptions;
- wildcard validation;
- JetStream management beyond this demo;
- advanced JetStream headers, flow control, heartbeats, or batch pulls.

Use a maintained NATS client in production. This code is useful for evaluating
the embedded server boundary and understanding the protocol, not for replacing
the full client.

## 10. Run checks

```powershell
task all
```

The tests start embedded servers with random ports or no TCP listener. They
exercise core delivery, protocol status handling, JetStream stream and consumer
creation, persistence acknowledgement, delivery, and explicit acknowledgement.
