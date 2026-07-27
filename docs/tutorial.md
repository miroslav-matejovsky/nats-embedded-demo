# Embedded NATS tutorial with in-process connections

> Scope: this document describes the demos currently implemented in the
> repository. They use the official `nats.go` client connected to an embedded
> server over `Server.InProcessConn()`, so no client TCP connection exists.
> See [Core broker from scratch](core-broker-from-scratch.md) for the
> direct-call design and [Existing alternatives](alternatives.md) for
> maintained options.

## 1. What this demo proves

An application can embed a NATS server and use the full `nats.go` client
without any client network connection. `Server.InProcessConn()` returns a
`net.Conn` connected to a server-side client through `net.Pipe`, and `nats.go`
can use that connection as its transport.

Two equivalent wiring options are demonstrated:

- `pkg/natsinprocess`: set the `nats.Options` field `InProcessServer` (or the
  `nats.InProcessServer(...)` option). The client calls
  `Server.InProcessConn()` itself.
- `pkg/natsdialer`: implement `nats.CustomDialer` with a `Dial` method that
  returns `Server.InProcessConn()`, registered with `nats.SetCustomDialer`,
  following <https://docs.nats.io/using-nats/developer/tutorials/custom_dialer>.

The `cmd/inprocess-*` and `cmd/dialer-*` applications are otherwise identical
and accept the same flags. The examples below use the in-process variants; the
dialer variants behave the same.

## 2. Comparing the two approaches

Both approaches end up on the same transport: a `net.Pipe` created by
`Server.InProcessConn()`. Message flow, semantics, and performance are
identical. They differ in how the connection is wired and how much control the
application keeps.

### InProcessServer option (`pkg/natsinprocess`)

Pros:

- One options field; no extra type to write or maintain.
- Intent is explicit: the option exists specifically for embedded servers.
- No placeholder URL. The client knows it is not dialing a network address.

Cons:

- Fixed behavior. The client always connects in-process; there is no place to
  add fallback servers, transport selection, or connection-time logic.
- Ties the client options to the `nats.InProcessConnProvider` interface, which
  in practice means holding a reference to the running `server.Server`.

### Custom dialer (`pkg/natsdialer`)

Pros:

- Full control over connection establishment. The dialer can log, wrap the
  connection, select among transports (in-process, Unix socket, TCP), or
  implement custom fallback logic.
- The same pattern works for any transport that yields a `net.Conn`, not only
  embedded servers.
- Documented upstream as a general-purpose extension point.

Cons:

- More code: a dedicated type implementing `nats.CustomDialer`.
- The connection URL becomes a placeholder that is parsed but never dialed,
  which can mislead readers and monitoring.
- The dialer ignores the dialed address, so nats.go's multi-server URL
  handling silently loses its meaning.

For a plain embedded-server setup the two are interchangeable. Choose based on
whether the fixed in-process wiring is enough or connection establishment
needs custom logic.

## 3. Prerequisites

- Go version from `go.mod`.
- Task, if running the full project checks.
- Two PowerShell terminals for the multi-process examples.

No external `nats-server` executable is needed.

All listeners and routes bind or connect only to loopback addresses. The demo
rejects a route whose host is not `localhost` or a loopback IP address.

## 4. Standalone core NATS

Run:

```powershell
go run ./cmd/inprocess-nats --client-port=-1 --cluster-port=-1
```

With the default `--mode=both`, the process:

1. Starts one embedded server.
2. Connects `nats.go` through the in-process connection.
3. Creates a synchronous subscription on `demo.core` and flushes it.
4. Publishes one message and flushes it.
5. Receives the message and shuts down.

Ports set to `-1` use random local ports. With no `--routes`, clustering is
disabled.

## 5. Core NATS across two processes

Each process owns its own embedded server and its own in-process client
connection. The servers connect through loopback route ports.

Terminal A:

```powershell
go run ./cmd/inprocess-nats `
  --name=core-a `
  --client-port=4222 `
  --cluster-port=6222 `
  --routes=nats://127.0.0.1:6223 `
  --mode=subscriber `
  --timeout=2m
```

Terminal B:

```powershell
go run ./cmd/inprocess-nats `
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

## 6. Standalone JetStream

Run:

```powershell
go run ./cmd/inprocess-jetstream `
  --client-port=-1 `
  --cluster-port=-1 `
  --store-dir=.test-results/standalone-js
```

The process:

1. Starts an embedded server with JetStream enabled.
2. Waits until the JetStream account API responds.
3. Creates or updates a file-backed stream with `CreateOrUpdateStream`.
4. Creates or updates a durable pull consumer with `CreateOrUpdateConsumer`.
5. Publishes with `jetstream.Publish` and prints the stream sequence from the
   acknowledgement.
6. Fetches the next message with `Consumer.Fetch` and acknowledges it.

The default file-backed stream survives process restarts when the same store
directory is reused. Use a new directory for a clean run.

## 7. JetStream across two processes

JetStream clustering requires configured routes. Each node below has a route to
the other node, including the node that starts first.

Terminal A:

```powershell
go run ./cmd/inprocess-jetstream `
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
go run ./cmd/inprocess-jetstream `
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

## 8. Server internals and process boundaries

`Server.InProcessConn()` creates a `net.Pipe`. The server registers its end by
calling its internal client creation path. From that point, parsing, permission
checks, subject matching, route forwarding, and JetStream handling use the same
server machinery as a TCP client. The `nats.go` client speaks the normal NATS
protocol over the pipe, so all client-library features (reconnect handling,
concurrent dispatch, request/reply, the JetStream API) work unchanged.

The in-process connection does not cross process boundaries. Cross-process
traffic follows this path:

```text
application A (nats.go)
  -> net.Pipe
  -> embedded server A
  -> loopback NATS route
  -> embedded server B
  -> net.Pipe
  -> application B (nats.go)
```

This preserves normal NATS routing behavior while avoiding a local TCP client
connection inside each process.

## 9. Run checks

```powershell
task all
```

The tests start embedded servers with no TCP client listener (`DontListen`).
They exercise both connection options, core delivery, JetStream stream and
consumer creation, persistence acknowledgement, delivery, and explicit
acknowledgement.
