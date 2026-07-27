# Existing alternatives

This document surveys maintained options for local messaging with a direct Go
API and compares them with the embedded-NATS approach used in this repository.
The ideal being evaluated is: direct Go publish and subscribe calls, separate
local processes, and no network transport.

## 1. Decision summary

No existing library can provide direct Go function calls between separate
processes. That combination is physically impossible. The practical choices
are:

1. Keep all message participants in one process and use an in-memory Go
   broker.
2. Expose direct Go calls to a local broker in each process and accept a hidden
   IPC transport between brokers.
3. Use an embedded file-backed broker and accept serialization, locking, and
   different delivery semantics.
4. Use a normal messaging server and maintained client, accepting the protocol
   boundary.

For the smallest same-process solution, use Go channels or Watermill
GoChannel. For local multi-process messaging where a shared file and durable
semantics are acceptable, Watermill SQLite is the closest maintained embedded
option found. For transient Core NATS semantics across processes, normal NATS
clients remain lower risk than building and maintaining a custom routing
protocol.

## 2. Comparison

| Alternative | Direct local Go API | Separate processes | Persistence | Main mismatch or cost |
| --- | --- | --- | --- | --- |
| Go channels or a small custom broker | Yes | No | No | Must implement subjects, fanout, limits, lifecycle, and observability |
| Watermill GoChannel | Yes | No | No | Same instance only; no consumer groups |
| Go CDK `mempubsub` | Yes | No | No | At-least-once acknowledgement model; intended for tests and local development |
| Go Micro memory broker | Yes | No | No | Pulls in a broader framework; memory implementation remains process-local |
| Watermill SQLite | Yes | Yes, through a shared database file | Yes | Polling and locking; semantics are durable and acknowledgement-oriented |
| Centrifuge MemoryBroker | Yes | No | No | Designed for realtime client systems, not a general Core NATS replacement |
| Custom broker plus named pipe or Unix socket | Yes, to the local node | Yes | No unless added | Must own framing, routing, limits, reconnection, and cross-node queue groups |
| Embedded NATS plus `InProcessConn` | No, it is `net.Pipe` plus protocol | Yes, through NATS routes | No for Core NATS | Exact server semantics, but no direct-call Go API |
| External NATS, Redis, or PostgreSQL | No | Yes | Product-dependent | Separate service or existing database dependency |

## 3. Same-process alternatives

### 3.1 Go channels and a small broker

Go channels are the lowest-dependency choice when requirements are narrow.
They provide typed or untyped queues, blocking or non-blocking sends, and
close-based lifecycle signaling.

Channels alone are point-to-point queues. They do not provide:

- subject matching;
- one-to-many fanout;
- queue groups separate from ordinary subscribers;
- request/reply inboxes;
- safe dynamic subscription removal;
- per-subscriber byte limits;
- metrics and slow-consumer reporting.

A small broker wrapped around channels can provide those features. This is the
best fit when the goal is specifically to own a direct-call implementation and
the system will stay in one process.

### 3.2 Watermill GoChannel

[Watermill GoChannel](https://watermill.io/pubsubs/gochannel/) is an in-memory
publish/subscribe implementation. Publisher and subscriber must share the same
GoChannel instance. Every subscriber receives every message, messages are not
marshaled, and data is not persistent.

Advantages:

- ready-made direct Go API;
- no server or transport;
- fanout behavior;
- part of an established messaging abstraction.

Limitations for this requirement:

- no cross-process communication;
- no consumer groups;
- its message acknowledgement model is Watermill's model, not Core NATS
  at-most-once semantics;
- NATS subject wildcard behavior would need an adapter or separate router.

Use it when all components can run in one process and a maintained abstraction
is preferable to a custom broker.

### 3.3 Go CDK in-memory Pub/Sub

[Go CDK `mempubsub`](https://pkg.go.dev/gocloud.dev/pubsub/mempubsub) provides
in-memory topics and subscriptions with an acknowledgement and redelivery
model. Its documentation describes it as intended for local development and
testing, not production.

It is useful when application code already uses Go CDK's portable Pub/Sub API.
It is not a close Core NATS substitute:

- it is process-local;
- it models at-least-once delivery;
- it has explicit acknowledgement behavior;
- it does not supply NATS subject and queue-group semantics.

### 3.4 Go Micro memory broker

[Go Micro's broker API](https://go-micro.dev/docs/broker.html) exposes
`Publish` and `Subscribe` and includes an in-memory implementation. It is a
direct Go API and is suitable for components already built around Go Micro.

It remains process-local. Adopting a larger service framework only for this
broker is difficult to justify when a small package or Watermill GoChannel is
enough.

### 3.5 Centrifuge MemoryBroker

[Centrifuge](https://github.com/centrifugal/centrifuge) includes a memory
broker for a single node and other brokers for multi-node realtime systems. It
is designed around realtime client connections, channels, presence, history,
and WebSocket-style application delivery.

It is a good alternative when the actual goal is a realtime server for browser
or mobile clients. It is broader than an internal Go event bus, and adopting it
only to imitate Core NATS subjects would add the wrong abstractions.

## 4. Local multi-process alternatives

### 4.1 Watermill SQLite

[Watermill SQLite](https://watermill.io/pubsubs/sqlite/) is an embedded,
pure-Go, file-backed publish/subscribe implementation. It supports persistence,
consumer groups, and ordered delivery without a separate database server.

This is the closest maintained option when all of these are acceptable:

- processes can share a database file;
- messages may be serialized;
- database polling latency is acceptable;
- durable, acknowledgement-oriented behavior is acceptable instead of Core
  NATS at-most-once behavior.

Important costs:

- SQLite locking and polling become part of the message path;
- crash and redelivery behavior differ from transient NATS;
- database file placement and permissions become deployment concerns;
- Watermill labels the implementation beta and documents limitations around
  locks and timing.

It should be evaluated with realistic process counts, message sizes, rates,
filesystem behavior, and shutdown cases.

### 4.2 Named pipes and Unix domain sockets

A custom local broker can expose direct Go methods to application code while a
private adapter connects broker nodes:

```go
type PeerTransport interface {
    Listen(ctx context.Context, endpoint string) (PeerListener, error)
    Dial(ctx context.Context, endpoint string) (PeerConn, error)
}
```

On Windows, [Microsoft go-winio](https://github.com/microsoft/go-winio)
provides named-pipe support. On Unix platforms, Go's
[`net` package](https://pkg.go.dev/net) supports Unix domain sockets.

This meets "no network port" but not "no transport." The implementation still
needs:

- message framing and maximum sizes;
- peer identity and handshake;
- subscription interest propagation;
- bounded queues and partial-write handling;
- liveness detection and reconnect;
- duplicate-connection resolution;
- cross-node queue group selection;
- deterministic shutdown and multi-process tests.

This is an implementation strategy, not a ready-made broker alternative.

### 4.3 Shared SQLite used directly

An application can implement a table-backed queue directly with SQLite in
[write-ahead logging mode](https://www.sqlite.org/wal.html). Separate
processes publish by inserting rows and consume by claiming or deleting rows.

This can be suitable for low-rate local work queues. It does not naturally
provide Core NATS fanout and ephemeral interest:

- one row consumed once models a work queue;
- fanout needs a delivery or cursor per subscriber;
- dynamic ephemeral subscriptions complicate cleanup;
- polling adds latency;
- retained rows introduce persistence and recovery semantics.

Use Watermill SQLite unless the database schema itself is a required part of
the application.

### 4.4 Existing PostgreSQL

If an application already depends on PostgreSQL,
[`LISTEN` and `NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html)
can signal events between processes. This is neither embedded nor
dependency-free, but it can avoid adding another operational service.

Payload size, transaction timing, connection ownership, and reconnect behavior
must be considered. It is not a Core NATS replacement and should not be added
only for local messaging.

## 5. Embedded NATS as used in this repository

This repository uses embedded `nats-server` and
`Server.InProcessConn()`. It has several valuable properties:

- real Core NATS subject matching and queue groups;
- tested slow-consumer behavior;
- real routes between embedded server processes;
- maintained protocol and operational behavior.

It does not provide direct publish and subscribe function calls. The in-process
connection is a `net.Pipe`, and application messages are NATS protocol frames.
This makes it a "no TCP client connection" solution, but not a
"no client protocol" solution.

The normal NATS client is still the lowest-risk option when exact Core NATS
semantics across separate processes matter more than avoiding a client
dependency.

## 6. Selection guide

### All participants can run in one process

Choose one:

- Go channels and a small broker when the API and semantics should be owned by
  this project.
- Watermill GoChannel when a maintained generic message abstraction is useful.
- Go CDK `mempubsub` when the code already uses Go CDK and its at-least-once
  model is desired for development.

Do not embed a full NATS server merely to pass messages between goroutines.

### Separate local processes are mandatory

Choose one:

- Watermill SQLite when durability, shared-file deployment, and polling are
  acceptable.
- Named pipes or Unix domain sockets plus a custom broker only when owning the
  routing system is an explicit product decision.
- A maintained NATS client when Core NATS semantics and performance are the
  priority.
- An existing Redis or PostgreSQL dependency when its weaker or different
  messaging semantics already satisfy the application.

There is no option that simultaneously provides separate processes, direct Go
calls between them, no IPC, and no shared storage.

## 7. Evaluation checklist

Before choosing an alternative, answer:

- Must components be separate operating-system processes?
- Is an IPC stream acceptable if no TCP port is opened?
- Is data loss acceptable when a subscriber, peer, or process is unavailable?
- Is persistence desirable or explicitly unwanted?
- Is delivery at-most-once or at-least-once?
- Is ordinary fanout required?
- Are queue groups required across processes?
- Are wildcard subjects required?
- What are the maximum message size and publish rate?
- What should happen to a slow subscriber?
- Is publisher-local ordering sufficient?
- Which operating systems must be supported?
- How are local endpoints discovered and access-controlled?
- Who will own protocol compatibility, reconnect behavior, metrics, and race
  testing over time?

These answers usually eliminate most options before benchmarking begins.

