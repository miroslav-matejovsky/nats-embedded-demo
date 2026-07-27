# Building a direct-call Core NATS-style broker from scratch

## 1. Result

A direct Go API for publish and subscribe is practical when all publishers,
subscribers, and the broker are in one process.

It is not possible to call Go functions directly across process boundaries.
Processes have separate address spaces, heaps, goroutines, maps, and channels.
If several applications run as separate processes, each application can call
its local embedded broker directly, but the broker instances must communicate
through an inter-process transport.

This leads to two distinct systems:

1. An in-process message broker with a direct Go API.
2. An optional node-to-node routing layer using named pipes, Unix domain
   sockets, loopback TCP, shared memory, or a shared database.

The second system is a distributed system even when every process runs on one
machine. Hiding its transport behind a Go interface does not remove the
transport, serialization, partial failure, or reconnection problems.

This document defines the minimum useful subset of Core NATS semantics. It
does not define persistence, replay, durable consumers, or replicated storage.

## 2. What the embedded NATS server does not expose

The public `nats-server` Go package can start and stop a server. Its supported
application-facing in-process entry point is `Server.InProcessConn()`. That
method creates a `net.Pipe` and registers one side as a normal NATS client.
Applications still exchange NATS protocol frames through the other side.

The server contains internal clients, subscriptions, subject indexes, message
handlers, account routing, and internal publish functions. Those types and
methods are not exported as a stable application API. Therefore, embedding the
published server module does not provide direct `Publish` and `Subscribe`
method calls.

Using reflection, `unsafe`, source linking tricks, or copied unexported types
would bind an application to implementation details. That is not a new broker
design and is not considered here.

## 3. Minimum behavior to replicate

Core NATS is an interest-based, transient, at-most-once messaging system. A
minimal compatible design needs more than a map of Go channels if it must
preserve the behavior users expect from Core NATS.

### 3.1 Message model

A message needs:

```go
type Message struct {
    Subject string
    Reply   string
    Header  map[string][]string
    Data    []byte
}
```

- `Subject` selects subscribers.
- `Reply` is optional and supports request/reply.
- `Header` is optional. It can be omitted from the first implementation.
- `Data` is an opaque byte slice. The broker does not interpret it.

The implementation must define ownership. The simplest safe contract is:

- `Publish` copies the message metadata and payload before it returns.
- Subscribers must treat received messages as immutable.
- A handler may retain a received message.

Copying has a cost, but an unclear zero-copy contract creates data races and
payload corruption. An optimized immutable or reference-counted payload can be
added only after measurement.

### 3.2 Subjects and interest

Subjects are dot-separated tokens such as `orders.created.eu`.

Subscribers may use:

- A literal token, such as `orders`.
- `*` to match exactly one token.
- `>` to match one or more remaining tokens. It must be the final token.

Publish subjects are literal. Wildcards are subscription syntax, not publish
syntax.

Examples:

| Subscription | Matches | Does not match |
| --- | --- | --- |
| `orders.created` | `orders.created` | `orders.updated` |
| `orders.*` | `orders.created` | `orders.created.eu` |
| `orders.>` | `orders.created`, `orders.created.eu` | `orders` |

Validate subjects when a subscription or publication is created. At minimum,
reject empty subjects, empty tokens, whitespace, and invalid wildcard
placement.

Interest-based delivery means a publication with no matching subscriber is
discarded immediately. It is not retained for a future subscriber.

### 3.3 Fanout

Every matching ordinary subscription receives one copy of a message.

If three ordinary subscriptions match `orders.created`, one publish produces
three deliveries. Two subscriptions owned by the same application are still
two independent subscriptions.

### 3.4 Queue groups

A queue subscription has a subject and a queue group name.

For each matching queue group, exactly one current member receives the
message. Ordinary subscribers still receive their copies. Different queue
groups each receive one copy.

For example, with two ordinary subscribers, three members of queue group
`workers`, and two members of queue group `audit`:

- both ordinary subscribers receive the message;
- one `workers` member receives it;
- one `audit` member receives it.

Core NATS distributes queue messages among available members. A from-scratch
broker may use random selection, round-robin selection, or another documented
fair policy. Selection must not hold the broker lock while invoking a handler.

### 3.5 At-most-once delivery

The broker does not store an acknowledgement state and does not redeliver.
Once a message has been handed to a subscriber mailbox, a process crash or
handler failure can lose it.

This is a central simplification:

- no message log;
- no acknowledgement protocol;
- no redelivery timer;
- no consumer position;
- no duplicate suppression for local delivery;
- no recovery after restart.

At-most-once means zero or one delivery, not guaranteed delivery.

### 3.6 Ordering

The minimum useful guarantee is publisher-local order:

> For one publisher issuing sequential `Publish` calls, a given subscription
> observes those accepted messages in the same order.

There is no global order across concurrent publishers. Queue group members do
not have a meaningful handler-completion order because different handlers run
concurrently.

The implementation must avoid sending one subscription's messages through
multiple unordered worker queues if it claims publisher-local order.

### 3.7 Slow consumers and bounded memory

An unbounded subscriber queue lets one slow handler exhaust process memory. A
production-safe broker needs limits in both message count and bytes.

For a transient NATS-style system, the recommended policy is:

1. Each subscription has a bounded mailbox.
2. Publishing to that mailbox is non-blocking.
3. If the mailbox is full, mark the subscription as slow.
4. Drop the attempted delivery or close the subscription according to a
   configured policy.
5. Expose a counter and a callback or error that makes the loss visible.

Blocking every publisher until the slow subscriber catches up is a valid event
bus design, but it is not NATS-like. It lets one subscriber stall unrelated
work and can deadlock when handlers publish recursively.

Byte limits matter because a count limit of 1,000 messages says nothing about
whether those messages occupy 100 KB or 1 GB.

### 3.8 Request/reply

Request/reply is built from publish and subscribe:

1. Generate a unique reply subject, commonly called an inbox.
2. Subscribe to that inbox.
3. Arrange for the subscription to accept at most one message.
4. Publish the request with the inbox in `Message.Reply`.
5. Wait for the first response, context cancellation, or timeout.
6. Remove the inbox subscription.

A responder publishes its response to the request's `Reply` subject.

The direct API should report a distinct no-responder error when no subscription
matches the request subject at publish time. This avoids waiting for a timeout
when the broker already knows that no responder exists.

### 3.9 Subscription lifecycle

A subscription needs explicit states:

```text
active -> draining -> closed
   |                    ^
   +--------------------+
```

- `Unsubscribe` stops new deliveries and closes promptly.
- `Drain` stops new deliveries, processes messages already accepted into the
  mailbox, then closes.
- `Close` on the broker stops new operations and drains or closes
  subscriptions according to a documented shutdown policy.

Unsubscribe and publish will race in real applications. Define the boundary:
a message already accepted into a mailbox may still run after unsubscribe;
messages matched after the subscription leaves the registry may not.

Handler panics must be recovered at the worker boundary. One application
callback must not crash the broker dispatcher or leave registry locks held.

## 4. Direct Go API

A small initial API can be concrete rather than framework-oriented:

```go
package corebroker

import "context"

type Message struct {
    Subject string
    Reply   string
    Header  map[string][]string
    Data    []byte
}

type Handler func(context.Context, Message)

type SubscribeOptions struct {
    Queue          string
    BufferMessages int
    BufferBytes    int
}

type Options struct {
    MaxPayloadBytes       int
    DefaultBufferMessages int
    DefaultBufferBytes    int
}

type Subscription struct {
    // unexported state
}

func New(options Options) (*Broker, error)

func (b *Broker) Publish(ctx context.Context, message Message) error

func (b *Broker) Subscribe(
    subject string,
    options SubscribeOptions,
    handler Handler,
) (*Subscription, error)

func (b *Broker) Request(
    ctx context.Context,
    subject string,
    data []byte,
) (Message, error)

func (s *Subscription) Unsubscribe() error

func (s *Subscription) Drain(ctx context.Context) error

func (b *Broker) Drain(ctx context.Context) error

func (b *Broker) Close() error
```

Useful exported errors include:

```go
var (
    ErrClosed       = errors.New("broker is closed")
    ErrNoResponders = errors.New("no responders")
    ErrSlowConsumer = errors.New("slow consumer")
)
```

Important API decisions must be documented:

- whether `Publish` means "matched and queued" or only "accepted for routing";
- whether zero matches is success for ordinary publish;
- whether cancellation can interrupt a partially completed fanout;
- whether a handler receives a broker shutdown context or a caller context;
- whether handlers may publish recursively;
- whether a slow-consumer event is returned to the publisher, reported
  asynchronously, or both.

Ordinary publish with zero matches should be successful to match Core NATS.
Request can separately return `ErrNoResponders`.

## 5. Single-process internals

### 5.1 Subscription record

Each internal subscription needs at least:

```go
type subscription struct {
    id          uint64
    pattern     string
    tokens      []string
    queue       string
    handler     Handler
    mailbox     chan delivery
    byteLimit   int
    queuedBytes atomic.Int64
    state       atomic.Uint32
    done        chan struct{}
}
```

The channel capacity limits message count. `queuedBytes` enforces the separate
byte limit. State changes need one owner or atomic compare-and-swap rules.

### 5.2 Subject index

There are two reasonable stages:

1. Start with an exact-subject map:

   ```go
   map[string][]*subscription
   ```

2. Add wildcard matching with a token trie.

A trie node has literal children plus optional `*` and `>` branches. Lookup
walks only branches that can match the published token. This avoids scanning
every subscription for every publication.

The index must support:

- add subscription;
- remove subscription by stable ID;
- match a literal publish subject;
- return ordinary subscriptions;
- group queue subscriptions by queue name.

Avoid exposing trie nodes outside the registry. A snapshot of matched
subscription pointers is sufficient for dispatch.

### 5.3 Concurrency model

Two simple models are viable.

#### Single registry goroutine

Subscribe, unsubscribe, and publish requests go through one control channel.
The control goroutine owns the index and produces immutable delivery target
snapshots.

Advantages:

- simple race behavior;
- deterministic subscription ordering;
- no registry mutex.

Costs:

- every publish passes through one goroutine;
- control-channel backpressure needs its own policy;
- handlers must never run on the control goroutine.

#### Lock and snapshot

Subscribe and unsubscribe take a write lock. Publish takes a read lock, finds
matches, copies target pointers, releases the lock, and enqueues deliveries.

Advantages:

- concurrent subject matching;
- direct and conventional Go implementation.

Costs:

- more race cases around unsubscribe and shutdown;
- queue group selection state needs careful synchronization;
- long match operations delay registry updates.

The lock-and-snapshot model is a reasonable first implementation. Never call a
handler, block on a mailbox, or copy a large payload while holding the registry
lock.

### 5.4 Publish path

The local publish algorithm is:

1. Validate the literal subject and message size.
2. Copy or freeze the message according to the ownership contract.
3. Match the subject in the registry.
4. Snapshot every ordinary target.
5. Select one target from each queue group.
6. Release the registry lock.
7. Attempt non-blocking enqueue to each selected mailbox.
8. Record delivered, no-interest, and slow-consumer metrics.
9. Return the result defined by the API contract.

Fanout needs a payload strategy. Copying the payload once per subscriber is
safe but expensive. A safe compromise is one immutable internal message shared
by all delivery records, with the public contract forbidding mutation.

### 5.5 Handler workers

The simplest model is one goroutine per subscription:

```text
subscription mailbox -> worker goroutine -> handler
```

It naturally preserves order for that subscription. It can be expensive with
hundreds of thousands of subscriptions, but it is a pragmatic starting point.
A shared worker pool requires per-subscription sequencing and is more complex.

The worker must:

- decrement queued-byte accounting when taking a message;
- recover and report handler panics;
- stop accepting messages before drain begins;
- close its completion channel exactly once.

### 5.6 Metrics

Even a small broker needs observable failure. Track at least:

- published messages and bytes;
- matched and delivered messages;
- publications with no interest;
- slow-consumer events and dropped deliveries;
- active ordinary and queue subscriptions;
- handler panics;
- request successes, no responders, and timeouts;
- current and maximum mailbox messages and bytes.

Metrics can initially be counters returned by a `Stats` method. A monitoring
HTTP server is not part of the broker minimum.

## 6. Multiple processes

### 6.1 The unavoidable boundary

This path cannot exist:

```text
process A Go call -> process B Go function
```

The operating system must move data between address spaces:

```text
application A
  -> direct call to broker A
  -> encode route message
  -> IPC transport
  -> decode at broker B
  -> direct local delivery to application B
```

Named pipes and Unix domain sockets avoid a routable network listener, but they
are still streams requiring framing, serialization, buffering, and failure
handling. Shared memory avoids a socket, but requires a ring buffer, ownership
protocol, synchronization primitives, crash recovery, and peer discovery. It
is usually harder to make correct.

### 6.2 Smallest workable topology

For a local-only demo with a small fixed number of processes, use a static full
mesh:

- each node has a stable unique node ID;
- every pair of nodes has exactly one connection;
- node ID ordering decides which side owns the connection;
- a local publication is delivered locally and forwarded to interested peers;
- a publication received from a peer is delivered locally and is not forwarded
  again.

The last rule is valid only for a full mesh. It prevents routing loops and
duplicate messages. If nodes are not fully connected, the protocol needs
multi-hop routing, message IDs, a hop limit, duplicate suppression, and a
topology algorithm.

This mirrors the important shape of a Core NATS cluster: servers share
subscription interest and forward a message only where interest exists.

### 6.3 Peer protocol

The route protocol needs framed messages even though the application API does
not. A minimal set is:

| Frame | Purpose |
| --- | --- |
| `HELLO` | Protocol version, node ID, process generation, limits. |
| `SUB` | Add or increment interest for a subject pattern and queue group. |
| `UNSUB` | Remove or decrement interest. |
| `SNAPSHOT` | Replace peer interest after connect or reconnect. |
| `PUB` | Subject, reply, optional headers, and payload. |
| `PING` / `PONG` | Detect a dead or stalled peer. |
| `ERROR` | Reject an invalid frame or incompatible peer. |

Use length-prefixed binary or JSON frames for a demo. Length prefixes make
partial reads and payload boundaries explicit. Every decoder must enforce a
maximum frame size before allocating memory.

`HELLO` should carry a random process generation in addition to the stable node
ID. It lets peers distinguish a delayed connection from an older process
instance.

### 6.4 Interest propagation

Forwarding every message to every peer works for a tiny demo, but it does not
replicate the interest-based behavior that makes NATS efficient.

Each node should aggregate local interest:

```text
(subject pattern, queue group) -> local member count
```

Send `SUB` when a count changes from zero to one. Send `UNSUB` when it changes
from one to zero. For queue groups, also propagate enough weight or membership
information to choose one destination fairly across nodes.

On every new connection or reconnection:

1. exchange `HELLO`;
2. send a complete interest `SNAPSHOT`;
3. apply later `SUB` and `UNSUB` changes in order;
4. only then consider the route synchronized.

A snapshot avoids trying to replay an unbounded history of changes made while
a peer was disconnected.

### 6.5 Queue groups across nodes

Ordinary interest is simple: forward one route message to every peer with
matching ordinary interest.

Queue groups need global selection. If two nodes both select a local member,
the group receives duplicates. A minimal algorithm is:

1. Build a candidate set containing local membership and interested peers.
2. Select exactly one candidate for each queue group.
3. If local was selected, choose one local member.
4. If a peer was selected, send one publication tagged for that queue group.
5. The peer chooses one of its local members and does not fan out to the whole
   group.

Candidate weights should reflect member counts if roughly even distribution
across all workers matters. Equal weighting by node instead favors members on
small nodes.

There is no transaction between nodes. A selected peer can fail after the
sender considers the message sent and before local delivery. At-most-once
semantics permit that loss.

### 6.6 Peer backpressure

Each peer requires a bounded outbound queue. A stalled process must not stall
the local broker.

When the peer queue exceeds its message or byte limit:

- disconnect the peer;
- discard queued transient messages;
- reconnect with backoff and jitter;
- exchange a fresh interest snapshot.

Do not silently grow the queue. Do not block local publishers indefinitely.
Expose peer disconnects and route drops through metrics and logs.

### 6.7 Reconnection and duplicate connections

Local processes start and stop in arbitrary order. The routing layer needs:

- retry with exponential backoff and jitter;
- handshake timeout;
- read and write deadlines or heartbeat timeout;
- a deterministic duplicate-connection rule;
- full interest resynchronization after reconnect;
- shutdown that stops reconnect loops.

At-most-once semantics mean publications made while a peer is disconnected are
lost for that peer. Buffering them for later changes the system into a durable
or at-least-once design and is outside this minimum.

### 6.8 Local IPC choices

| Transport | Platforms | Advantages | Costs |
| --- | --- | --- | --- |
| Windows named pipe | Windows | Local namespace, access controls, no TCP port | Windows-specific adapter and stream framing |
| Unix domain socket | Linux, macOS, Unix | Local filesystem namespace, mature Go support | Not the Windows transport |
| Loopback TCP | All common platforms | Simplest portable implementation | Uses the network stack and local ports |
| Shared SQLite file | All common platforms | Discovery and durable handoff can be delegated to a database | Polling, locking, serialization, different delivery semantics |
| Shared memory | Platform-specific details | Potentially low copy and low latency | Most complex synchronization and crash behavior |

For a Windows-only local demo, a named pipe is the clearest interpretation of
"no network." For a portable demo, hide named pipes and Unix domain sockets
behind a small `PeerTransport` interface. Loopback TCP remains the simplest
portable reference implementation.

## 7. What is not required for the minimum

The following NATS server capabilities are not required for the stated
transient local broker:

- the NATS client text protocol and parser;
- public TCP client listeners;
- accounts, users, permissions, credentials, and TLS;
- configuration files and live configuration reload;
- leaf nodes, gateways, and superclusters;
- service import and export rules;
- WebSocket and MQTT listeners;
- a monitoring HTTP service;
- persistence, replay, acknowledgement, and redelivery;
- replicated logs and consensus.

Local IPC still needs operating-system access controls if untrusted local users
can reach it. "Local only" is not an authentication mechanism.

## 8. Implementation stages

### Stage 1: direct single-process bus

Implement:

- exact literal subjects;
- ordinary fanout;
- bounded per-subscription mailboxes;
- at-most-once delivery;
- unsubscribe, drain, and broker shutdown;
- payload ownership rules;
- metrics and handler panic recovery.

This proves the direct-call API and concurrency model without routing or
wildcard complexity.

### Stage 2: Core NATS-style local semantics

Add:

- subject validation and wildcard trie;
- queue groups;
- request/reply and no-responder detection;
- message and byte limits;
- publisher-local ordering tests;
- race and benchmark coverage.

At this point the package is useful as an in-process Core NATS-style broker. It
is not a NATS server and is not protocol-compatible with NATS clients.

### Stage 3: local multi-process nodes

Add:

- peer transport abstraction;
- one platform transport first;
- static full-mesh topology;
- framed peer protocol;
- interest snapshots and deltas;
- route health, limits, reconnection, and duplicate-connection handling;
- cross-node queue group selection;
- multi-process tests.

Do not start with dynamic topology, shared memory, or multi-hop routes. They
expand the failure surface without proving the basic requirement.

## 9. Test plan

### Unit tests

- valid and invalid subjects;
- every `*` and `>` matching boundary;
- ordinary fanout to zero, one, and many subscribers;
- exactly one delivery per queue group;
- publisher-local order;
- full mailbox behavior and byte limits;
- unsubscribe racing with publish;
- drain processes accepted work and rejects new work;
- handler panic isolation;
- request response, no responder, timeout, and cancellation;
- idempotent close operations.

Use deterministic synchronization channels in tests. Avoid sleeps.

### Concurrency tests

- run all broker tests under `go test -race`;
- publish concurrently while subscriptions are added and removed;
- close during publish and during handler execution;
- recursively publish from a handler;
- force mailboxes and route queues to their exact boundaries.

### Multi-process tests

Start helper processes with unique pipe or socket names and verify:

- full interest snapshot on first connect;
- subscription add and removal propagation;
- local and remote fanout;
- one queue delivery across several nodes;
- no forwarding loop;
- deterministic duplicate-connection resolution;
- peer death during a partial frame;
- reconnect with a new process generation;
- publications during disconnection are not replayed;
- clean shutdown stops every listener and reconnect goroutine.

Fuzz the frame decoder with truncated, oversized, and malformed frames.

### Benchmarks

Measure:

- exact and wildcard match cost;
- publish latency with zero, one, and many subscribers;
- payload-copy cost by size;
- slow-subscriber isolation;
- route throughput and allocation count;
- subscribe and unsubscribe churn.

Do not optimize until profiles identify the limiting component.

## 10. Effort and maintenance boundary

The stages have very different risk:

- A direct single-process event bus is a small, conventional Go package.
- Wildcards, queue groups, limits, lifecycle races, and stable observability
  turn it into a real broker.
- Multi-process interest routing, backpressure, reconnection, and membership
  turn it into a distributed system with ongoing compatibility and operations
  costs.

The code volume is not the main risk. The difficult work is defining behavior
at cancellation, shutdown, queue overflow, peer failure, and concurrent
subscription changes, then keeping those behaviors tested.

If all messaging can stay in one process, build or adopt an in-memory broker
and use direct calls. If separate processes are mandatory, first choose an
existing IPC-backed alternative. A custom router is justified only when its
specific latency, dependency, or deployment constraints are worth owning a new
messaging system.

## 11. Primary references

- [`nats-server` Go package](https://pkg.go.dev/github.com/nats-io/nats-server/v2/server)
- [NATS: What is NATS?](https://docs.nats.io/nats-concepts/what-is-nats)
- [NATS subjects and wildcards](https://docs.nats.io/nats-concepts/subjects)
- [NATS queue groups](https://docs.nats.io/nats-concepts/core-nats/queue)
- [NATS request/reply](https://docs.nats.io/nats-concepts/core-nats/reqreply)
- [NATS slow consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers)
- [NATS clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering)
- [NATS cluster protocol](https://docs.nats.io/reference/reference-protocols/nats-server-protocol)
