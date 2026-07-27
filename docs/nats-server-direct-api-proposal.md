# Proposal for a direct embedded API in nats-server

## 1. Research result

Research date: 2026-07-27.

No existing nats-server issue, pull request, or discussion was found that
proposes an exported, protocol-free Go API for direct Core NATS publish and
subscribe calls.

There is important adjacent work:

- [nats-server PR #2360](https://github.com/nats-io/nats-server/pull/2360)
  added `Server.InProcessConn()` and was merged on 2022-06-28.
- [nats.go PR #774](https://github.com/nats-io/nats.go/pull/774) added
  `nats.InProcessServer()` and was merged on 2022-07-31.
- [nats-server issue #5387](https://github.com/nats-io/nats-server/issues/5387)
  and [PR #5395](https://github.com/nats-io/nats-server/pull/5395) added an
  `IN_PROCESS` connection type to authorization rules.
- [nats-server PR #8364](https://github.com/nats-io/nats-server/pull/8364)
  proposes Unix domain socket client connections. It remains a protocol client
  connection and does not provide direct Go calls.

The current supported upstream answer is therefore:

```go
nc, err := nats.Connect("", nats.InProcessServer(ns))
```

This avoids TCP but still imports `nats.go`, creates a client, performs the
NATS handshake, encodes commands, parses frames, and uses `net.Pipe`.

The requested API would be new upstream functionality, not a different use of
an existing public API.

### 1.1 Search coverage

The nats-server issue and pull request search included:

- `InProcessConn`
- `in-process`
- `direct publish`
- `direct subscribe`
- `embedded API`
- `programmatic publish`
- `server-side publish`
- `without client`
- `internal client`
- `subscribeInternal`
- `sendInternalAccountMsg`

GitHub discussions and general web results were also searched for the same
concepts. Searches can miss deleted content, private discussions, unindexed
comments, or terminology that does not overlap these terms. The conclusion is
"no equivalent public proposal found," not proof that the idea was never
mentioned in a meeting, Slack channel, or unrelated issue.

## 2. What the existing in-process work decided

PR #2360 is the closest precedent and should be referenced in any new issue.

Its stated use case was an application that can run as:

- a monolith in one process;
- several processes using the same messaging model;
- environments where opening sockets is unsuitable, including mobile and
  WebAssembly.

The accepted solution created a `net.Pipe` and registered one end as a normal
server client. The companion nats.go PR made that pipe usable by the normal Go
client.

This choice retained:

- authentication and authorization;
- client connection behavior;
- subscription lifecycle;
- server limits;
- slow-consumer handling;
- headers and request/reply;
- protocol compatibility;
- normal client metrics and monitoring;
- one implementation of client semantics.

The review also exposed a `net.Pipe` problem. Because it is unbuffered, a server
write and client write can block each other during connection setup or
authorization failure. Maintainers required testing and discussed the change
internally before accepting it.

The PR remained open for almost a year. Maintainers described the use case as
important for future deployments but initially considered it a lower priority.
This history suggests that a second in-process API needs a specific benefit and
a precise behavioral contract. "It avoids TCP" is not sufficient because
`InProcessConn()` already does that.

## 3. Related authorization decisions

Issue #5387 asked how credentials could be restricted to in-process
connections when the same embedded server also accepted network clients.
Merged PR #5395 added a distinct `IN_PROCESS` connection type that can be used
in JWT connection restrictions.

This is relevant because the direct API has no NATS `CONNECT` operation and no
client credentials unless it deliberately creates a virtual authenticated
client.

There are two possible security models:

1. A trusted, account-scoped server API that bypasses user authentication and
   permissions.
2. An authenticated virtual client that reproduces normal connection setup,
   identity, permissions, limits, and monitoring without protocol frames.

The first model is implementable with existing internals. The second model
duplicates a substantial part of the client connection path and weakens the
reason for creating a direct API.

The recommended proposal is the first model. Its name and documentation must
make the trust boundary explicit.

PR #8364 reinforces this concern. In its review, a maintainer stated that
customers asking for local Unix sockets cared primarily about authorization,
not a performance benefit, and that placement of the feature depended on the
authorization design. A direct server API bypasses more connection machinery
than a Unix socket and will receive at least the same scrutiny.

## 4. Current internal implementation

The source was inspected at nats-server main commit
[`5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367`](https://github.com/nats-io/nats-server/tree/5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367).
The relevant code is unchanged in the v2.14.3 module used by this repository.

### 4.1 In-process connection

[`Server.InProcessConn()`](https://github.com/nats-io/nats-server/blob/5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367/server/server.go#L2852-L2869)
calls `net.Pipe()` and passes the server end to the normal client creation
path.

It is not a direct message API.

### 4.2 Internal account client

[`Server.createInternalClient`](https://github.com/nats-io/nats-server/blob/5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367/server/server.go#L1895-L1922)
creates unexported internal client types for system, account, and server work.

An account lazily owns one internal account client. The client:

- participates in the account subscription index;
- can propagate subscription interest to routes, gateways, and leaf nodes;
- can publish through the normal server message matching path;
- is not a normal authenticated client;
- has no network connection;
- is not exposed through monitoring as an ordinary application connection.

This is the closest internal foundation for the requested API.

### 4.3 Internal subscriptions

[`Account.subscribeInternal`](https://github.com/nats-io/nats-server/blob/5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367/server/accounts.go#L2139-L2168)
creates a subscription on the account's internal client.

The underlying
[`client.processSubEx`](https://github.com/nats-io/nats-server/blob/5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367/server/client.go#L2987-L3077)
already supports:

- literal and wildcard subjects;
- queue group names;
- insertion into the account subject index;
- stream import shadow subscriptions;
- route and gateway interest propagation;
- leaf-node interest propagation;
- unsubscribe through the normal internal machinery.

Permission checks are applied only to normal clients. An account internal
client is trusted and bypasses client subscribe permissions.

The callback type and subscription type are unexported. They must remain
unexported. Exposing them would turn server implementation details into a
public compatibility commitment.

### 4.4 Internal publish

[`Server.sendInternalAccountMsgWithReply`](https://github.com/nats-io/nats-server/blob/5e645df4c0ce2a0b4e0c985f6a51a34cd4f53367/server/events.go#L707-L734)
queues an account-scoped message for processing by an internal client.

The internal send loop:

1. prepares publish arguments;
2. adds the NATS message terminator expected by internal code;
3. sets optional header offsets;
4. invokes the normal inbound client message path;
5. lets the existing server perform matching, mappings, imports, queue
   selection, and route forwarding.

This method accepts arbitrary internal values and can marshal them. A public
application API should accept only explicit byte payloads and must copy them
before asynchronous processing.

### 4.5 Callback safety

The internal message handler documentation states that data must be copied if
it is needed after the callback.

More importantly, account internal callbacks are invoked synchronously in the
server delivery path. The server logs a warning when an internal callback
takes too long.

Calling application code directly from that callback would be unsafe:

- a slow handler could stall a client read loop or route delivery;
- a handler panic could escape into server code;
- recursive publish and unsubscribe could create lock and ordering problems;
- application code could retain or mutate reused message storage.

An upstream API must copy the message and enqueue it to a bounded application
mailbox. It must not invoke user code from the internal callback.

## 5. Upstream acceptance assessment

The proposal is technically feasible. Upstream acceptance is uncertain.

### 5.1 Reasons maintainers may accept it

- It supports embedded applications without a client module or protocol
  parser.
- It removes `net.Pipe` setup and protocol work for trusted same-process code.
- It reuses the real server subject index, mappings, imports, queue selection,
  and routing.
- It can serve environments where application size or dependency control is
  strict.
- The server already has the required internal publish and subscribe
  machinery.
- A narrow wrapper can avoid exposing internal server types.

### 5.2 Reasons maintainers may reject it

- `InProcessConn()` plus nats.go is already the supported solution.
- It adds a second application-facing client model to the server package.
- It bypasses authentication, permissions, connection limits, and normal
  client monitoring.
- It needs separate buffering, slow-consumer, drain, and callback behavior.
- Its exported API becomes a long-term compatibility commitment.
- Direct and protocol clients may differ in subtle metrics and edge cases.
- The performance benefit may be small compared with the maintenance cost.
- Client functionality is normally owned by client repositories, not the
  server.

The issue should acknowledge these objections. It should not present the
change as a small export of existing methods.

## 6. Recommended API scope

The first proposal should be intentionally narrow:

- Core NATS only.
- Same-process use only.
- Trusted account-scoped access.
- Publish, subscribe, queue subscribe, and unsubscribe.
- Subject, optional reply subject, and byte payload.
- Bounded subscription mailboxes.
- Explicit close behavior.
- No protocol parsing.
- No new external dependency.

Request/reply can be composed from an inbox subscription and publish. A helper
does not need to be in the first pull request.

Headers can be deferred unless maintainers require semantic parity in the
first version. A header API needs a stable representation and conversion
between public values and the server's internal encoded header form.

### 6.1 Proposed public shape

Names are provisional and should be decided in the issue:

```go
package server

type EmbeddedClientOptions struct {
    Account         string
    PendingMessages int
    PendingBytes    int
}

type EmbeddedMessage struct {
    Subject string
    Reply   string
    Data    []byte
}

type EmbeddedClient struct {
    // unexported
}

type EmbeddedSubscription struct {
    // unexported
}

func (s *Server) NewEmbeddedClient(
    options EmbeddedClientOptions,
) (*EmbeddedClient, error)

func (c *EmbeddedClient) Publish(
    subject string,
    data []byte,
) error

func (c *EmbeddedClient) PublishMsg(
    message *EmbeddedMessage,
) error

func (c *EmbeddedClient) Subscribe(
    subject string,
) (*EmbeddedSubscription, error)

func (c *EmbeddedClient) QueueSubscribe(
    subject string,
    queue string,
) (*EmbeddedSubscription, error)

func (s *EmbeddedSubscription) NextMsg(
    ctx context.Context,
) (*EmbeddedMessage, error)

func (s *EmbeddedSubscription) Unsubscribe() error

func (c *EmbeddedClient) Close() error
```

`NextMsg` is preferred over an application callback in the first version. It
keeps application code out of server goroutines and makes cancellation
explicit.

A read-only message channel is another possible API, but `NextMsg` provides a
clear place to return slow-consumer, unsubscribe, and server-shutdown errors.

### 6.2 Trust contract

The constructor should accept an account name and look it up through the
server. It must reject:

- an empty or unknown account;
- an account not attached to that server;
- a server that has not started;
- a server that is shutting down;
- invalid pending limits.

The API should state:

> An EmbeddedClient is trusted server-side code. It is scoped to an account but
> is not authenticated as a NATS user. User publish and subscribe permissions,
> connection limits, and connection authentication do not apply.

This is a capability API. Code that holds the `*EmbeddedClient` can publish and
subscribe anywhere in its account.

Do not silently use the system account or global account when an account name
is wrong.

### 6.3 Publish contract

`Publish` should:

1. reject an invalid or wildcard publish subject;
2. reject an invalid reply subject;
3. reject a payload larger than the server maximum;
4. copy the payload before returning;
5. enqueue through the existing account internal publish path;
6. use echo behavior that permits subscriptions on the same embedded account
   client to receive the publication;
7. return once the server accepts the message for asynchronous processing.

A successful return must not claim that any subscriber received the message.
This matches Core NATS publish behavior.

The implementation should extract or generalize the existing internal publish
helper. It should not duplicate the full inbound message routing algorithm.

### 6.4 Subscription contract

Subscribe should:

1. validate the subject and queue group;
2. create a stable internal subscription ID;
3. use the account internal client and normal `processSubEx` path;
4. allow route, gateway, and leaf interest propagation;
5. copy subject, reply, and payload in the internal callback;
6. enqueue to a bounded mailbox without blocking the server callback;
7. expose messages through `NextMsg`;
8. propagate unsubscribe through `processUnsub`.

The default mailbox limits should be finite. Zero values can select documented
defaults.

When a mailbox exceeds either limit, the recommended first behavior is:

1. atomically mark the subscription as a slow consumer;
2. remove it from the server subscription index;
3. close its mailbox;
4. make `NextMsg` return a specific slow-consumer error after already accepted
   messages are read or immediately, according to one documented rule.

Silently dropping individual messages would make message loss hard to detect.
Blocking the server delivery path is not acceptable.

### 6.5 Message ownership

The API contract should be:

- The caller may reuse or mutate a publish buffer after `Publish` returns.
- A received `EmbeddedMessage` and its `Data` belong to the receiver.
- The server does not mutate received data after returning it.
- Each subscription receives independent metadata.
- Payload sharing between subscriptions is an internal optimization only if
  the public contract still prevents cross-subscriber mutation.

The simplest correct implementation copies on publish and again when moving
data out of a server-owned callback buffer.

### 6.6 Ordering

The internal publish queue serializes embedded publications. The public
contract should promise only the ordering that can be demonstrated by tests:

- sequential publishes from one embedded client are observed in order by one
  subscription;
- concurrent publishers have no total order;
- queue group handler completion order is not defined.

### 6.7 Lifecycle

`Unsubscribe` must be idempotent.

`Close` must:

- stop accepting publishes and subscriptions;
- remove every owned subscription;
- unblock every `NextMsg`;
- stop all embedded-client goroutines;
- not stop the server itself.

Server shutdown must also unblock embedded subscriptions. This likely requires
registering embedded clients with server lifecycle state rather than relying
only on the account's shared internal client.

A drain operation can be added later. Its semantics require a clear boundary
between messages already accepted into a mailbox and messages still in the
server routing path.

## 7. Internal implementation approach

The implementation should live in the `server` package, probably in:

```text
server/embedded.go
server/embedded_test.go
```

It needs package access to unexported clients and subscriptions. A separate Go
subpackage cannot use those internals without widening the public surface or
creating an import cycle.

### 7.1 Embedded client state

Suggested internal fields:

```go
type EmbeddedClient struct {
    mu     sync.Mutex
    srv    *Server
    acc    *Account
    subs   map[uint64]*EmbeddedSubscription
    closed bool
}
```

Do not expose the account internal client or internal subscription.

An account currently owns one shared internal client. The wrapper can reuse it
for routing, while maintaining independent public lifecycle and mailbox state
per `EmbeddedClient`.

### 7.2 Embedded subscription state

Suggested internal fields:

```go
type EmbeddedSubscription struct {
    mu           sync.Mutex
    owner        *EmbeddedClient
    internal     *subscription
    messages     chan *EmbeddedMessage
    queuedBytes  int
    byteLimit    int
    terminalErr  error
    closed       bool
    done         chan struct{}
}
```

The exact synchronization should be selected after writing race tests. The
important invariants are:

- a message is either accepted into the mailbox or rejected as slow;
- byte accounting is updated exactly once;
- unsubscribe removes internal interest exactly once;
- shutdown unblocks all waiters;
- no send occurs on a closed channel;
- the internal callback never blocks.

### 7.3 Internal callback

The callback should perform only bounded work:

```text
split message
copy subject, reply, and payload
try bounded enqueue
on overflow, schedule unsubscribe
return
```

It should not:

- call user code;
- wait for a mailbox reader;
- acquire locks in an order that conflicts with account or client locks;
- publish another message;
- log every dropped message.

Panic recovery is still appropriate around the callback adapter because it is
crossing a public API boundary, even though the adapter should contain only
server-owned code.

### 7.4 Publish path

Do not export `sendInternalAccountMsgWithReply` directly. Its `any` payload,
raw internal header bytes, system-send-queue assumptions, and echo parameter
are not suitable as a public API.

Instead:

1. add a typed private helper for account-scoped byte publication;
2. keep existing internal callers on their current wrapper;
3. have the public method validate and copy;
4. enqueue through the same serialized internal send path;
5. preserve normal server matching and forwarding.

The code should explicitly test behavior when no system account event queue is
available. If the existing internal send queue cannot operate in every valid
embedded server configuration, the proposal needs a dedicated queue or a
small refactor that is independent of system events.

### 7.5 Metrics and monitoring

The issue must ask maintainers which metrics should include embedded activity.

At minimum:

- server inbound and outbound message totals should remain correct;
- embedded subscriptions should not appear as network connections;
- slow embedded subscriptions need an observable counter;
- monitoring must not dereference a missing `net.Conn`;
- account interest and routed subscription counts must remain correct.

Adding an embedded client to `/connz` would imply a normal connection identity
and may be misleading. A separate count in server or account monitoring is
cleaner if maintainers want visibility.

## 8. Questions for maintainers before implementation

The issue should request decisions on these points:

1. Is a second, protocol-free embedded API acceptable in the server package?
2. Should it be explicitly trusted and account-scoped, or must it model an
   authenticated virtual client?
3. Is `EmbeddedClient` an acceptable name?
4. Should the first version expose `NextMsg`, a channel, or callbacks?
5. Should headers be required in the first version?
6. Should queue subscriptions be included in the first version?
7. Should embedded publications and deliveries affect normal client metrics?
8. Should embedded clients appear in monitoring?
9. Is immediate unsubscribe on mailbox overflow the preferred slow-consumer
   policy?
10. May the implementation reuse the account internal client, or should it
    introduce a distinct internal client kind?
11. Must the API support account imports, mappings, routes, gateways, and leaf
    nodes from its first release?
12. Should the API be marked experimental for one release cycle?

Do not open a code pull request until maintainers accept the trust model and
public API direction.

## 9. Copy-paste issue draft

The repository's
[proposal template](https://github.com/nats-io/nats-server/blob/main/.github/ISSUE_TEMPLATE/proposal.yml)
asks for a proposed change, use case, and contribution intent.

Suggested title:

```text
Proposal: account-scoped direct API for embedded Core NATS publish and subscribe
```

Suggested body:

```markdown
### Proposed change

Add a narrow, protocol-free Go API for trusted applications that embed
`nats-server` in the same process.

The proposed API would create an account-scoped embedded client with:

- byte payload publish with optional reply subject;
- ordinary and queue subscriptions;
- bounded subscription mailboxes;
- explicit unsubscribe and close;
- no TCP, `net.Pipe`, protocol encoding or parsing in the application path, or
  nats.go dependency.

This differs from `Server.InProcessConn()` added in #2360 and
`nats.InProcessServer()` added in nats-io/nats.go#774. Those APIs avoid TCP but
still create a normal NATS client over `net.Pipe`.

The implementation would wrap existing unexported account internal-client
paths instead of exporting internal types:

- `Account.subscribeInternal` / `client.processSubEx` for interest;
- `sendInternalAccountMsgWithReply` and the normal inbound message path for
  publication and routing.

Application code would never run on a server callback. The internal callback
would copy a message into a bounded mailbox, and the public subscription would
expose `NextMsg(context.Context)`.

The proposed security contract is intentionally trusted and account-scoped.
It would not authenticate a NATS user and would not apply user publish or
subscribe permissions. The caller already owns the embedded `*Server` and
receives a capability for one named account.

Before implementing, I would like guidance on:

1. whether this API belongs in the server package;
2. whether a trusted account-scoped model is acceptable;
3. whether `NextMsg` is preferable to callbacks or a channel;
4. whether headers and queue subscriptions are required in the first version;
5. expected metrics and monitoring behavior;
6. the desired slow-consumer policy for a bounded embedded mailbox.

### Use case

We are evaluating `nats-server` as an embedded message bus for Go
applications. The application needs real Core NATS subject matching, fanout,
queue groups, mappings, and server routing, but its local components should
publish and receive through direct Go calls.

`InProcessConn()` removes TCP but still requires a client implementation and
the NATS wire protocol. The goal is to avoid that protocol boundary for trusted
same-process code while reusing the server's existing Core NATS routing
machinery.

This proposal defines only application-to-server access inside one process.
Separate embedded server processes would continue to communicate through
existing NATS routes. The proposal does not claim direct Go calls between
processes and does not add a new cross-process transport.

### Contribution

I am interested in contributing the implementation, tests, benchmarks, and API
documentation after the security model and API shape are accepted.

The first pull request would be limited to Core NATS publish, subscribe, queue
subscribe, unsubscribe, bounded buffering, and close. It would add no external
dependencies.
```

## 10. Evidence to attach to the issue

A small benchmark and behavior prototype will make the proposal easier to
evaluate. It should compare:

1. `nats.go` with `nats.InProcessServer(ns)`;
2. a temporary direct wrapper inside a nats-server development branch.

Measure:

- publish-to-receive latency;
- messages per second;
- allocations per publication;
- bytes allocated per publication;
- executable size difference when an application does not import nats.go;
- behavior with one, ten, and one hundred subscriptions;
- behavior with small and representative payload sizes.

Also include tests demonstrating:

- wildcard matching;
- queue group selection;
- account isolation;
- route forwarding;
- no server read-loop blockage from a full mailbox;
- payload ownership after the caller mutates its original byte slice.

Do not claim a performance improvement without these measurements.

## 11. Pull request plan

The nats-server
[contribution guide](https://github.com/nats-io/nats-server/blob/main/CONTRIBUTING.md)
recommends opening an issue before a source-code pull request. It requires
tests for new features, discourages new dependencies, asks contributors to
rebase on current main, and requires signed-off commits under a legal name.

### 11.1 Before coding

1. Open the proposal issue.
2. Link PR #2360, nats.go PR #774, issue #5387, and PR #5395.
3. Wait for agreement on the trust model and API shape.
4. Record maintainer decisions in the issue.
5. Revise the proposed API before creating exported names.

If maintainers prefer a discussion before an issue, use the repository's
GitHub Discussions page or NATS Slack, then link the result from the proposal.

### 11.2 First implementation pull request

Keep the first pull request small:

```text
server/embedded.go
server/embedded_test.go
```

Modify `accounts.go`, `events.go`, or lifecycle code only where a narrow
private refactor is necessary.

Implement:

- constructor validation;
- trusted account scoping;
- publish and publish-with-reply;
- ordinary and queue subscriptions;
- bounded message and byte limits;
- non-blocking internal callback adapter;
- `NextMsg`;
- idempotent unsubscribe and close;
- server-shutdown unblocking;
- package documentation for every exported type and method.

Do not include:

- request/reply convenience methods;
- codecs or typed payloads;
- automatic reconnect;
- network transport;
- client credentials;
- new account management APIs;
- persistence;
- unrelated server refactoring.

### 11.3 Required tests

Add deterministic tests for:

- invalid options, subjects, replies, and queue names;
- server not started and server shutting down;
- zero-interest publish;
- exact and wildcard fanout;
- exactly one delivery to a queue group;
- reply subject preservation;
- sequential publish order;
- payload copy ownership;
- account isolation;
- imports and mappings;
- route propagation between two servers;
- pending message and byte overflow;
- slow subscription removal without blocking publishers;
- concurrent publish and unsubscribe;
- client close and server shutdown;
- repeated unsubscribe and close;
- goroutine cleanup.

Run race tests. Do not use sleeps for synchronization.

Add benchmarks for the direct path. Keep comparison benchmarks outside normal
tests if they require nats.go only for measurement.

### 11.4 Pull request description

Open the pull request as a draft while tests and API review are in progress.
The repository pull request template points contributors back to
`CONTRIBUTING.md` and the sign-off requirement.

Suggested title:

```text
[ADDED] Account-scoped API for embedded Core NATS clients
```

The description should:

- start with `Resolves #NNNN`;
- repeat the trusted security contract;
- list included and excluded scope;
- explain why callbacks cannot run on server goroutines;
- document copy and slow-consumer behavior;
- report benchmarks without overstating them;
- list test commands and results;
- state that no external dependency was added;
- identify any differences from normal protocol clients.

### 11.5 Likely review points

Expect detailed review of:

- exported naming and long-term API stability;
- authentication and authorization bypass;
- account lifetime and reload behavior;
- server shutdown races;
- queue group propagation;
- import and mapping behavior;
- callback lock ordering;
- payload copies and allocations;
- byte-limit accounting;
- metrics consistency;
- duplicate client semantics;
- whether the feature belongs in nats-server at all.

An issue-first design discussion is the cheapest place to resolve these points.

## 12. Fallback if upstream rejects the API

If maintainers reject a public direct API, the response should be captured in
this document and linked from the issue.

The supported choices would then be:

1. use `nats.go` with `nats.InProcessServer(ns)`;
2. maintain a small protocol client such as the current experiment;
3. build a separate in-process broker;
4. maintain a private nats-server patch and accept rebasing costs.

A private patch should still use a narrow wrapper around the internal account
client. It should not export `client`, `subscription`, `msgHandler`, or the
general internal message functions.
