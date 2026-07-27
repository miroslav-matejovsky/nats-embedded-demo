# NATS-embedded demo

This repository demonstrates message passing through an embedded NATS server
using the official `nats.go` client without any client network connection.
The client connects to the embedded server entirely over in-memory pipes, so
no client TCP connection exists.

The repository contains two approaches with two local-only applications each.
They differ only in how the client obtains its in-process transport; see
[docs/tutorial.md](docs/tutorial.md) for a pros and cons comparison.

The in-process approach sets the `nats.Options` field `InProcessServer`, which
maps directly to `Server.InProcessConn()`:

- `cmd/inprocess-nats`: core NATS publish and subscribe.
- `cmd/inprocess-jetstream`: JetStream stream creation, durable pull
  consumption, publishing, and explicit acknowledgement.

The custom dialer approach reaches the same in-process connection through
`nats.SetCustomDialer`, following
<https://docs.nats.io/using-nats/developer/tutorials/custom_dialer>:

- `cmd/dialer-nats`: core NATS publish and subscribe.
- `cmd/dialer-jetstream`: the same JetStream round trip.

Each process embeds `github.com/nats-io/nats-server/v2/server`. In both
approaches the client transport is a `net.Pipe` from `Server.InProcessConn()`,
so no client TCP connection exists. There is no client port to configure:
a standalone server runs with `DontListen` and opens no client listener at
all. Multiple processes exchange messages through NATS routes bound to
loopback addresses, configured with `--route-port` and `--routes`.

## Quick start

Run a standalone core NATS round trip:

```powershell
go run ./cmd/inprocess-nats
go run ./cmd/dialer-nats
```

Run a standalone JetStream round trip:

```powershell
go run ./cmd/inprocess-jetstream `
  --store-dir=.test-results/quickstart-js
go run ./cmd/dialer-jetstream `
  --store-dir=.test-results/quickstart-dialer-js
```

Use double-dash application flags with `go run` so the Go command does not
consume similarly named flags.

See [docs/README.md](docs/README.md) for:

- a tutorial for the demos in this repository;
- a comparison of same-process and local multi-process messaging alternatives;
- design studies of a protocol-free, direct-call Core NATS-style broker.

## Architecture

```text
process A                                      process B
+-------------------------------+              +-------------------------------+
| demo application              |              | demo application              |
|   nats.go client              |              |   nats.go client              |
|          |                    |              |          |                    |
|   Server.InProcessConn()      |              |   Server.InProcessConn()      |
|          | net.Pipe           |              |          | net.Pipe           |
|   embedded NATS server        |<------------>|   embedded NATS server        |
|                               | local route  |                               |
+-------------------------------+     TCP      +-------------------------------+
```

Shared code is organized as follows:

- `pkg/embeddedserver`: reusable local server configuration and startup.
- `pkg/natsinprocess`: `nats.go` connection through the `InProcessServer`
  option.
- `pkg/natsdialer`: `nats.go` connection through a custom in-process dialer.
- `internal/demo`: shared command behavior types.

## Verification

```powershell
task all
```
