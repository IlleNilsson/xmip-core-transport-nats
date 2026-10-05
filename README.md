# xmip-core-transport-nats

NATS transport: one PUB is one Stream, the subject beside it; a Location subscribes or publishes through a server, or accepts clients directly. Core NATS, at-most-once. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location publishes on a client connected once per server and kept (`transport::Pool`), and flushes: the PONG says the server has the message. `Client` is the one NATS client in the estate, the CONNECT written as JSON by `serde_json`; `nats-jetstream` speaks its API over it. Until 2026-09-27 every publish connected.

A Receive Location subscribes once: its first receive connects and subscribes, and the subscription stays attached between receives, so what the server delivers meanwhile waits in the socket for the next; each receive takes what came until the server is quiet for the timeout (`transport::pool::delivered`). A subscription the server closed is replaced. Until 2026-09-28 every receive connected and subscribed anew, and nothing delivered between two receives reached either.

**Acceptance is at-most-once here.** Core NATS has no acknowledgement: the server sends a message once to whoever is subscribed and keeps nothing to send again, so a message is gone from the server as it arrives, and a crash before the runtime has it durably loses it. Every arrival says so (`Acknowledgement::at_most_once`, `client::AT_MOST_ONCE`) and nothing waits for the receive cycle's verdict. A Location that needs at-least-once uses `nats-jetstream`.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## The deduplication key

Core NATS has no identifier a server deduplicates by, and a send here carries no key (`Transport::send_keyed` is the default). What `nats-jetstream` needs for its `Nats-Msg-Id` is here since 2026-10-04: `wire::Line::HPub`, a PUB with headers, written and read, and `Client::connect_with_headers`, which says `headers` in CONNECT; `Client::connect` does not, so a subscriber is delivered messages without them.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
