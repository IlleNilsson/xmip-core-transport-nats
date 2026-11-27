# xmip-core-transport-nats

NATS transport: one PUB is one Stream, the subject beside it; a Location subscribes or publishes through a server, or accepts clients directly. Core NATS, at-most-once. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location publishes on a client connected once per server and kept (`transport::Pool`), and flushes: the PONG says the server has the message. `Client` is the one NATS client in the estate, the CONNECT written as JSON by `serde_json`; `nats-jetstream` speaks its API over it. Until 2026-09-27 every publish connected.

A Receive Location subscribes once: its first receive connects and subscribes, and the subscription stays attached between receives, so what the server delivers meanwhile waits in the socket for the next; each receive takes what came until the server is quiet for the timeout (`transport::pool::delivered`). A subscription the server closed is replaced. Until 2026-09-28 every receive connected and subscribed anew, and nothing delivered between two receives reached either.

**Acceptance is at-most-once here.** Core NATS has no acknowledgement: the server sends a message once to whoever is subscribed and keeps nothing to send again, so a message is gone from the server as it arrives, and a crash before the runtime has it durably loses it. Every arrival says so (`Acknowledgement::at_most_once`, `client::AT_MOST_ONCE`) and nothing waits for the receive cycle's verdict. A Location that needs at-least-once uses `nats-jetstream`.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

A Send Location publishes to the subject its `subject` setting names, on the server its address names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which `transport::destination::on_server` reads as that subject. A target naming another still wins — `orders.cancel`, or `nats://host:4222/orders.cancel` (ADR-0064, amendment 2026-11-10).

## The deduplication key

Core NATS has no identifier a server deduplicates by, and a send here carries no key (the key `Transport::send_from` is handed, `Outgoing::key`, goes nowhere). What `nats-jetstream` needs for its `Nats-Msg-Id` is here since 2026-10-04: `wire::Line::HPub`, a PUB with headers, written and read, and `Client::connect_with_headers`, which says `headers` in CONNECT; `Client::connect` does not, so a subscriber is delivered messages without them.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `subject`, `name` and `timeout`, and `SCHEMES`, `nats`. `name` is `declaration::NAME`, which `nats-jetstream` declares from too, and its default, `declaration::DEFAULT_NAME`, is what a client presents unless told otherwise. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — `serde_json` among them — are behind the default feature. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check a NATS Location's settings though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
