# xmip-core-transport-nats

NATS transport: one PUB is one Stream, the subject beside it; a Location subscribes or publishes through a server, or accepts clients directly. Core NATS, at-most-once. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location publishes on a client connected once per server and kept (`transport::Pool`), and flushes: the PONG says the server has the message. `Client` is the one NATS client in the estate, the CONNECT written as JSON by `serde_json`; `nats-jetstream` speaks its API over it. Until 2026-09-27 every publish connected.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
