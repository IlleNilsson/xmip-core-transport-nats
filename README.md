# xmip-core-transport-nats

NATS transport: one PUB is one Stream, the subject beside it; a Location subscribes or publishes through a server, or accepts clients directly. Core NATS, at-most-once. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
