//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`. The name a
//! Location presents in CONNECT is [`NAME`], which `nats-jetstream` declares
//! from too, and its default, [`DEFAULT_NAME`], is what a client presents
//! unless told otherwise.

use xcore::settings::{Applies, Fixed, Kind, Presence, Setting, Settings};

/// The name a Location presents in CONNECT unless told otherwise.
pub const DEFAULT_NAME: &str = "xmip";

/// The name a Location presents to the server in CONNECT: one setting for
/// every technology that speaks NATS.
pub const NAME: Setting = Setting {
    name: "name",
    kind: Kind::Text,
    presence: Presence::Default(Fixed::Text(DEFAULT_NAME)),
    meaning: "The name a Location presents to the server in CONNECT.",
    applies: Applies::Both,
};

/// Every setting a Location takes beyond its address, the server's host
/// and port: where a Location connects.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "subject",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The subject a Receive Location subscribes to, and the one a Send \
                      Location publishes on when its target names none.",
            applies: Applies::Both,
        },
        NAME,
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a peer that stops mid-line is waited on, and how long a \
             quiet server ends a receive; unbounded when left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["nats"];
