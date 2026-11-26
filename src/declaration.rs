//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48). The name a Location presents in CONNECT is [`NAME`], which
//! `nats-jetstream` declares from too, and its default, [`DEFAULT_NAME`],
//! is what a client presents unless told otherwise.

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

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["nats"];
