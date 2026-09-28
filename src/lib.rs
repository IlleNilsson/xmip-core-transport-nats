#![forbid(unsafe_code)]

//! Streams that arrive as NATS messages. One PUB is one Stream, the subject
//! kept beside it.
//!
//! NATS is the cloud-native bus: a server in the middle, subjects with
//! wildcards, a text protocol a person can read, over TCP on port 4222. A
//! Receive Location connects to the server, subscribes to a subject and takes
//! what the server delivers; a Send Location connects and publishes. Either
//! may instead accept clients directly through [`Session`], which is one
//! client's worth of server — the shape a service that publishes straight to
//! Xmip needs, and no more.
//!
//! What is here is core NATS: at-most-once, no acknowledgement, which is
//! what the protocol is. Durable streams and consumers are `JetStream` and live
//! in `xmip-core-transport-nats-jetstream`, over this. TLS is the transport
//! capability's, per ADR-0033.
//!
//! The origin URI carries what the line knew: `nats://server/subject?sid=1`.

pub mod client;
pub mod session;
pub mod wire;

use std::net::TcpListener;
use std::time::Duration;

pub use client::Client;
use net::Target;
pub use session::{Event, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::pool::delivered;
use transport::socket;
use transport::{Arrived, Configured, Directions, Pool, Transport};
pub use wire::Line;
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

/// The name a Location presents in CONNECT unless told otherwise.
pub const DEFAULT_NAME: &str = "xmip";

#[derive(Clone)]
pub struct NatsTransport {
    server: String,
    subject: String,
    name: String,
    timeout: Option<Duration>,
    /// The clients a send publishes on, connected once per server and kept.
    clients: Pool<Client>,
    /// The client a receive takes from, connected and subscribed on the
    /// first receive and kept subscribed.
    subscriptions: Pool<Client>,
}

impl NatsTransport {
    /// Speak to the server at `server` about `subject`.
    #[must_use]
    pub fn new(server: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            subject: subject.into(),
            name: DEFAULT_NAME.to_string(),
            timeout: None,
            clients: Pool::new(),
            subscriptions: Pool::new(),
        }
    }

    /// The name this Location presents in CONNECT.
    #[must_use]
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Give up on a peer that stops mid-line, and stop receiving when the
    /// server has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the server as a client.
    ///
    /// # Errors
    /// Where the server could not be reached or did not speak NATS.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.server, &self.name, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.server)
    }

    /// Accept one client on an already-bound listener.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the handshake failed.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.timeout)
    }

    /// Where a target names the server and subject itself —
    /// `nats://host:4222/orders.new` — or is a subject alone on this
    /// transport's server.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match Target::under(&["nats"], target).map(|named| (named.authority(), named.path())) {
            Some((peer, "")) => (peer, &self.subject),
            Some(pair) => pair,
            None => (&self.server, target),
        }
    }
}

impl Transport for NatsTransport {
    fn name(&self) -> &'static str {
        "nats"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Take what the server delivers until it is quiet for the timeout, or
    /// closes, on the subscription the first receive made and kept: what
    /// the server delivered between two receives waits in the socket.
    fn receive(&self) -> Result<Vec<Arrived>> {
        self.subscriptions.exchange(
            self.server.as_str(),
            || {
                let mut client = self.connect()?;
                client.subscribe(&self.subject)?;
                Ok(client)
            },
            |client| delivered(client, Client::next_message),
        )
    }

    /// Publish on the client kept for the server, connected on the first
    /// send to it, and flush: the PONG says the server has the message.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, subject) = self.resolve(target);
        self.clients.exchange(
            server,
            || Client::connect(server, &self.name, self.timeout),
            |client| {
                client.publish(subject, bytes)?;
                client.flush()
            },
        )
    }
}

impl Configured for NatsTransport {
    /// The address is the server's host and port: where a Location connects.
    const SETTINGS: &'static Settings = &Settings {
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
            Setting {
                name: "name",
                kind: Kind::Text,
                presence: Presence::Default(Fixed::Text(DEFAULT_NAME)),
                meaning: "The name a Location presents to the server in CONNECT.",
                applies: Applies::Both,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a peer that stops mid-line is waited on, and how long a \
                          quiet server ends a receive; unbounded when left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let transport = Self::new(address, settings.text("subject")).named(settings.text("name"));
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl NatsTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one subject called `probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for NatsTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener)?;
        let arrived = session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client closed without publishing"))?;
        // The client flushes with PING after PUB and waits for the PONG.
        session.answer_flush()?;
        Ok(arrived)
    }
}

impl Loopback for NatsTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A fresh client to `address`, publishing on this transport's subject
    /// and flushed before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self {
            server: address.to_string(),
            ..self.clone()
        }
        .send(&format!("nats://{address}/{}", self.subject), payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::{edge_payloads, sized_payloads};

    #[test]
    fn nats_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(NatsTransport::SETTINGS.problems(), Vec::<String>::new());
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let given = [text("subject", "orders.*"), text("timeout", "2s")];
        let built = NatsTransport::open("bus:4222", Applies::Receive, &given).expect("built");
        assert_eq!(built.server, "bus:4222");
        assert_eq!(built.subject, "orders.*");
        assert_eq!(built.name, DEFAULT_NAME);
        assert_eq!(built.timeout, Some(secs(2)));
        let Err(refused) = NatsTransport::open("bus:4222", Applies::Send, &given[1..]) else {
            panic!("subject is required");
        };
        assert!(
            refused.message.contains("\"subject\""),
            "{}",
            refused.message
        );
    }

    #[test]
    fn a_client_publishes_to_a_session() {
        let far_end = NatsTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            let near = NatsTransport::new(address.clone(), "probe")
                .named("probe")
                .timing_out_after(secs(2));
            near.send("orders.new", b"order 1\r\nline 2")?;
            near.send(&format!("nats://{address}/orders.cancel"), b"")
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert!(session.connect().contains(r#""name":"probe""#));
        let first = session.next_publish().expect("first").expect("one");
        assert_eq!(first.bytes, b"order 1\r\nline 2");
        assert!(first.origin_uri.ends_with("/orders.new"));
        // The same server, so the same client: connected once.
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/orders.cancel"));
        assert!(second.bytes.is_empty());
        assert!(session.next_publish().expect("closed").is_none());
        sender.join().expect("thread").expect("sending");
    }

    #[test]
    fn a_thousand_publishes_connect_once_and_a_client_the_server_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = NatsTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = NatsTransport::new(address, "probe").timing_out_after(secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("orders.new", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a publish.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("orders.new", b"after the close")
        });
        // One CONNECT for every publish: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for n in 0..SENDS {
            let arrived = session.next_publish().expect("publish").expect("one");
            assert_eq!(arrived.bytes, n.to_string().as_bytes());
            session.answer_flush().expect("flushed");
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new client");
        let last = again.next_publish().expect("publish").expect("one");
        assert_eq!(last.bytes, b"after the close");
        again.answer_flush().expect("flushed");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.clients.opened(), 2);
    }

    #[test]
    fn a_session_delivers_to_a_subscribed_client() {
        let far_end = NatsTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            NatsTransport::new(address, "orders.*")
                .timing_out_after(secs(2))
                .receive()
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(
            session.next_event().expect("subscribed"),
            Some(Event::Subscribed {
                subject: "orders.*".to_string(),
                sid: "1".to_string()
            })
        );
        session.deliver("orders.new", b"first").expect("first");
        session.deliver("orders.cancel", b"second").expect("second");
        drop(session);
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"first");
        assert!(arrived[1].origin_uri.ends_with("/orders.cancel?sid=1"));
    }

    #[test]
    fn five_receives_subscribe_once_and_a_subscription_the_server_closed_is_replaced() {
        let far_end = NatsTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = NatsTransport::new(address, "orders.*").timing_out_after(millis(100));
        let receiving = near.clone();
        let (taken, told) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut arrived = Vec::new();
            while arrived.len() < 6 {
                let now = receiving.receive()?;
                if !now.is_empty() {
                    taken.send(()).expect("told");
                }
                arrived.extend(now.into_iter().map(|one| one.bytes));
            }
            Ok::<_, transport::TransportError>(arrived)
        });
        let subscribed = |session: &mut Session| {
            let event = session.next_event().expect("subscribed");
            assert!(matches!(event, Some(Event::Subscribed { .. })), "{event:?}");
        };
        // One SUB for every receive: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        subscribed(&mut session);
        for round in 0..5u8 {
            session.deliver("orders.new", &[round]).expect("delivered");
            told.recv().expect("taken");
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new client");
        subscribed(&mut again);
        again.deliver("orders.new", &[5]).expect("delivered");
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived, (0..6u8).map(|n| vec![n]).collect::<Vec<_>>());
        assert_eq!(near.subscriptions.opened(), 2);
    }

    #[test]
    fn a_server_that_does_not_speak_nats_is_a_permanent_error() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            std::io::Write::write_all(&mut stream, b"220 mail.example ESMTP\r\n").expect("write");
        });
        let error = NatsTransport::new(address, "t")
            .timing_out_after(secs(2))
            .connect()
            .err()
            .expect("refused");
        assert!(!error.retryable);
        assert!(NatsTransport::new("127.0.0.1:0", "t").claims().is_none());
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = NatsTransport::loopback();
        let arrived = loopback.round(b"pub\r\nlished").expect("round");
        assert_eq!(arrived.bytes, b"pub\r\nlished");
        assert!(arrived.origin_uri.starts_with("nats://127.0.0.1:"));
        assert!(arrived.origin_uri.ends_with("/probe"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = NatsTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn millis(n: u64) -> Duration {
        Duration::from_millis(n)
    }
}
