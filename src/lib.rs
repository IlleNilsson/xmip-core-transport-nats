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
pub use session::{Event, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};
pub use wire::Line;

#[derive(Clone)]
pub struct NatsTransport {
    server: String,
    subject: String,
    name: String,
    timeout: Option<Duration>,
}

impl NatsTransport {
    /// Speak to the server at `server` about `subject`.
    #[must_use]
    pub fn new(server: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            subject: subject.into(),
            name: "xmip".to_string(),
            timeout: None,
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
        match socket::target("nats", target) {
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

    /// Subscribe and take what the server delivers until it is quiet for the
    /// timeout, or closes.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        client.subscribe(&self.subject)?;
        let mut arrived = Vec::new();
        loop {
            match client.next_message() {
                Ok(Some(message)) => arrived.push(message),
                Ok(None) => break,
                Err(error) if error.retryable && !arrived.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, subject) = self.resolve(target);
        let mut client = Client::connect(server, &self.name, self.timeout)?;
        client.publish(subject, bytes)?;
        client.flush()
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
        // The client flushes with PING after PUB and waits for the PONG;
        // serve it, and see the client close.
        session.next_publish()?;
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
        assert!(session.next_publish().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener).expect("second");
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/orders.cancel"));
        assert!(second.bytes.is_empty());
        assert!(session.next_publish().expect("closed").is_none());
        sender.join().expect("thread").expect("sending");
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
}
