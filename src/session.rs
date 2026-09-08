//! The server's side of one connection: what a Receive Location that accepts
//! clients directly runs, and what a test puts at the far end.
//!
//! Not a server. One session serves one client and keeps no subject tree;
//! what it takes is handed up as Streams and what it is given is delivered
//! to its one client under the sid the client chose. A Location that needs
//! fan-out or `JetStream` talks to a server through [`crate::Client`].

use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::wire::{Line, encode, read};

/// What a client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published; here is the Stream.
    Published(Arrived),
    /// The client subscribed to `subject` under `sid`.
    Subscribed { subject: String, sid: String },
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    connect: String,
    subscribed: Vec<(String, String)>,
}

impl Session {
    /// Accept one client on `listener`, send INFO and take its CONNECT.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the client did not
    /// answer INFO with CONNECT.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut session = Self {
            reader,
            writer,
            peer,
            connect: String::new(),
            subscribed: Vec::new(),
        };
        session.write(&Line::Info(
            r#"{"server_id":"xmip","version":"0.1.0","headers":false,"max_payload":1048576}"#
                .to_string(),
        ))?;
        match read(&mut session.reader)? {
            Some(Line::Connect(connect)) => session.connect = connect,
            _ => {
                return Err(protocol_error(
                    "the client did not answer INFO with CONNECT",
                ));
            }
        }
        Ok(session)
    }

    /// The CONNECT the client sent, as it wrote it.
    #[must_use]
    pub fn connect(&self) -> &str {
        &self.connect
    }

    /// The subscriptions taken so far, subject and sid.
    #[must_use]
    pub fn subscribed(&self) -> &[(String, String)] {
        &self.subscribed
    }

    /// The next message the client publishes, or `None` when it closed.
    /// Subscriptions are taken on the way and pings answered.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Arrived>> {
        loop {
            match self.next_event()? {
                Some(Event::Published(arrived)) => return Ok(Some(arrived)),
                Some(Event::Subscribed { .. }) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next thing the client did, or `None` when it closed.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or the
    /// client sent what only a server sends.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            match read(&mut self.reader)? {
                Some(Line::Pub {
                    subject, payload, ..
                }) => {
                    let origin = format!("nats://{}/{subject}", self.peer);
                    return Ok(Some(Event::Published(Arrived::new(origin, payload))));
                }
                Some(Line::Sub { subject, sid, .. }) => {
                    self.subscribed.push((subject.clone(), sid.clone()));
                    return Ok(Some(Event::Subscribed { subject, sid }));
                }
                Some(Line::Unsub { sid }) => self.subscribed.retain(|(_, s)| *s != sid),
                Some(Line::Ping) => self.write(&Line::Pong)?,
                Some(Line::Pong | Line::Connect(_)) => {}
                None => return Ok(None),
                Some(other) => {
                    self.write(&Line::Err("Unknown Protocol Operation".to_string()))?;
                    return Err(protocol_error(format!("{other:?} from a client")));
                }
            }
        }
    }

    /// Deliver `payload` on `subject` to the client, under the sid it
    /// subscribed with — or the first subscription's, or `1`.
    ///
    /// # Errors
    /// Where the client went away.
    pub fn deliver(&mut self, subject: &str, payload: &[u8]) -> Result<()> {
        let sid = self
            .subscribed
            .iter()
            .find(|(s, _)| s == subject)
            .or_else(|| self.subscribed.first())
            .map_or_else(|| "1".to_string(), |(_, sid)| sid.clone());
        self.write(&Line::Msg {
            subject: subject.to_string(),
            sid,
            reply: None,
            payload: payload.to_vec(),
        })
    }

    fn write(&mut self, line: &Line) -> Result<()> {
        self.writer
            .write_all(&encode(line))
            .map_err(|e| classify("writing a protocol line", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a protocol line", &e))
    }
}
