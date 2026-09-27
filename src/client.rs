//! The client's side of one connection to a server: the one NATS client in
//! the estate. `JetStream` (`xmip-core-transport-nats-jetstream`) speaks its
//! API over this one, with the lines it writes and reads.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::pool::{Pooled, alive};
use transport::{Arrived, socket};

use crate::wire::{Line, encode, read};

/// One connected client: publishes, subscribes, takes what the server
/// sends. Kept between sends while the server keeps it open.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    server: String,
    info: String,
    next_sid: u64,
}

impl Client {
    /// Connect to `server`, take its INFO and answer with CONNECT, presenting
    /// `name`.
    ///
    /// # Errors
    /// Where the server could not be reached or did not open with INFO.
    pub fn connect(server: &str, name: &str, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(server, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            server: server.to_string(),
            info: String::new(),
            next_sid: 0,
        };
        match read(&mut client.reader)? {
            Some(Line::Info(info)) => client.info = info,
            _ => return Err(protocol_error("the server did not open with INFO")),
        }
        let connect = serde_json::json!({
            "verbose": false,
            "pedantic": false,
            "headers": false,
            "name": name,
            "lang": "rust",
            "version": "0.1.0",
        });
        client.write(&Line::Connect(connect.to_string()))?;
        Ok(client)
    }

    /// The INFO the server opened with, as it wrote it.
    #[must_use]
    pub fn info(&self) -> &str {
        &self.info
    }

    /// The server this client is connected to, as it was named.
    #[must_use]
    pub fn server(&self) -> &str {
        &self.server
    }

    /// How long a read waits for the server; `None` for as long as it takes.
    #[must_use]
    pub fn read_timeout(&self) -> Option<Duration> {
        self.reader.get_ref().read_timeout().ok().flatten()
    }

    /// Publish `payload` on `subject`. Fire and forget, as NATS is.
    ///
    /// # Errors
    /// Where the server went away.
    pub fn publish(&mut self, subject: &str, payload: &[u8]) -> Result<()> {
        self.write(&Line::Pub {
            subject: subject.to_string(),
            reply: None,
            payload: payload.to_vec(),
        })
    }

    /// Subscribe to `subject`; the sid that names the subscription.
    ///
    /// # Errors
    /// Where the server went away.
    pub fn subscribe(&mut self, subject: &str) -> Result<String> {
        self.next_sid += 1;
        let sid = self.next_sid.to_string();
        self.write(&Line::Sub {
            subject: subject.to_string(),
            queue: None,
            sid: sid.clone(),
        })?;
        Ok(sid)
    }

    /// Wait for the server to catch up: PING, and the PONG that says so.
    ///
    /// # Errors
    /// Where the server went away or answered with an error.
    pub fn flush(&mut self) -> Result<()> {
        self.write(&Line::Ping)?;
        loop {
            match self.next_line()? {
                Some(Line::Pong) => return Ok(()),
                Some(_) => {}
                None => return Err(protocol_error("the server closed before PONG")),
            }
        }
    }

    /// The next message the server delivers, or `None` when it closed.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or the
    /// server reported an error.
    pub fn next_message(&mut self) -> Result<Option<Arrived>> {
        loop {
            match self.next_line()? {
                Some(Line::Msg {
                    subject,
                    sid,
                    payload,
                    ..
                }) => {
                    return Ok(Some(Arrived::new(
                        format!("nats://{}/{subject}?sid={sid}", self.server),
                        payload,
                    )));
                }
                Some(_) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next line the server sends, or `None` when it closed: its pings
    /// answered on the way, and its `-ERR` raised.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or the
    /// server reported an error.
    pub fn next_line(&mut self) -> Result<Option<Line>> {
        loop {
            match read(&mut self.reader)? {
                Some(Line::Ping) => self.write(&Line::Pong)?,
                Some(Line::Err(message)) => return Err(protocol_error(message)),
                other => return Ok(other),
            }
        }
    }

    /// Write `line` to the server and flush it.
    ///
    /// # Errors
    /// Where the server went away.
    pub fn write(&mut self, line: &Line) -> Result<()> {
        self.writer
            .write_all(&encode(line))
            .map_err(|e| classify("writing a protocol line", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a protocol line", &e))
    }
}

impl Pooled for Client {
    /// While the server has not closed the connection.
    fn usable(&mut self) -> bool {
        alive(&self.writer)
    }
}
