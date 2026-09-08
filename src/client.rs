//! The client's side of one connection to a server.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::wire::{Line, encode, read};

/// One connected client: publishes, subscribes, takes what the server sends.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    server: String,
    info: String,
    next_sid: u64,
}

impl Client {
    /// Connect to `server`, take its INFO and answer with CONNECT.
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
        client.write(&Line::Connect(format!(
            r#"{{"verbose":false,"pedantic":false,"name":"{name}","lang":"rust","version":"0.1.0"}}"#
        )))?;
        Ok(client)
    }

    /// The INFO the server opened with, as it wrote it.
    #[must_use]
    pub fn info(&self) -> &str {
        &self.info
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
            match read(&mut self.reader)? {
                Some(Line::Pong) => return Ok(()),
                Some(Line::Ping) => self.write(&Line::Pong)?,
                Some(Line::Err(message)) => return Err(protocol_error(message)),
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
            match read(&mut self.reader)? {
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
                Some(Line::Ping) => self.write(&Line::Pong)?,
                Some(Line::Err(message)) => return Err(protocol_error(message)),
                Some(_) => {}
                None => return Ok(None),
            }
        }
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
