//! The NATS protocol on the wire: one line, CRLF-terminated, and a payload
//! after PUB and MSG whose length the line already said.
//!
//! Text, deliberately: the protocol is meant to be read by a person with
//! `telnet`, and this file keeps it that way. The JSON that INFO and CONNECT
//! carry is passed through as text; nothing here reads it.

use std::io::BufRead;

use transport::error::{Result, classify, protocol_error};

/// One protocol line, either direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Line {
    /// Server to client, first thing.
    Info(String),
    /// Client to server, in answer to INFO.
    Connect(String),
    Pub {
        subject: String,
        reply: Option<String>,
        payload: Vec<u8>,
    },
    Sub {
        subject: String,
        queue: Option<String>,
        sid: String,
    },
    Unsub {
        sid: String,
    },
    Msg {
        subject: String,
        sid: String,
        reply: Option<String>,
        payload: Vec<u8>,
    },
    Ping,
    Pong,
    Ok,
    Err(String),
}

/// `line` as bytes on the wire.
#[must_use]
pub fn encode(line: &Line) -> Vec<u8> {
    let mut out = Vec::new();
    match line {
        Line::Info(json) => out.extend_from_slice(format!("INFO {json}\r\n").as_bytes()),
        Line::Connect(json) => out.extend_from_slice(format!("CONNECT {json}\r\n").as_bytes()),
        Line::Pub {
            subject,
            reply,
            payload,
        } => {
            let head = match reply {
                Some(reply) => format!("PUB {subject} {reply} {}\r\n", payload.len()),
                None => format!("PUB {subject} {}\r\n", payload.len()),
            };
            out.extend_from_slice(head.as_bytes());
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
        }
        Line::Sub {
            subject,
            queue,
            sid,
        } => {
            let head = match queue {
                Some(queue) => format!("SUB {subject} {queue} {sid}\r\n"),
                None => format!("SUB {subject} {sid}\r\n"),
            };
            out.extend_from_slice(head.as_bytes());
        }
        Line::Unsub { sid } => out.extend_from_slice(format!("UNSUB {sid}\r\n").as_bytes()),
        Line::Msg {
            subject,
            sid,
            reply,
            payload,
        } => {
            let head = match reply {
                Some(reply) => format!("MSG {subject} {sid} {reply} {}\r\n", payload.len()),
                None => format!("MSG {subject} {sid} {}\r\n", payload.len()),
            };
            out.extend_from_slice(head.as_bytes());
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
        }
        Line::Ping => out.extend_from_slice(b"PING\r\n"),
        Line::Pong => out.extend_from_slice(b"PONG\r\n"),
        Line::Ok => out.extend_from_slice(b"+OK\r\n"),
        Line::Err(message) => out.extend_from_slice(format!("-ERR '{message}'\r\n").as_bytes()),
    }
    out
}

/// Read one line and its payload, or `None` when the peer closed between
/// lines.
///
/// # Errors
/// A connection that closes mid-line, a line that is not NATS, or a payload
/// shorter than its length.
pub fn read(reader: &mut impl BufRead) -> Result<Option<Line>> {
    let mut raw = Vec::new();
    let read = reader
        .read_until(b'\n', &mut raw)
        .map_err(|e| classify("reading a protocol line", &e))?;
    if read == 0 {
        return Ok(None);
    }
    if !raw.ends_with(b"\r\n") {
        return Err(protocol_error("a protocol line without its CRLF"));
    }
    raw.truncate(raw.len() - 2);
    let text = String::from_utf8(raw).map_err(|_| protocol_error("a line that is not UTF-8"))?;
    let (verb, rest) = text.split_once(' ').unwrap_or((text.as_str(), ""));
    let line = match verb.to_ascii_uppercase().as_str() {
        "INFO" => Line::Info(rest.trim().to_string()),
        "CONNECT" => Line::Connect(rest.trim().to_string()),
        "PUB" => {
            let words: Vec<&str> = rest.split_whitespace().collect();
            let (subject, reply, length) = match words.as_slice() {
                [subject, length] => (*subject, None, *length),
                [subject, reply, length] => (*subject, Some((*reply).to_string()), *length),
                _ => return Err(protocol_error("a PUB that is not subject [reply] length")),
            };
            Line::Pub {
                subject: subject.to_string(),
                reply,
                payload: payload(reader, length)?,
            }
        }
        "SUB" => {
            let words: Vec<&str> = rest.split_whitespace().collect();
            let (subject, queue, sid) = match words.as_slice() {
                [subject, sid] => (*subject, None, *sid),
                [subject, queue, sid] => (*subject, Some((*queue).to_string()), *sid),
                _ => return Err(protocol_error("a SUB that is not subject [queue] sid")),
            };
            Line::Sub {
                subject: subject.to_string(),
                queue,
                sid: sid.to_string(),
            }
        }
        "UNSUB" => Line::Unsub {
            sid: rest
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string(),
        },
        "MSG" => {
            let words: Vec<&str> = rest.split_whitespace().collect();
            let (subject, sid, reply, length) = match words.as_slice() {
                [subject, sid, length] => (*subject, *sid, None, *length),
                [subject, sid, reply, length] => {
                    (*subject, *sid, Some((*reply).to_string()), *length)
                }
                _ => {
                    return Err(protocol_error(
                        "a MSG that is not subject sid [reply] length",
                    ));
                }
            };
            Line::Msg {
                subject: subject.to_string(),
                sid: sid.to_string(),
                reply,
                payload: payload(reader, length)?,
            }
        }
        "PING" => Line::Ping,
        "PONG" => Line::Pong,
        "+OK" => Line::Ok,
        "-ERR" => Line::Err(rest.trim().trim_matches('\'').to_string()),
        other => return Err(protocol_error(format!("{other:?} is not a NATS verb"))),
    };
    Ok(Some(line))
}

fn payload(reader: &mut impl BufRead, length: &str) -> Result<Vec<u8>> {
    let length: usize = length
        .parse()
        .map_err(|_| protocol_error(format!("{length:?} is not a payload length")))?;
    let mut bytes = vec![0u8; length + 2];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| classify("reading a payload", &e))?;
    if !bytes.ends_with(b"\r\n") {
        return Err(protocol_error("a payload not followed by CRLF"));
    }
    bytes.truncate(length);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(line: &Line) {
        let bytes = encode(line);
        let back = read(&mut bytes.as_slice()).expect("read").expect("one");
        assert_eq!(&back, line);
    }

    #[test]
    fn every_line_round_trips() {
        round_trip(&Line::Info(r#"{"server_id":"x"}"#.into()));
        round_trip(&Line::Connect(r#"{"verbose":false}"#.into()));
        round_trip(&Line::Pub {
            subject: "orders.new".into(),
            reply: None,
            payload: b"hello\r\nworld".to_vec(),
        });
        round_trip(&Line::Pub {
            subject: "orders.new".into(),
            reply: Some("_INBOX.1".into()),
            payload: Vec::new(),
        });
        round_trip(&Line::Sub {
            subject: "orders.*".into(),
            queue: Some("workers".into()),
            sid: "1".into(),
        });
        round_trip(&Line::Sub {
            subject: ">".into(),
            queue: None,
            sid: "2".into(),
        });
        round_trip(&Line::Unsub { sid: "2".into() });
        round_trip(&Line::Msg {
            subject: "orders.new".into(),
            sid: "1".into(),
            reply: Some("_INBOX.1".into()),
            payload: vec![0, 1, 255],
        });
        round_trip(&Line::Msg {
            subject: "orders.new".into(),
            sid: "1".into(),
            reply: None,
            payload: b"x".to_vec(),
        });
        round_trip(&Line::Ping);
        round_trip(&Line::Pong);
        round_trip(&Line::Ok);
        round_trip(&Line::Err("Unknown Protocol Operation".into()));
    }

    #[test]
    fn what_is_not_nats_is_refused() {
        assert!(read(&mut &b""[..]).expect("closed").is_none());
        assert!(read(&mut &b"PING\n"[..]).is_err(), "bare LF");
        assert!(read(&mut &b"PUB a 5\r\nhel\r\n"[..]).is_err(), "short");
        assert!(read(&mut &b"PUB a x\r\n"[..]).is_err(), "length");
        assert!(read(&mut &b"PUB a\r\n"[..]).is_err(), "no length");
        assert!(read(&mut &b"HELO\r\n"[..]).is_err(), "verb");
        assert!(read(&mut &b"PUB a 2\r\nabXX"[..]).is_err(), "no CRLF");
        let lower = read(&mut &b"ping\r\n"[..]).expect("read");
        assert_eq!(lower, Some(Line::Ping), "verbs are case-insensitive");
    }
}
