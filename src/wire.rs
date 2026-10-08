//! The NATS protocol on the wire: one line, written CRLF-terminated and read
//! to its CRLF or bare LF as the server reads it, and a payload after PUB and
//! MSG whose length the line already said.
//!
//! Text, deliberately: the protocol is meant to be read by a person with
//! `telnet`, and this file keeps it that way. The JSON that INFO and CONNECT
//! carry is passed through as text; nothing here reads it.
//!
//! HPUB, a PUB with headers (NATS 2.2), is written and read since
//! 2026-10-04, for the one header Xmip sends: `JetStream`'s `Nats-Msg-Id`.
//! Its header block is `NATS/1.0`, then `Name: value` lines, then a blank
//! line, and its two lengths are the block's and the block's and payload's.

use std::io::BufRead;

use net::ceiling;
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
    /// A PUB with headers, each a name and its value, in order.
    HPub {
        subject: String,
        reply: Option<String>,
        headers: Vec<(String, String)>,
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
    /// A MSG with headers (NATS 2.2): what a subscriber is delivered of a
    /// message published with them, each a name and its value, in order.
    HMsg {
        subject: String,
        sid: String,
        reply: Option<String>,
        headers: Vec<(String, String)>,
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
        Line::HPub {
            subject,
            reply,
            headers,
            payload,
        } => {
            let block = header_block(headers);
            let total = block.len() + payload.len();
            let head = match reply {
                Some(reply) => format!("HPUB {subject} {reply} {} {total}\r\n", block.len()),
                None => format!("HPUB {subject} {} {total}\r\n", block.len()),
            };
            out.extend_from_slice(head.as_bytes());
            out.extend_from_slice(&block);
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
        Line::HMsg {
            subject,
            sid,
            reply,
            headers,
            payload,
        } => {
            let block = header_block(headers);
            let total = block.len() + payload.len();
            let head = match reply {
                Some(reply) => format!("HMSG {subject} {sid} {reply} {} {total}\r\n", block.len()),
                None => format!("HMSG {subject} {sid} {} {total}\r\n", block.len()),
            };
            out.extend_from_slice(head.as_bytes());
            out.extend_from_slice(&block);
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
/// A connection that closes mid-line, a line over `net::read::MAX_LINE` or
/// not UTF-8, a line that is not NATS, a length over `net::MAX_BODY`, or a
/// payload shorter than its length.
pub fn read(reader: &mut impl BufRead) -> Result<Option<Line>> {
    let Some(text) = net::read::line(reader)? else {
        return Ok(None);
    };
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
        "HPUB" => {
            let words: Vec<&str> = rest.split_whitespace().collect();
            let (subject, reply, block, total) = match words.as_slice() {
                [subject, block, total] => (*subject, None, *block, *total),
                [subject, reply, block, total] => {
                    (*subject, Some((*reply).to_string()), *block, *total)
                }
                _ => {
                    return Err(protocol_error(
                        "an HPUB that is not subject [reply] header-length length",
                    ));
                }
            };
            let (headers, payload) = headed(reader, block, total)?;
            Line::HPub {
                subject: subject.to_string(),
                reply,
                headers,
                payload,
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
        "HMSG" => hmsg(reader, rest)?,
        "PING" => Line::Ping,
        "PONG" => Line::Pong,
        "+OK" => Line::Ok,
        "-ERR" => Line::Err(rest.trim().trim_matches('\'').to_string()),
        other => return Err(protocol_error(format!("{other:?} is not a NATS verb"))),
    };
    Ok(Some(line))
}

/// An HMSG's line after its verb, `rest`, and what follows it on `reader`.
fn hmsg(reader: &mut impl BufRead, rest: &str) -> Result<Line> {
    let words: Vec<&str> = rest.split_whitespace().collect();
    let (subject, sid, reply, block, total) = match words.as_slice() {
        [subject, sid, block, total] => (*subject, *sid, None, *block, *total),
        [subject, sid, reply, block, total] => {
            (*subject, *sid, Some((*reply).to_string()), *block, *total)
        }
        _ => {
            return Err(protocol_error(
                "an HMSG that is not subject sid [reply] header-length length",
            ));
        }
    };
    let (headers, payload) = headed(reader, block, total)?;
    Ok(Line::HMsg {
        subject: subject.to_string(),
        sid: sid.to_string(),
        reply,
        headers,
        payload,
    })
}

/// Headers, each a name and its value in order, and the payload after them.
type Headed = (Vec<(String, String)>, Vec<u8>);

/// The headers and the payload an HPUB or an HMSG carries: `total` bytes,
/// the first `block` of them the header block.
fn headed(reader: &mut impl BufRead, block: &str, total: &str) -> Result<Headed> {
    let mut payload = payload(reader, total)?;
    let block = block
        .parse::<usize>()
        .ok()
        .filter(|block| *block <= payload.len())
        .ok_or_else(|| protocol_error(format!("{block:?} is not a header length")))?;
    let headers = headers_of(&payload[..block])?;
    payload.drain(..block);
    Ok((headers, payload))
}

/// The header block HPUB carries: `NATS/1.0`, each header on a line of its
/// own, and the blank line that ends them.
fn header_block(headers: &[(String, String)]) -> Vec<u8> {
    let mut block = String::from("NATS/1.0\r\n");
    for (name, value) in headers {
        block.push_str(name);
        block.push_str(": ");
        block.push_str(value);
        block.push_str("\r\n");
    }
    block.push_str("\r\n");
    block.into_bytes()
}

/// The headers a header block carries, in order.
fn headers_of(block: &[u8]) -> Result<Vec<(String, String)>> {
    let text = std::str::from_utf8(block)
        .map_err(|_| protocol_error("a header block that is not UTF-8"))?;
    let mut lines = text.split("\r\n");
    if !lines
        .next()
        .is_some_and(|first| first.starts_with("NATS/1.0"))
    {
        return Err(protocol_error("a header block that does not open NATS/1.0"));
    }
    lines
        .take_while(|line| !line.is_empty())
        .map(|line| {
            line.split_once(':')
                .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
                .ok_or_else(|| protocol_error(format!("{line:?} is not a header")))
        })
        .collect()
}

fn payload(reader: &mut impl BufRead, length: &str) -> Result<Vec<u8>> {
    let length: usize = length
        .parse()
        .map_err(|_| protocol_error(format!("{length:?} is not a payload length")))?;
    ceiling::within(length, net::MAX_BODY, "Xmip reads in one payload")?;
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
        round_trip(&Line::HPub {
            subject: "orders.new".into(),
            reply: Some("_INBOX.1".into()),
            headers: vec![("Nats-Msg-Id".into(), "0b6f5a52".into())],
            payload: b"hello\r\nworld".to_vec(),
        });
        round_trip(&Line::HPub {
            subject: "orders.new".into(),
            reply: None,
            headers: Vec::new(),
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
        assert_eq!(
            read(&mut &b"PING\n"[..]).expect("bare LF"),
            Some(Line::Ping),
            "a bare LF ends a line, as the NATS server itself reads it"
        );
        let claimed = read(&mut &b"PUB a 18446744073709551615\r\n"[..]);
        assert!(claimed.expect_err("claimed").message.contains("over the"));
        assert!(read(&mut &b"PUB a 5\r\nhel\r\n"[..]).is_err(), "short");
        assert!(read(&mut &b"PUB a x\r\n"[..]).is_err(), "length");
        assert!(read(&mut &b"PUB a\r\n"[..]).is_err(), "no length");
        assert!(read(&mut &b"HELO\r\n"[..]).is_err(), "verb");
        let wire = b"HPUB a 15 15\r\nNATS/1.0\r\nX\r\n\r\n\r\n";
        assert!(
            read(&mut &wire[..]).is_err(),
            "a header line without a colon"
        );
        let past = b"HPUB a 9 4\r\nabcd\r\n";
        assert!(read(&mut &past[..]).is_err(), "a block past the end");
        assert!(read(&mut &b"PUB a 2\r\nabXX"[..]).is_err(), "no CRLF");
        let lower = read(&mut &b"ping\r\n"[..]).expect("read");
        assert_eq!(lower, Some(Line::Ping), "verbs are case-insensitive");
    }

    #[test]
    fn an_hmsg_is_delivered_with_the_headers_its_publisher_set() {
        let hmsg = Line::HMsg {
            subject: "orders".into(),
            sid: "1".into(),
            reply: None,
            headers: vec![("Sender".into(), "c1".into())],
            payload: b"{}".to_vec(),
        };
        let wire = encode(&hmsg);
        assert!(wire.starts_with(b"HMSG orders 1 "), "{wire:?}");
        assert_eq!(read(&mut wire.as_slice()).expect("read"), Some(hmsg));
    }
}
