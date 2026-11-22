//! Object framing: `[magic: 4][version: 1][postcard payload][crc32: 4]`.

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

// 2: timestamped versions, per-writer logs.
const VERSION: u8 = 2;

#[derive(Copy, Clone, Debug)]
pub struct Kind {
    pub magic: [u8; 4],
}

pub const MANIFEST: Kind = Kind { magic: *b"BTMF" };
pub const WAL: Kind = Kind { magic: *b"BTWL" };
pub const NODE: Kind = Kind { magic: *b"BTND" };
pub const LEASE: Kind = Kind { magic: *b"BTLS" };

pub fn encode<T: Serialize>(kind: Kind, value: &T) -> Bytes {
    // Serializing owned in-memory types into a Vec cannot fail.
    let payload = postcard::to_stdvec(value).expect("postcard serialization into Vec");
    let mut buf = BytesMut::with_capacity(payload.len() + 9);
    buf.put_slice(&kind.magic);
    buf.put_u8(VERSION);
    buf.put_slice(&payload);
    buf.put_u32(crc32fast::hash(&buf));
    buf.freeze()
}

pub fn decode<T: DeserializeOwned>(kind: Kind, path: &str, data: &[u8]) -> Result<T> {
    let corrupt = |reason: &str| Error::Corrupt {
        path: path.to_string(),
        reason: reason.to_string(),
    };
    let body_len = data
        .len()
        .checked_sub(4)
        .filter(|&n| n >= 5)
        .ok_or_else(|| corrupt("too short"))?;
    let (body, crc) = data.split_at(body_len);
    let expected = u32::from_be_bytes([crc[0], crc[1], crc[2], crc[3]]);
    if crc32fast::hash(body) != expected {
        return Err(corrupt("checksum mismatch"));
    }
    if body[..4] != kind.magic {
        return Err(corrupt("wrong magic"));
    }
    if body[4] != VERSION {
        return Err(corrupt("unsupported version"));
    }
    postcard::from_bytes(&body[5..]).map_err(|e| corrupt(&e.to_string()))
}
