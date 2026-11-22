//! Object framing:
//! `[magic: 4][version: 1][codec: 1][payload][crc32: 4]`, where the payload
//! is postcard, compressed per `codec`. Decoding reads the codec byte, so
//! compressed and uncompressed objects mix freely.

use bytes::{BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

// 2: timestamped versions, per-writer logs. 3: append-only runs, manifest
// records the applier. 4: codec byte. 5: leases record log placement.
// 6: leases record applier candidacy.
const VERSION: u8 = 6;
const HEADER: usize = 6;

#[derive(Copy, Clone, Debug)]
pub struct Kind {
    pub magic: [u8; 4],
}

pub const MANIFEST: Kind = Kind { magic: *b"SDMF" };
pub const WAL: Kind = Kind { magic: *b"SDWL" };
pub const NODE: Kind = Kind { magic: *b"SDND" };
pub const LEASE: Kind = Kind { magic: *b"SDLS" };

/// Compression of tree nodes and log batches.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    /// Fast (GB/s) with typical 2-4x savings on structured data. Objects that
    /// don't shrink are stored uncompressed.
    #[default]
    Lz4,
}

const CODEC_NONE: u8 = 0;
const CODEC_LZ4: u8 = 1;

pub fn encode<T: Serialize>(kind: Kind, value: &T, compression: Compression) -> Bytes {
    // Serializing owned in-memory types into a Vec cannot fail.
    let raw = postcard::to_stdvec(value).expect("postcard serialization into Vec");
    let (codec, payload) = match compression {
        Compression::None => (CODEC_NONE, raw),
        Compression::Lz4 => {
            let packed = lz4_flex::compress_prepend_size(&raw);
            if packed.len() < raw.len() {
                (CODEC_LZ4, packed)
            } else {
                (CODEC_NONE, raw)
            }
        }
    };
    let mut buf = BytesMut::with_capacity(payload.len() + HEADER + 4);
    buf.put_slice(&kind.magic);
    buf.put_u8(VERSION);
    buf.put_u8(codec);
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
        .filter(|&n| n >= HEADER)
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
    let payload = &body[HEADER..];
    match body[5] {
        CODEC_NONE => postcard::from_bytes(payload).map_err(|e| corrupt(&e.to_string())),
        CODEC_LZ4 => {
            let raw = lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| corrupt(&e.to_string()))?;
            postcard::from_bytes(&raw).map_err(|e| corrupt(&e.to_string()))
        }
        _ => Err(corrupt("unknown codec")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_skips_compression_that_does_not_help() {
        let text: Vec<String> = (0..200).map(|i| format!("user:{i:05} value")).collect();
        let none = encode(NODE, &text, Compression::None);
        let lz4 = encode(NODE, &text, Compression::Lz4);
        assert!(
            lz4.len() < none.len() / 2,
            "{} vs {}",
            lz4.len(),
            none.len()
        );
        let back: Vec<String> = decode(NODE, "t", &lz4).unwrap();
        assert_eq!(back, text);
        let noise: Vec<u8> = (0..256u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let stored = encode(NODE, &noise, Compression::Lz4);
        assert_eq!(stored[5], CODEC_NONE);
        assert_eq!(decode::<Vec<u8>>(NODE, "t", &stored).unwrap(), noise);
    }
}
