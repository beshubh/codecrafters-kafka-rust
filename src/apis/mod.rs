pub mod api_versions;
pub mod describe_topic_partitions;

use api_versions::{ApiVersionsRequest, ApiVersionsResponse};
use describe_topic_partitions::{DescribeTopicsRequest, DescribeTopicsResponse};

use crate::wire::{Decode, DecodeError, Encode, EncodeError};
use bytes::Buf;
use std::io::Cursor;

pub fn read_uvarint(cur: &mut Cursor<&[u8]>) -> Result<u32, DecodeError> {
    let mut value: u32 = 0;
    let mut shift = 0;

    loop {
        if cur.remaining() < 1 {
            return Err(DecodeError::Truncated);
        }
        let byte = cur.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;

        if (byte & 0x80) == 0 {
            return Ok(value);
        }

        shift += 7;
        if shift > 28 {
            return Err(DecodeError::InvalidLength); // varint too long
        }
    }
}

// Kafka flexible "tag buffer": 0 means no tagged fields
pub fn encode_empty_tag_buffer(out: &mut Vec<u8>) {
    write_uvarint(out, 0);
}

pub fn write_uvarint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn decode_compact_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let len_plus_one = read_uvarint(cur)? as i64;

    // For COMPACT_STRING (non-nullable), 0 is invalid.
    if len_plus_one == 0 {
        return Err(DecodeError::InvalidLength);
    }

    let len = (len_plus_one - 1) as usize;
    if cur.remaining() < len {
        return Err(DecodeError::Truncated);
    }

    let mut buf = vec![0u8; len];
    cur.copy_to_slice(&mut buf);
    String::from_utf8(buf).map_err(|_| DecodeError::InvalidUtf8)
}

pub fn encode_compact_string(out: &mut Vec<u8>, s: &str) {
    let len = s.len();
    write_uvarint(out, len as u32 + 1);
    out.extend_from_slice(s.as_bytes());
}

pub fn encode_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

pub fn encode_uuid(out: &mut Vec<u8>, value: &[u8; 16]) {
    out.extend_from_slice(value);
}

#[derive(Debug, Clone, Default)]
pub struct TagBuffer; // placeholder until you implement parsing

impl Decode for TagBuffer {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        if cur.remaining() < 1 {
            return Err(DecodeError::Truncated);
        }
        let len = cur.get_i8();
        if len < 0 {
            return Err(DecodeError::Truncated); // or InvalidLength
        }
        let n = len as usize;
        if cur.remaining() < n {
            return Err(DecodeError::Truncated);
        }
        cur.advance(n); // consumes the tag bytes
        Ok(Self)
    }
}

pub const API_VERSION: i16 = 18;
pub const DESCRIBE_TOPIC_PARTITIONS: i16 = 75;
pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const OFFSET_FETCH: i16 = 9;

#[derive(Debug, Clone)]
pub enum ReqBody {
    ApiVersions(ApiVersionsRequest),
    DescribeTopics(DescribeTopicsRequest),
    // Metadata(MetadataRequest),
    // Produce(ProduceRequest),
    // Fetch(FetchRequest),
    // ListOffsets(ListOffsetsRequest),
}

#[derive(Debug, Clone)]
pub enum ResBody {
    ApiVersions(ApiVersionsResponse),
    DescribeTopics(DescribeTopicsResponse),
    ErrorCode(i16),
}

pub trait BodyDecoder: Sized {
    fn decode(cur: &mut Cursor<&[u8]>, api_key: i16) -> Result<Self, DecodeError>;
}

pub trait BodyEncoder {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError>;
}

impl BodyDecoder for ReqBody {
    fn decode(cur: &mut Cursor<&[u8]>, api_key: i16) -> Result<Self, DecodeError> {
        match api_key {
            API_VERSION => Ok(Self::ApiVersions(ApiVersionsRequest::decode(cur)?)),
            DESCRIBE_TOPIC_PARTITIONS => {
                Ok(Self::DescribeTopics(DescribeTopicsRequest::decode(cur)?))
            }
            _ => Err(DecodeError::UnknownApiKey(api_key)),
        }
    }
}

impl BodyEncoder for ResBody {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Self::ApiVersions(response) => response.encode(out),
            Self::DescribeTopics(response) => response.encode(out),
            Self::ErrorCode(error_code) => {
                out.extend_from_slice(&error_code.to_be_bytes());
                Ok(())
            }
        }
    }
}
