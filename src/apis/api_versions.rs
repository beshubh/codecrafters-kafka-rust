use bytes::Buf;
use std::io::Cursor;

use crate::apis::TagBuffer;
use crate::wire::{Decode, DecodeError, Encode, EncodeError};

#[derive(Debug, Clone)]
pub struct ClientSoftwareName(pub String);
#[derive(Debug, Clone)]
pub struct ClientSoftwareVersion(pub String);

fn read_uvarint(cur: &mut Cursor<&[u8]>) -> Result<u32, DecodeError> {
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

fn decode_compact_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
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

impl Decode for ClientSoftwareName {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        Ok(Self(decode_compact_string(cur)?))
    }
}

impl Decode for ClientSoftwareVersion {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        Ok(Self(decode_compact_string(cur)?))
    }
}

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    client_id: ClientSoftwareName,
    client_software_version: ClientSoftwareVersion,
    tag_buffer: TagBuffer,
}
impl Decode for ApiVersionsRequest {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let client_id = ClientSoftwareName::decode(cur)?;
        let client_software_version = ClientSoftwareVersion::decode(cur)?;
        let tag_buffer = TagBuffer::decode(cur)?;
        Ok(Self {
            client_id,
            client_software_version,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: (),
}

impl Encode for ApiKey {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.api_key.to_be_bytes());
        out.extend_from_slice(&self.min_version.to_be_bytes());
        out.extend_from_slice(&self.max_version.to_be_bytes());

        // flexible versions require a tag buffer at end of struct
        encode_empty_tag_buffer(out);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,
    pub throttle_time_ms: i32,
    pub tag_buffer: (),
}

// Kafka flexible "tag buffer": 0 means no tagged fields
fn encode_empty_tag_buffer(out: &mut Vec<u8>) {
    write_uvarint(out, 0);
}

fn write_uvarint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

impl Encode for ApiVersionsResponse {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.error_code.to_be_bytes());
        // COMPACT_ARRAY length is UNSIGNED_VARINT(len + 1)
        let len_plus_one = (self.api_keys.len() as u32) + 1;
        write_uvarint(out, len_plus_one);

        for api_key in &self.api_keys {
            api_key.encode(out)?;
        }
        out.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        encode_empty_tag_buffer(out);
        Ok(())
    }
}

pub fn handle(_request: &ApiVersionsRequest, api_version: i16) -> ApiVersionsResponse {
    if !(0..=4).contains(&api_version) {
        return ApiVersionsResponse {
            error_code: 35,
            api_keys: vec![],
            throttle_time_ms: 0,
            tag_buffer: (),
        };
    }

    ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![ApiKey {
            api_key: 18,
            min_version: 0,
            max_version: 4,
            tag_buffer: (),
        }],
        throttle_time_ms: 0,
        tag_buffer: (),
    }
}
