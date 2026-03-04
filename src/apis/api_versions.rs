use bytes::Buf;
use std::io::Cursor;

use crate::apis::{
    decode_compact_string, encode_empty_tag_buffer, read_uvarint, write_uvarint, TagBuffer,
};
use crate::wire::{Decode, DecodeError, Encode, EncodeError};

#[derive(Debug, Clone)]
pub struct ClientSoftwareName(pub String);
#[derive(Debug, Clone)]
pub struct ClientSoftwareVersion(pub String);

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

    let support_apis = vec![
        ApiKey {
            api_key: 18,
            min_version: 0,
            max_version: 4,
            tag_buffer: (),
        },
        ApiKey {
            api_key: 75,
            min_version: 0,
            max_version: 0,
            tag_buffer: (),
        },
    ];
    ApiVersionsResponse {
        error_code: 0,
        api_keys: support_apis,
        throttle_time_ms: 0,
        tag_buffer: (),
    }
}
