pub mod api_versions;
pub mod describe_topic_partitions;
pub mod fetch;
pub mod produce;

use crate::apis::produce::{ProduceApiResponse, ProduceRequest};
use crate::binary::{read_compact_string as read_compact_string_impl, write_uvarint};
use crate::wire::{Decode, DecodeError, Encode, EncodeError};
use api_versions::{ApiVersionsRequest, ApiVersionsResponse};
use describe_topic_partitions::{DescribeTopicsRequest, DescribeTopicsResponse};
use fetch::{FetchRequest, FetchResponse};
use std::io::Cursor;

// Kafka flexible "tag buffer": 0 means no tagged fields
pub fn encode_empty_tag_buffer(out: &mut Vec<u8>) {
    write_uvarint(out, 0);
}

pub fn decode_compact_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    read_compact_string_impl(cur)
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

pub use crate::binary::TagBuffer;

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
    Fetch(FetchRequest),
    // Metadata(MetadataRequest),
    Produce(ProduceRequest),
    // Fetch(FetchRequest),
    // ListOffsets(ListOffsetsRequest),
}

#[derive(Debug, Clone)]
pub enum ResBody {
    ApiVersions(ApiVersionsResponse),
    DescribeTopics(DescribeTopicsResponse),
    Fetch(FetchResponse),
    ErrorCode(i16),
    Produce(ProduceApiResponse),
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
            FETCH => Ok(Self::Fetch(FetchRequest::decode(cur)?)),
            _ => Err(crate::unknown_api_key!(cur, api_key)),
        }
    }
}

impl BodyEncoder for ResBody {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Self::ApiVersions(response) => response.encode(out),
            Self::DescribeTopics(response) => response.encode(out),
            Self::Fetch(response) => response.encode(out),
            Self::Produce(response) => response.encode(out),
            Self::ErrorCode(error_code) => {
                out.extend_from_slice(&error_code.to_be_bytes());
                Ok(())
            }
        }
    }
}
