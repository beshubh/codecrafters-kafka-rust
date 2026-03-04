pub mod api_versions;
use api_versions::{ApiVersionsRequest, ApiVersionsResponse};

use crate::wire::{Decode, DecodeError, Encode, EncodeError};
use bytes::Buf;
use std::io::Cursor;

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
pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const OFFSET_FETCH: i16 = 9;

#[derive(Debug, Clone)]
pub enum ReqBody {
    ApiVersions(ApiVersionsRequest),
    // Metadata(MetadataRequest),
    // Produce(ProduceRequest),
    // Fetch(FetchRequest),
    // ListOffsets(ListOffsetsRequest),
}

#[derive(Debug, Clone)]
pub enum ResBody {
    ApiVersions(ApiVersionsResponse),
    ErrorCode(i16),
}

pub trait BodyDecoder: Sized {
    fn decode(cur: &mut Cursor<&[u8]>, api_key: i16) -> Result<Self, DecodeError>;
}

pub trait BodyEncoder {
    fn encode(&self, out: &mut Vec<u8>, api_key: i16) -> Result<(), EncodeError>;
}

impl BodyDecoder for ReqBody {
    fn decode(cur: &mut Cursor<&[u8]>, api_key: i16) -> Result<Self, DecodeError> {
        match api_key {
            API_VERSION => Ok(Self::ApiVersions(ApiVersionsRequest::decode(cur)?)),
            _ => Err(DecodeError::UnknownApiKey(api_key)),
        }
    }
}

impl BodyEncoder for ResBody {
    fn encode(&self, out: &mut Vec<u8>, api_key: i16) -> Result<(), EncodeError> {
        match (api_key, self) {
            (API_VERSION, ResBody::ApiVersions(response)) => response.encode(out),
            (_, ResBody::ErrorCode(error_code)) => {
                out.extend_from_slice(&error_code.to_be_bytes());
                Ok(())
            }
            _ => Err(EncodeError::UnknownApiKey(api_key)),
        }
    }
}
