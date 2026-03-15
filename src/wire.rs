use std::io::Cursor;

use crate::apis::{self, BodyDecoder, BodyEncoder, ReqBody, ResBody};
use crate::binary::{ensure_remaining, read_i16, read_i32, read_u16, read_u32};
use bytes::Buf;

// Re-export so all existing import paths keep working.
pub use crate::binary::TagBuffer;
pub use crate::errors::DecodeError;

pub trait Decode: Sized {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError>;
}
pub trait DecodeVersioned: Sized {
    fn decode_versioned(cur: &mut Cursor<&[u8]>, version: i16) -> Result<Self, DecodeError>;
}
pub trait Encode {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError>;
}

#[derive(Debug)]
pub enum EncodeError {
    UnknownApiKey(i16),
    Message(String),
}

fn read_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let len = read_u16(cur, "string length")?;
    let n = len as usize;
    if cur.remaining() < n {
        return Err(crate::truncated!(cur, "string bytes"));
    }
    let mut buf = vec![0u8; n];
    cur.copy_to_slice(&mut buf);
    String::from_utf8(buf).map_err(|_| crate::invalid_utf8!(cur, "string"))
}

#[derive(Debug, Clone)]
pub struct HClientId(pub String);

impl Decode for HClientId {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        // Kafka STRING length is INT16; -1 => null
        let len = read_i16(cur, "client_id length")?;
        if len == -1 {
            return Ok(Self(String::new())); // or return Err / or use Option<String>
        }
        if len < 0 {
            return Err(crate::invalid_length!(cur, "client_id length", len));
        }

        let len_usize = len as usize;
        if cur.remaining() < len_usize {
            return Err(crate::truncated!(cur, "client_id bytes"));
        }

        let mut content_bytes = vec![0u8; len_usize];
        cur.copy_to_slice(&mut content_bytes);

        let contents =
            String::from_utf8(content_bytes).map_err(|_| crate::invalid_utf8!(cur, "client_id"))?;

        Ok(Self(contents))
    }
}

#[derive(Debug, Clone)]
pub struct ReqHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: HClientId,
    pub tag_buffer: TagBuffer,
}

impl Decode for ReqHeader {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        // request_api_key (i16) + request_api_version (i16) + correlation_id (i32) = 8 bytes
        ensure_remaining(cur, 8, "request header")?;
        let request_api_key = read_i16(cur, "request_api_key")?;
        let request_api_version = read_i16(cur, "request_api_version")?;
        let correlation_id = read_i32(cur, "correlation_id")?;
        // These consume bytes from the same cursor, in-order.
        let client_id = HClientId::decode(cur)?;
        let tag_buffer = TagBuffer::decode(cur)?;

        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReqMessage {
    pub message_size: u32,
    pub header: ReqHeader,
    pub body: ReqBody,
}

impl Decode for ReqMessage {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let message_size = read_u32(cur, "message_size")?;
        if cur.remaining() < message_size as usize {
            return Err(crate::truncated!(cur, "message payload"));
        }
        let header = ReqHeader::decode(cur)?;
        let body = ReqBody::decode(cur, header.request_api_key)?;
        Ok(Self {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResHeaderV0 {
    pub correlation_id: i32,
}

impl Encode for ResHeaderV0 {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.correlation_id.to_be_bytes());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ResHeaderV1 {
    pub correlation_id: i32,
    pub tag_buffer: TagBuffer,
}

impl Encode for ResHeaderV1 {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.correlation_id.to_be_bytes());
        self.tag_buffer.encode(out);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ResHeader {
    V0(ResHeaderV0),
    V1(ResHeaderV1),
}

impl ResHeader {
    pub fn v0(correlation_id: i32) -> Self {
        Self::V0(ResHeaderV0 { correlation_id })
    }

    pub fn v1(correlation_id: i32, tag_buffer: TagBuffer) -> Self {
        Self::V1(ResHeaderV1 {
            correlation_id,
            tag_buffer,
        })
    }
}

impl Encode for ResHeader {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Self::V0(header) => header.encode(out),
            Self::V1(header) => header.encode(out),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResMessage {
    // pub api_key: i16,
    pub header: ResHeader,
    pub body: ResBody,
}

impl Encode for ResMessage {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let mut payload = Vec::new();
        self.header.encode(&mut payload)?;
        self.body.encode(&mut payload)?;
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes()); // message size
        out.extend_from_slice(&payload);
        Ok(())
    }
}

impl ResMessage {
    pub fn to_bytes(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        self.encode(&mut out)?;
        Ok(out)
    }
}
