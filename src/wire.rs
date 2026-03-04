use std::io::Cursor;

use crate::apis::{self, BodyDecoder, BodyEncoder, ReqBody, ResBody};
use bytes::Buf;

// Re-export so all existing import paths keep working.
pub use crate::binary::{DecodeError, TagBuffer};

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
}

fn read_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    if cur.remaining() < 2 {
        return Err(DecodeError::Truncated);
    }
    let len = cur.get_u16();
    let n = len as usize;
    if cur.remaining() < n {
        return Err(DecodeError::Truncated);
    }
    let mut buf = vec![0u8; n];
    cur.copy_to_slice(&mut buf);
    String::from_utf8(buf).map_err(|_| DecodeError::InvalidUtf8)
}

#[derive(Debug, Clone)]
pub struct HClientId(pub String);

impl Decode for HClientId {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        if cur.remaining() < 2 {
            return Err(DecodeError::Truncated);
        }

        // Kafka STRING length is INT16; -1 => null
        let len = cur.get_i16(); // requires Buf::get_i16()
        if len == -1 {
            return Ok(Self(String::new())); // or return Err / or use Option<String>
        }
        if len < 0 {
            return Err(DecodeError::InvalidLength);
        }

        let len_usize = len as usize;
        if cur.remaining() < len_usize {
            return Err(DecodeError::Truncated);
        }

        let mut content_bytes = vec![0u8; len_usize];
        cur.copy_to_slice(&mut content_bytes);

        let contents = String::from_utf8(content_bytes).map_err(|_| DecodeError::InvalidUtf8)?;

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
        if cur.remaining() < 8 {
            return Err(DecodeError::Truncated);
        }
        let request_api_key = cur.get_i16();
        let request_api_version = cur.get_i16();
        let correlation_id = cur.get_i32();
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
        let message_size = cur.get_u32();
        if cur.remaining() < message_size as usize {
            return Err(DecodeError::Truncated);
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
