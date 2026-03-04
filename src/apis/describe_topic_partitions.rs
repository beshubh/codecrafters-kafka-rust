use bytes::Buf;
use std::io::Cursor;

use crate::apis::api_versions::ApiVersionsResponse;
use crate::apis::{
    self, BodyDecoder, BodyEncoder, ReqBody, ResBody, TagBuffer, decode_compact_string,
    encode_bool, encode_compact_string, encode_empty_tag_buffer, encode_uuid, read_uvarint,
    write_uvarint,
};
use crate::wire::{Decode, DecodeError, Encode, EncodeError};

#[derive(Debug, Clone)]
pub struct Topic {
    name: String,
    tag_buffer: apis::TagBuffer,
}

impl Decode for Topic {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let topic_name = decode_compact_string(cur)?;
        let tag_buffer = apis::TagBuffer::decode(cur)?;
        Ok(Self {
            name: topic_name,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DescribeTopicsRequest {
    topics: Vec<Topic>,
    response_partition_limit: u32,
    cursor: u8,
    tag_buffer: apis::TagBuffer,
}

impl Decode for DescribeTopicsRequest {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let topics_len_plus1 = read_uvarint(cur)?;
        if topics_len_plus1 == 0 {
            return Err(DecodeError::InvalidLength);
        }
        let topics_len = (topics_len_plus1 - 1) as usize;
        if cur.remaining() < topics_len {
            return Err(DecodeError::Truncated);
        }
        let mut topics = Vec::with_capacity(topics_len as usize);
        for _ in 0..topics_len {
            topics.push(Topic::decode(cur)?);
        }
        let response_partition_limit = cur.get_u32();
        let cursor = cur.get_u8(); // TODO: what is the type of cursor?
        let tag_buffer = apis::TagBuffer::decode(cur)?;
        Ok(Self {
            topics,
            response_partition_limit,
            cursor,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata;

impl Encode for PartitionMetadata {
    fn encode(&self, _out: &mut Vec<u8>) -> Result<(), EncodeError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    error_code: i16,
    topic_name: String,
    topic_id: [u8; 16],
    is_internal: bool,
    partitions: Vec<PartitionMetadata>,
    topic_authorized_operations: i32,
    tag_buffer: apis::TagBuffer,
}

impl Encode for TopicMetadata {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.error_code.to_be_bytes());
        encode_compact_string(out, &self.topic_name);
        encode_uuid(out, &self.topic_id);
        encode_bool(out, self.is_internal);
        let len_plus_one = (self.partitions.len() as u32) + 1;
        write_uvarint(out, len_plus_one);
        for partition in &self.partitions {
            partition.encode(out)?;
        }
        out.extend_from_slice(&self.topic_authorized_operations.to_be_bytes());
        encode_empty_tag_buffer(out);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DescribeTopicsResponse {
    throttle_time_ms: i32,
    topics: Vec<TopicMetadata>,
    next_cursor: i8,
    tag_buffer: apis::TagBuffer,
}

impl Encode for DescribeTopicsResponse {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.throttle_time_ms.to_be_bytes());

        // COMPACT_ARRAY length is UNSIGNED_VARINT(len + 1)
        let len_plus_one = (self.topics.len() as u32) + 1;
        write_uvarint(out, len_plus_one);

        for topic in &self.topics {
            topic.encode(out)?;
        }
        out.extend_from_slice(&self.next_cursor.to_be_bytes());
        encode_empty_tag_buffer(out);
        Ok(())
    }
}

pub fn handle(request: &DescribeTopicsRequest, _api_version: i16) -> DescribeTopicsResponse {
    let topics = vec![TopicMetadata {
        error_code: 3,
        topic_name: request.topics[0].name.clone(),
        topic_id: [0; 16],
        is_internal: false,
        partitions: vec![],
        topic_authorized_operations: 0,
        tag_buffer: TagBuffer,
    }];
    DescribeTopicsResponse {
        throttle_time_ms: 0,
        topics,
        next_cursor: -1,
        tag_buffer: apis::TagBuffer,
    }
}
