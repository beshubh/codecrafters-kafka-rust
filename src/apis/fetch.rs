use bytes::Buf;
use std::io::Cursor;
use tracing::info;

use crate::binary::{
    TagBuffer, read_compact_array_len, read_compact_string, read_uuid, write_uvarint,
};
use crate::wire::{Decode, DecodeError, Encode, EncodeError};

// ── Request structs ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FetchRequest {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: Vec<TopicFetchRequest>,
    forgotten_topics_data: Vec<TopicFetchRequest>,
    rack_id: String,
    // cluster_id (tag 0) and replica_state (tag 1) are tagged fields;
    // we skip them for now via TagBuffer::decode.
    tag_buffer: TagBuffer,
}

#[derive(Debug, Clone)]
pub struct TopicFetchRequest {
    topic_id: [u8; 16],
    partitions: Vec<PartitionFetchRequest>,
}

#[derive(Debug, Clone)]
pub struct PartitionFetchRequest {
    partition: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

// ── Decode impls ──────────────────────────────────────────────────────────────

impl Decode for FetchRequest {
    // Fetch Request V16
    //   max_wait_ms => INT32
    //   min_bytes => INT32
    //   max_bytes => INT32
    //   isolation_level => INT8
    //   session_id => INT32
    //   session_epoch => INT32
    //   topics => topic_id [partitions]
    //     topic_id => UUID
    //     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes
    //       partition => INT32
    //       current_leader_epoch => INT32
    //       fetch_offset => INT64
    //       last_fetched_epoch => INT32
    //       log_start_offset => INT64
    //       partition_max_bytes => INT32
    //   forgotten_topics_data => topic_id [partitions]
    //     topic_id => UUID
    //     partitions => INT32
    //   rack_id => COMPACT_STRING
    //   cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
    //   replica_state<tag: 1> => replica_id replica_epoch
    //     replica_id => INT32
    //     replica_epoch => INT64
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let max_wait_ms = cur.get_i32();
        let min_bytes = cur.get_i32();
        let max_bytes = cur.get_i32();
        let isolation_level = cur.get_i8();
        let session_id = cur.get_i32();
        let session_epoch = cur.get_i32();

        let num_topics = read_compact_array_len(cur)?;
        let mut topics = Vec::with_capacity(num_topics);
        for _ in 0..num_topics {
            topics.push(TopicFetchRequest::decode(cur)?);
        }

        let num_forgotten = read_compact_array_len(cur)?;
        let mut forgotten_topics_data = Vec::with_capacity(num_forgotten);
        for _ in 0..num_forgotten {
            forgotten_topics_data.push(TopicFetchRequest::decode(cur)?);
        }

        let rack_id = read_compact_string(cur)?;

        // Skip the trailing tag buffer (cluster_id tag 0, replica_state tag 1, etc.)
        let tag_buffer = TagBuffer::decode(cur)?;

        Ok(Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            tag_buffer,
        })
    }
}

impl Decode for TopicFetchRequest {
    // topic_id [partitions] TAG_BUFFER
    //   topic_id   => UUID
    //   partitions => COMPACT_ARRAY of PartitionFetchRequest
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let topic_id = read_uuid(cur)?;
        let num_partitions = read_compact_array_len(cur)?;
        let mut partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            partitions.push(PartitionFetchRequest::decode(cur)?);
        }
        TagBuffer::decode(cur)?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

impl Decode for PartitionFetchRequest {
    // partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let s = Self {
            partition: src.get_i32(),
            current_leader_epoch: src.get_i32(),
            fetch_offset: src.get_i64(),
            last_fetched_epoch: src.get_i32(),
            log_start_offset: src.get_i64(),
            partition_max_bytes: src.get_i32(),
        };
        TagBuffer::decode(src)?;
        Ok(s)
    }
}

// ── Response structs ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TopicFetchResponse {}

impl Encode for TopicFetchResponse {
    fn encode(&self, _out: &mut Vec<u8>) -> Result<(), EncodeError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FetchResponse {
    error_code: i16,
    throttle_time_ms: i32,
    session_id: i32,
    responses: Vec<TopicFetchResponse>,
    tag_buffer: TagBuffer,
}

impl Encode for FetchResponse {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.throttle_time_ms.to_be_bytes()); // INT32 first per spec
        out.extend_from_slice(&self.error_code.to_be_bytes());
        out.extend_from_slice(&self.session_id.to_be_bytes());

        // COMPACT_ARRAY: length = N+1 encoded as uvarint
        write_uvarint(out, (self.responses.len() + 1) as u32);
        for topic in &self.responses {
            topic.encode(out)?;
        }

        // TAG_BUFFER: 0x00 = zero tagged fields
        self.tag_buffer.encode(out);
        Ok(())
    }
}

pub fn handle(request: &FetchRequest, _api_version: i16) -> FetchResponse {
    info!("FetchRequest: {:?}", request);
    FetchResponse {
        error_code: 0,
        session_id: request.session_id,
        throttle_time_ms: 0,
        responses: Vec::new(),
        tag_buffer: TagBuffer {},
    }
}
