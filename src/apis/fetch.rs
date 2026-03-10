use bytes::Buf;
use std::io::Cursor;
use tracing::info;

use crate::binary::{
    read_compact_array_len, read_compact_string, read_uuid, write_uvarint, TagBuffer,
};
use crate::router::RequestContext;
use crate::wire::{Decode, DecodeError, Encode, EncodeError};

// ── Error codes ───────────────────────────────────────────────────────────────

const ERROR_UNKNOWN_TOPIC_ID: i16 = 100;

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
    pub topic_id: [u8; 16],
    pub partitions: Vec<PartitionFetchRequest>,
}

#[derive(Debug, Clone)]
pub struct PartitionFetchRequest {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
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

/// Per-partition response inside a topic response.
///
/// Fetch Response partition (v16):
///   partition_index        => INT32
///   error_code             => INT16
///   high_watermark         => INT64
///   last_stable_offset     => INT64
///   log_start_offset       => INT64
///   aborted_transactions   => COMPACT_ARRAY (null/empty = varint 0x01 for empty)
///   preferred_read_replica => INT32  (-1 = none)
///   records                => COMPACT_NULLABLE_BYTES (null = 0x00)
///   TAG_BUFFER
#[derive(Debug, Clone)]
pub struct PartitionFetchResponse {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    preferred_read_replica: i32,
    /// Raw RecordBatch bytes read directly from the partition log file.
    /// None => null COMPACT_NULLABLE_BYTES (varint 0)
    /// Some(bytes) => varint(len+1) followed by raw bytes
    records: Option<Vec<u8>>,
}

impl Encode for PartitionFetchResponse {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.partition_index.to_be_bytes());
        out.extend_from_slice(&self.error_code.to_be_bytes());
        out.extend_from_slice(&self.high_watermark.to_be_bytes());
        out.extend_from_slice(&self.last_stable_offset.to_be_bytes());
        out.extend_from_slice(&self.log_start_offset.to_be_bytes());
        // aborted_transactions: empty COMPACT_ARRAY => varint 1 (N+1 where N=0)
        write_uvarint(out, 1);
        // preferred_read_replica
        out.extend_from_slice(&self.preferred_read_replica.to_be_bytes());
        // records: COMPACT_NULLABLE_BYTES
        match &self.records {
            None => write_uvarint(out, 0), // null
            Some(bytes) => {
                write_uvarint(out, bytes.len() as u32 + 1);
                out.extend_from_slice(bytes);
            }
        }
        // TAG_BUFFER
        TagBuffer.encode(out);
        Ok(())
    }
}

/// Per-topic response inside the outer responses array.
///
/// Fetch Response topic (v16):
///   topic_id   => UUID
///   partitions => COMPACT_ARRAY of PartitionFetchResponse
///   TAG_BUFFER
#[derive(Debug, Clone)]
pub struct TopicFetchResponse {
    topic_id: [u8; 16],
    partitions: Vec<PartitionFetchResponse>,
}

impl Encode for TopicFetchResponse {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.topic_id);
        // COMPACT_ARRAY length = N+1
        write_uvarint(out, (self.partitions.len() + 1) as u32);
        for p in &self.partitions {
            p.encode(out)?;
        }
        TagBuffer.encode(out);
        Ok(())
    }
}

/// Top-level Fetch Response (v16):
///   throttle_time_ms => INT32
///   error_code       => INT16
///   session_id       => INT32
///   responses        => COMPACT_ARRAY of TopicFetchResponse
///   TAG_BUFFER
#[derive(Debug, Clone)]
pub struct FetchResponse {
    throttle_time_ms: i32,
    error_code: i16,
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

pub fn handle(request: &FetchRequest, ctx: &mut RequestContext) -> FetchResponse {
    info!("FetchRequest: {:?}", request);

    // Resolve topic_id -> topic_name under the read lock, then drop the lock
    // before borrowing query_engine mutably.
    let topic_names: Vec<Option<String>> = {
        let cluster_metadata = ctx
            .cluster_metadata
            .read()
            .expect("cluster metadata lock poisoned");
        request
            .topics
            .iter()
            .map(|t| {
                cluster_metadata
                    .topics
                    .get(&t.topic_id)
                    .map(|m| m.topic.name.clone())
            })
            .collect()
    }; // lock dropped here

    let responses: Vec<TopicFetchResponse> = request
        .topics
        .iter()
        .zip(topic_names.iter())
        .map(|(topic, topic_name)| {
            if let Some(topic_name) = topic_name {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|p| {
                        let records = ctx
                            .query_engine
                            .fetch_messages(topic_name, p.partition, p.fetch_offset)
                            .unwrap_or_else(|err| {
                                info!(error = ?err, "failed to fetch messages from disk");
                                None
                            });
                        PartitionFetchResponse {
                            partition_index: p.partition,
                            error_code: 0,
                            high_watermark: 0,
                            last_stable_offset: 0,
                            log_start_offset: 0,
                            preferred_read_replica: -1,
                            records,
                        }
                    })
                    .collect();
                TopicFetchResponse {
                    topic_id: topic.topic_id,
                    partitions,
                }
            } else {
                // Topic not found — UNKNOWN_TOPIC_ID (100) on every requested partition
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|p| PartitionFetchResponse {
                        partition_index: p.partition,
                        error_code: ERROR_UNKNOWN_TOPIC_ID,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        log_start_offset: -1,
                        preferred_read_replica: -1,
                        records: None,
                    })
                    .collect();
                TopicFetchResponse {
                    topic_id: topic.topic_id,
                    partitions,
                }
            }
        })
        .collect();

    FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: request.session_id,
        responses,
        tag_buffer: TagBuffer {},
    }
}
