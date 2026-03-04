use bytes::Buf;
use std::io::Cursor;

use crate::apis::{
    self, TagBuffer, decode_compact_string, encode_bool, encode_compact_string,
    encode_empty_tag_buffer, encode_uuid, read_uvarint, write_uvarint,
};
use crate::kraft;
use crate::wire::{Decode, DecodeError, Encode, EncodeError};
use tracing::{error, trace};

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
        let topics_len_plus1 = read_uvarint(cur).unwrap();
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
pub struct PartitionMetadata {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: Vec<i32>, // see if broker Ids are actually i32
    isr_nodes: Vec<i32>,
    eligible_leader_replicas: Vec<i32>,
    last_known_elr: Vec<i32>,
    offline_replicas: Vec<i32>,
    tag_buffer: apis::TagBuffer,
}

impl Encode for PartitionMetadata {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.error_code.to_be_bytes());
        out.extend_from_slice(&self.partition_index.to_be_bytes());
        out.extend_from_slice(&self.leader_id.to_be_bytes());
        out.extend_from_slice(&self.leader_epoch.to_be_bytes());

        write_uvarint(out, (self.replica_nodes.len() as u32) + 1);
        for replica in &self.replica_nodes {
            out.extend_from_slice(&replica.to_be_bytes());
        }

        write_uvarint(out, (self.isr_nodes.len() as u32) + 1);
        for isr in &self.isr_nodes {
            out.extend_from_slice(&isr.to_be_bytes());
        }

        write_uvarint(out, (self.eligible_leader_replicas.len() as u32) + 1);
        for elr in &self.eligible_leader_replicas {
            out.extend_from_slice(&elr.to_be_bytes());
        }

        write_uvarint(out, (self.last_known_elr.len() as u32) + 1);
        for last_known_elr in &self.last_known_elr {
            out.extend_from_slice(&last_known_elr.to_be_bytes());
        }

        write_uvarint(out, (self.offline_replicas.len() as u32) + 1);
        for offline_replica in &self.offline_replicas {
            out.extend_from_slice(&offline_replica.to_be_bytes());
        }

        encode_empty_tag_buffer(out);
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
    use crate::kraft::RecordValue;
    use std::collections::HashMap;

    let mut topics_out = Vec::new();

    let batches = match kraft::load_metadata_image() {
        Ok(b) => b,
        Err(err) => {
            error!(error = ?err, "failed to load metadata image");
            return DescribeTopicsResponse {
                throttle_time_ms: 0,
                topics: topics_out,
                next_cursor: -1,
                tag_buffer: apis::TagBuffer,
            };
        }
    };

    // --- pass 1: collect Topic and Partition records from all batches ---
    // topic_id -> topic name
    let mut topic_names: HashMap<[u8; 16], String> = HashMap::new();
    // topic_id -> list of partitions
    let mut partitions_by_topic: HashMap<[u8; 16], Vec<kraft::Partition>> = HashMap::new();

    for batch in &batches {
        for record in &batch.records {
            match &record.value {
                RecordValue::Topic(t) => {
                    topic_names.insert(t.topic_id, t.name.clone());
                }
                RecordValue::Partition(p) => {
                    partitions_by_topic
                        .entry(p.topic_id)
                        .or_default()
                        .push(p.clone());
                }
                _ => {}
            }
        }
    }

    // sort partitions within each topic
    for parts in partitions_by_topic.values_mut() {
        parts.sort_by_key(|p| p.partition_id);
    }

    // --- pass 2: resolve requested topic names and build response ---
    let requested_names: Vec<String> = if request.topics.is_empty() {
        let mut names: Vec<String> = topic_names.values().cloned().collect();
        names.sort();
        names
    } else {
        request.topics.iter().map(|t| t.name.clone()).collect()
    };
    trace!(requested_topics = ?requested_names, "resolved requested topic names");

    for topic_name in requested_names {
        // find the topic_id for this name
        let maybe_id = topic_names
            .iter()
            .find(|(_, n)| *n == &topic_name)
            .map(|(id, _)| *id);

        if let Some(topic_id) = maybe_id {
            let empty = Vec::new();
            let parts = partitions_by_topic.get(&topic_id).unwrap_or(&empty);

            let partitions = parts
                .iter()
                .map(|p| PartitionMetadata {
                    error_code: 0,
                    partition_index: p.partition_id,
                    leader_id: p.leader,
                    leader_epoch: p.leader_epoch,
                    replica_nodes: p.replicas.clone(),
                    isr_nodes: p.sync_replicas.clone(),
                    eligible_leader_replicas: vec![],
                    last_known_elr: vec![],
                    offline_replicas: vec![],
                    tag_buffer: TagBuffer,
                })
                .collect();

            topics_out.push(TopicMetadata {
                error_code: 0,
                topic_name,
                topic_id,
                is_internal: false,
                partitions,
                topic_authorized_operations: 0,
                tag_buffer: TagBuffer,
            });
        } else {
            topics_out.push(TopicMetadata {
                error_code: 3,
                topic_name,
                topic_id: [0; 16],
                is_internal: false,
                partitions: vec![],
                topic_authorized_operations: 0,
                tag_buffer: TagBuffer,
            });
        }
    }

    DescribeTopicsResponse {
        throttle_time_ms: 0,
        topics: topics_out,
        next_cursor: -1,
        tag_buffer: apis::TagBuffer,
    }
}
