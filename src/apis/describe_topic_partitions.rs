use anyhow::{Context, Result};
use std::io::Cursor;

use crate::apis::{
    self, decode_compact_string, encode_bool, encode_compact_string, encode_empty_tag_buffer,
    encode_uuid, write_uvarint, TagBuffer,
};
use crate::binary::{read_u32, read_u8, read_uvarint};
use crate::router::RequestContext;
use crate::wire::{Decode, DecodeError, Encode, EncodeError};
use tracing::trace;

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
            return Err(crate::invalid_length!(
                cur,
                "describe_topics.topics length",
                topics_len_plus1
            ));
        }
        let topics_len = (topics_len_plus1 - 1) as usize;
        let mut topics = Vec::with_capacity(topics_len);
        for _ in 0..topics_len {
            topics.push(Topic::decode(cur)?);
        }
        let response_partition_limit = read_u32(cur, "describe_topics.response_partition_limit")?;
        let cursor = read_u8(cur, "describe_topics.cursor")?;
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

pub fn handle(
    request: &DescribeTopicsRequest,
    ctx: &RequestContext,
) -> Result<DescribeTopicsResponse> {
    let mut topics_out = Vec::new();
    let cluster_metadata = ctx
        .cluster_metadata
        .read()
        .map_err(|err| anyhow::anyhow!("cluster metadata lock poisoned: {err}"))
        .context("failed to read cluster metadata for describe topics")?;

    // --- pass 2: resolve requested topic names and build response ---
    let requested_names: Vec<String> = if request.topics.is_empty() {
        let mut names: Vec<String> = cluster_metadata
            .topics
            .values()
            .map(|topic_meta| topic_meta.topic.name.clone())
            .collect();
        names.sort();
        names
    } else {
        request.topics.iter().map(|t| t.name.clone()).collect()
    };
    trace!(requested_topics = ?requested_names, "resolved requested topic names");

    for topic_name in requested_names {
        // find the topic_id for this name
        let maybe_id = cluster_metadata
            .topics
            .iter()
            .find(|(_, topic_meta)| topic_meta.topic.name == topic_name)
            .map(|(id, _)| *id);

        if let Some(topic_id) = maybe_id {
            let Some(topic_meta) = cluster_metadata.topics.get(&topic_id) else {
                continue;
            };

            let mut parts: Vec<_> = topic_meta.partitions.values().collect();
            parts.sort_by_key(|partition| partition.partition_id);

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
    topics_out.sort_by_key(|t| t.topic_name.clone());

    Ok(DescribeTopicsResponse {
        throttle_time_ms: 0,
        topics: topics_out,
        next_cursor: -1,
        tag_buffer: apis::TagBuffer,
    })
}
