use anyhow::Result;
use bytes::Buf;
use std::io::Cursor;

use crate::binary::{
    TagBuffer, read_compact_array_len, read_compact_nullable_string, read_compact_string, read_i16,
    read_i32, read_uvarint, write_uvarint,
};
use crate::kraft::RecordBatch;
use crate::router::RequestContext;
use crate::wire::{Decode, DecodeError, Encode, EncodeError};

fn encode_compact_nullable_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => write_uvarint(out, 0),
        Some(value) => {
            write_uvarint(out, value.len() as u32 + 1);
            out.extend_from_slice(value.as_bytes());
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionProduceReq {
    partition_index: i32,
    record_batch: Vec<RecordBatch>,
    tag_buffer: TagBuffer,
}

impl Decode for PartitionProduceReq {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let partition_index = read_i32(cur, "produce.partition_index")?;
        let records_len_plus_one = read_uvarint(cur)?;
        let mut record_batch = Vec::new();
        if records_len_plus_one > 0 {
            let records_len = (records_len_plus_one - 1) as usize;
            if cur.remaining() < records_len {
                return Err(crate::truncated!(cur, "produce.records bytes"));
            }

            let mut records_bytes = vec![0u8; records_len];
            cur.copy_to_slice(&mut records_bytes);

            let mut records_cur = Cursor::new(records_bytes.as_slice());
            while (records_cur.position() as usize) < records_bytes.len() {
                record_batch.push(RecordBatch::decode(&mut records_cur)?);
            }
        }
        let tag_buffer = TagBuffer::decode(cur)?;
        Ok(PartitionProduceReq {
            partition_index,
            record_batch,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TopicProduceReq {
    topic_name: String,
    partitions: Vec<PartitionProduceReq>,
    tag_buffer: TagBuffer,
}

impl Decode for TopicProduceReq {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let topic_name = read_compact_string(cur)?;
        let partitions_len = read_compact_array_len(cur)?;
        let mut partitions = Vec::with_capacity(partitions_len);
        for _ in 0..partitions_len {
            partitions.push(PartitionProduceReq::decode(cur)?);
        }
        let tag_buffer = TagBuffer::decode(cur)?;
        Ok(Self {
            topic_name,
            partitions,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    // txid: compact_nullable_string
    // required_acks: 2 byte, big endian integer
    // timeout: 4 byte, big endian integer
    // topics: compact array.
    // - array len: lenght of array + 1, encoded as usigned varint
    // - topic_name: compact string
    // - partitions: array of partitions of this topic.
    //      - partition_index: 4 byte big endian integer.
    //      - record_batch: array[RecordBatch]
    //      - tag_buffer
    txid: String,
    required_acks: i16,
    timeout: i32,
    topics: Vec<TopicProduceReq>,
    tag_buffer: TagBuffer,
}

impl Decode for ProduceRequest {
    fn decode(cur: &mut Cursor<&[u8]>) -> std::result::Result<Self, DecodeError> {
        let txid = read_compact_nullable_string(cur)?.unwrap_or_default();
        let required_acks = read_i16(cur, "produce.required_acks")?;
        let timeout = read_i32(cur, "produce.timeout")?;
        let topics_len = read_compact_array_len(cur)?;
        let mut topics = Vec::with_capacity(topics_len);
        for _ in 0..topics_len {
            topics.push(TopicProduceReq::decode(cur)?);
        }
        let tag_buffer = TagBuffer::decode(cur)?;
        Ok(Self {
            txid,
            required_acks,
            timeout,
            topics,
            tag_buffer,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RecordError {
    batch_index: i32,
    batch_index_error_message: String,
}

impl Encode for RecordError {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.batch_index.to_be_bytes());
        encode_compact_nullable_string(out, Some(&self.batch_index_error_message));
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PartitionProduceResp {
    index: i32,
    error_code: i16,
    base_offset: i64,
    log_append_time_ms: i64,
    log_start_offset: i64,
    record_errors: Vec<RecordError>,
    error_message: String,
    tag_buffer: TagBuffer,
}

impl Encode for PartitionProduceResp {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&self.index.to_be_bytes());
        out.extend_from_slice(&self.error_code.to_be_bytes());
        out.extend_from_slice(&self.base_offset.to_be_bytes());
        out.extend_from_slice(&self.log_append_time_ms.to_be_bytes());
        out.extend_from_slice(&self.log_start_offset.to_be_bytes());

        write_uvarint(out, (self.record_errors.len() + 1) as u32);
        for record_error in &self.record_errors {
            record_error.encode(out)?;
        }

        encode_compact_nullable_string(out, Some(&self.error_message));
        self.tag_buffer.encode(out);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TopicProduceResp {
    name: String,
    partition_responses: Vec<PartitionProduceResp>,
    tag_buffer: TagBuffer,
}

impl Encode for TopicProduceResp {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        write_uvarint(out, self.name.len() as u32 + 1);
        out.extend_from_slice(self.name.as_bytes());
        write_uvarint(out, (self.partition_responses.len() + 1) as u32);
        for partition_response in &self.partition_responses {
            partition_response.encode(out)?;
        }
        self.tag_buffer.encode(out);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProduceApiResponse {
    // Produce Response (Version: 11) => [responses] throttle_time_ms <tag: 0>
    // responses => name [partition_responses]
    //     name => COMPACT_STRING
    //     partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0>
    //     index => INT32
    //     error_code => INT16
    //     base_offset => INT64
    //     log_append_time_ms => INT64
    //     log_start_offset => INT64
    //     record_errors => batch_index batch_index_error_message
    //         batch_index => INT32
    //         batch_index_error_message => COMPACT_NULLABLE_STRING
    //     error_message => COMPACT_NULLABLE_STRING
    //     current_leader<tag: 0> => leader_id leader_epoch
    //         leader_id => INT32
    //         leader_epoch => INT32
    // throttle_time_ms => INT32
    // empty tag
    responses: Vec<TopicProduceResp>,
    throttle_time_ms: i32,
    tag_buffer: TagBuffer,
}

impl Encode for ProduceApiResponse {
    fn encode(&self, out: &mut Vec<u8>) -> std::result::Result<(), EncodeError> {
        write_uvarint(out, (self.responses.len() + 1) as u32);
        for response in &self.responses {
            response.encode(out)?;
        }
        out.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        self.tag_buffer.encode(out);
        Ok(())
    }
}

pub fn handle(request: &ProduceRequest, ctx: &RequestContext) -> ProduceApiResponse {
    let cluster_metadata = ctx.cluster_metadata.read().unwrap();

    let responses = request
        .topics
        .iter()
        .map(|topic| {
            let topic_metadata = cluster_metadata
                .topics
                .values()
                .find(|candidate| candidate.topic.name == topic.topic_name);

            let partition_responses = topic
                .partitions
                .iter()
                .map(|partition| {
                    let partition_metadata = topic_metadata.and_then(|topic_metadata| {
                        topic_metadata.partitions.get(&partition.partition_index)
                    });

                    if partition_metadata.is_some() {
                        PartitionProduceResp {
                            index: partition.partition_index,
                            error_code: 0,
                            base_offset: 0,
                            log_append_time_ms: -1,
                            log_start_offset: 0,
                            record_errors: vec![],
                            error_message: String::new(),
                            tag_buffer: TagBuffer,
                        }
                    } else {
                        PartitionProduceResp {
                            index: partition.partition_index,
                            error_code: 3,
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: -1,
                            record_errors: vec![],
                            error_message: String::new(),
                            tag_buffer: TagBuffer,
                        }
                    }
                })
                .collect();

            TopicProduceResp {
                name: topic.topic_name.clone(),
                partition_responses,
                tag_buffer: TagBuffer,
            }
        })
        .collect();

    ProduceApiResponse {
        responses,
        throttle_time_ms: 0,
        tag_buffer: TagBuffer,
    }
}
