use std::fs;
use std::io::Cursor;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Context;
use bytes::{Buf, BufMut};
use tracing::{debug, trace};

use crate::binary::{
    DecodeError, TaggedField, TaggedFields, ensure_remaining, read_compact_array_i32,
    read_compact_array_uuid, read_compact_string, read_i8, read_i16, read_i32, read_i64,
    read_tagged_fields, read_u8, read_uuid, read_uvarint, read_varint, write_uvarint,
};
use crate::wire::EncodeError;
use crate::wire::{Decode, Encode};

const TOPIC_RECORD_TYPE: i8 = 2;
const PARTITION_RECORD_TYPE: i8 = 3;
const FEATURE_LEVEL: i8 = 12;

// ── Public entry points ───────────────────────────────────────────────────────

pub fn default_metadata_log_path() -> PathBuf {
    std::env::var("KAFKA_METADATA_LOG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
        })
}

pub fn load_metadata_image() -> anyhow::Result<Vec<RecordBatch>> {
    load_metadata_image_from_path(default_metadata_log_path())
}

pub fn load_metadata_image_from_path(path: impl AsRef<Path>) -> anyhow::Result<Vec<RecordBatch>> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read metadata log: {}", path.display()))?;
    debug!(path = %path.display(), bytes = bytes.len(), "loaded metadata log file");
    parse_metadata_image(&bytes)
        .map_err(anyhow::Error::new)
        .context("failed to parse metadata image")
}

fn parse_metadata_image(bytes: &[u8]) -> Result<Vec<RecordBatch>, DecodeError> {
    let mut cur = Cursor::new(bytes);
    let mut batches = vec![];
    while cur.remaining() >= 12 {
        batches.push(RecordBatch::decode(&mut cur)?);
    }
    Ok(batches)
}

fn write_varint(out: &mut Vec<u8>, value: i32) {
    let zigzag = ((value << 1) ^ (value >> 31)) as u32;
    write_uvarint(out, zigzag);
}

fn write_compact_string(out: &mut Vec<u8>, value: &str) {
    write_uvarint(out, value.len() as u32 + 1);
    out.extend_from_slice(value.as_bytes());
}

fn write_compact_array_i32(out: &mut Vec<u8>, values: &[i32]) {
    write_uvarint(out, values.len() as u32 + 1);
    for value in values {
        out.put_i32(*value);
    }
}

fn write_compact_array_uuid(out: &mut Vec<u8>, values: &[[u8; 16]]) {
    write_uvarint(out, values.len() as u32 + 1);
    for value in values {
        out.extend_from_slice(value);
    }
}

fn write_tagged_fields(out: &mut Vec<u8>, fields: &[TaggedField]) {
    write_uvarint(out, fields.len() as u32);
    for field in fields {
        write_uvarint(out, field.tag);
        write_uvarint(out, field.data.len() as u32);
        out.extend_from_slice(&field.data);
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f63b78 & mask);
        }
    }
    !crc
}

// ── RecordBatch ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: u8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl Decode for RecordBatch {
    /// On-disk layout:
    ///   base_offset            : i64
    ///   batch_length           : i32  (covers everything that follows)
    ///   partition_leader_epoch : i32
    ///   magic                  : u8
    ///   crc                    : i32
    ///   attributes             : i16
    ///   last_offset_delta      : i32
    ///   base_timestamp         : i64
    ///   max_timestamp          : i64
    ///   producer_id            : i64
    ///   producer_epoch         : i16
    ///   base_sequence          : i32
    ///   records_count          : i32
    ///   records                : Record[records_count]
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        ensure_remaining(cur, 12, "record_batch header")?;

        let base_offset = read_i64(cur, "record_batch.base_offset")?;
        let batch_len = read_i32(cur, "record_batch.batch_len")?;
        tracing::info!("base_offset={base_offset}, batch_len={batch_len}");

        if batch_len < 0 {
            return Err(crate::invalid_length!(
                cur,
                "record_batch.batch_len",
                batch_len
            ));
        }
        let batch_len = batch_len as usize;
        if cur.remaining() < batch_len {
            return Err(crate::truncated!(cur, "record_batch body"));
        }

        let partition_leader_epoch = read_i32(cur, "record_batch.partition_leader_epoch")?;
        let magic = read_u8(cur, "record_batch.magic")?;
        let crc = read_i32(cur, "record_batch.crc")?;
        let attributes = read_i16(cur, "record_batch.attributes")?;
        let last_offset_delta = read_i32(cur, "record_batch.last_offset_delta")?;
        let base_timestamp = read_i64(cur, "record_batch.base_timestamp")?;
        let max_timestamp = read_i64(cur, "record_batch.max_timestamp")?;
        let producer_id = read_i64(cur, "record_batch.producer_id")?;
        let producer_epoch = read_i16(cur, "record_batch.producer_epoch")?;
        let base_sequence = read_i32(cur, "record_batch.base_sequence")?;
        let records_count = read_i32(cur, "record_batch.records_count")?;

        if records_count < 0 {
            return Err(crate::invalid_length!(
                cur,
                "record_batch.records_count",
                records_count
            ));
        }

        let mut records = Vec::with_capacity(records_count as usize);
        for _ in 0..records_count {
            records.push(Record::decode(cur)?);
        }

        Ok(RecordBatch {
            base_offset,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl Encode for RecordBatch {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let mut records = Vec::new();
        for record in &self.records {
            record.encode(&mut records)?;
        }

        let mut crc_body = Vec::new();
        crc_body.put_i16(self.attributes);
        crc_body.put_i32(self.last_offset_delta);
        crc_body.put_i64(self.base_timestamp);
        crc_body.put_i64(self.max_timestamp);
        crc_body.put_i64(self.producer_id);
        crc_body.put_i16(self.producer_epoch);
        crc_body.put_i32(self.base_sequence);
        crc_body.put_i32(self.records.len() as i32);
        crc_body.extend_from_slice(&records);

        let crc = crc32c(&crc_body) as i32;

        let mut body = Vec::new();
        body.put_i32(self.partition_leader_epoch);
        body.put_u8(self.magic);
        body.put_i32(crc);
        body.extend_from_slice(&crc_body);

        out.put_i64(self.base_offset);
        out.put_i32(body.len() as i32);
        out.extend_from_slice(&body);
        Ok(())
    }
}

// ── Record ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Record {
    pub attributes: i8,
    pub key: Option<Vec<u8>>,
    pub timestamp_delta: i32,
    pub offset_delta: i32,
    pub frame_version: i8,
    pub value_type: i8,
    pub value: RecordValue,
    pub value_bytes: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

impl Decode for Record {
    /// On-disk layout:
    ///   length          : signed varint
    ///   attributes      : i8
    ///   timestamp_delta : signed varint
    ///   offset_delta    : signed varint
    ///   key_length      : signed varint  (-1 = null)
    ///   key             : bytes[key_length]
    ///   value_length    : signed varint  (-1 = null)
    ///   value           : bytes[value_length]
    ///     [0] frame_version : i8
    ///     [1] type          : i8  → dispatched to RecordValue
    ///     [2..] payload
    ///   headers_count   : unsigned varint
    ///   headers         : Header[headers_count]
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let record_len = read_varint(cur)?;
        if record_len < 0 {
            return Err(crate::invalid_length!(cur, "record.length", record_len));
        }
        if cur.remaining() < record_len as usize {
            return Err(crate::truncated!(cur, "record body"));
        }

        let attributes = read_i8(cur, "record.attributes")?;
        let timestamp_delta = read_varint(cur)?;
        let offset_delta = read_varint(cur)?;

        // key
        let key_len = read_varint(cur)?;
        let key = if key_len < 0 {
            None
        } else {
            let len = key_len as usize;
            let mut buf = vec![0u8; len];
            cur.read_exact(&mut buf)
                .map_err(|err| crate::io_err!(cur, "record.key", err))?;
            Some(buf)
        };

        // value bytes
        let val_len = read_varint(cur)?;
        let value_bytes: Option<Vec<u8>> = if val_len < 0 {
            None
        } else {
            let len = val_len as usize;
            let mut buf = vec![0u8; len];
            cur.read_exact(&mut buf)
                .map_err(|err| crate::io_err!(cur, "record.value", err))?;
            Some(buf)
        };

        // parse value payload
        let (frame_version, value_type, value) = if let Some(bytes) = &value_bytes {
            let mut vc = Cursor::new(bytes.as_slice());
            if vc.remaining() < 2 {
                (
                    0,
                    0,
                    RecordValue::Unknown {
                        frame_version: 0,
                        type_: 0,
                        payload: bytes.clone(),
                    },
                )
            } else {
                let frame_version = read_i8(&mut vc, "record.value.frame_version")?;
                let type_ = read_i8(&mut vc, "record.value.type")?;
                let record_value = match type_ {
                    FEATURE_LEVEL => {
                        let version = read_i8(&mut vc, "record.value.feature_level.version")?;
                        let name = read_compact_string(&mut vc)?;
                        let feature_level = read_i16(&mut vc, "record.value.feature_level.level")?;
                        let tag_buffer = read_tagged_fields(&mut vc)?;
                        RecordValue::FeatureLevel(FeatureLevel {
                            version,
                            name,
                            feature_level,
                            tag_buffer,
                        })
                    }
                    TOPIC_RECORD_TYPE => RecordValue::Topic(Topic::decode(&mut vc)?),
                    PARTITION_RECORD_TYPE => RecordValue::Partition(Partition::decode(&mut vc)?),
                    _ => RecordValue::Unknown {
                        frame_version,
                        type_,
                        payload: bytes[(vc.position() as usize)..].to_vec(),
                    },
                };
                (frame_version, type_, record_value)
            }
        } else {
            (
                0,
                0,
                RecordValue::Unknown {
                    frame_version: 0,
                    type_: 0,
                    payload: Vec::new(),
                },
            )
        };

        // headers
        let headers_count = read_uvarint(cur)? as usize;
        let mut headers = Vec::with_capacity(headers_count);
        for _ in 0..headers_count {
            let key = read_compact_string(cur)?;
            let val_len = read_varint(cur)?;
            let value = if val_len < 0 {
                None
            } else {
                let len = val_len as usize;
                ensure_remaining(cur, len, "record.header.value")?;
                let mut buf = vec![0u8; len];
                cur.read_exact(&mut buf)
                    .map_err(|err| crate::io_err!(cur, "record.header.value", err))?;
                Some(buf)
            };
            headers.push(Header { key, value });
        }

        Ok(Record {
            attributes,
            key,
            timestamp_delta,
            offset_delta,
            frame_version,
            value_type,
            value,
            value_bytes,
            headers,
        })
    }
}

impl Record {
    fn encoded_value_bytes(&self) -> Result<Option<Vec<u8>>, EncodeError> {
        if let Some(bytes) = &self.value_bytes {
            return Ok(Some(bytes.clone()));
        }

        match &self.value {
            RecordValue::Unknown { .. } if self.frame_version == 0 && self.value_type == 0 => {
                Ok(None)
            }
            RecordValue::Unknown {
                frame_version,
                type_,
                payload,
            } => {
                let mut buf = Vec::new();
                buf.put_i8(*frame_version);
                buf.put_i8(*type_);
                buf.extend_from_slice(payload);
                Ok(Some(buf))
            }
            value => {
                let mut buf = Vec::new();
                buf.put_i8(self.frame_version);
                buf.put_i8(self.value_type);
                value.encode(&mut buf)?;
                Ok(Some(buf))
            }
        }
    }
}

impl Encode for Record {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let value_bytes = self.encoded_value_bytes()?;

        let mut body = Vec::new();
        body.put_i8(self.attributes);
        write_varint(&mut body, self.timestamp_delta);
        write_varint(&mut body, self.offset_delta);

        match &self.key {
            Some(key) => {
                write_varint(&mut body, key.len() as i32);
                body.extend_from_slice(key);
            }
            None => write_varint(&mut body, -1),
        }

        match &value_bytes {
            Some(value_bytes) => {
                write_varint(&mut body, value_bytes.len() as i32);
                body.extend_from_slice(value_bytes);
            }
            None => write_varint(&mut body, -1),
        }

        write_uvarint(&mut body, self.headers.len() as u32);
        for header in &self.headers {
            header.encode(&mut body)?;
        }

        write_varint(out, body.len() as i32);
        out.extend_from_slice(&body);
        Ok(())
    }
}

// ── RecordValue ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum RecordValue {
    FeatureLevel(FeatureLevel),
    Topic(Topic),
    Partition(Partition),
    Unknown {
        frame_version: i8,
        type_: i8,
        payload: Vec<u8>,
    },
}

impl Encode for RecordValue {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            RecordValue::FeatureLevel(value) => value.encode(out),
            RecordValue::Topic(value) => value.encode(out),
            RecordValue::Partition(value) => value.encode(out),
            RecordValue::Unknown {
                frame_version,
                type_,
                payload,
            } => {
                if *frame_version != 0 || *type_ != 0 {
                    out.put_i8(*frame_version);
                    out.put_i8(*type_);
                }
                out.extend_from_slice(payload);
                Ok(())
            }
        }
    }
}

// ── FeatureLevel ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FeatureLevel {
    pub version: i8,
    pub name: String,
    pub feature_level: i16,
    pub tag_buffer: TaggedFields,
}

impl Encode for FeatureLevel {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.put_i8(self.version);
        write_compact_string(out, &self.name);
        out.put_i16(self.feature_level);
        write_tagged_fields(out, &self.tag_buffer);
        Ok(())
    }
}

// ── Topic ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Topic {
    pub version: i8,
    pub name: String,
    pub topic_id: [u8; 16],
    pub tag_buffer: TaggedFields,
}

impl Decode for Topic {
    /// Layout (after frame_version + type consumed):
    ///   version    : i8
    ///   name       : compact string
    ///   topic_id   : UUID (16 bytes)
    ///   tag_buffer : tagged fields
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let version = read_i8(cur, "topic.version")?;
        let name = read_compact_string(cur)?;
        let topic_id = read_uuid(cur)?;
        let tag_buffer = read_tagged_fields(cur)?;
        Ok(Topic {
            version,
            name,
            topic_id,
            tag_buffer,
        })
    }
}

impl Encode for Topic {
    /// Layout (caller writes frame_version + type first):
    ///   version    : i8
    ///   name       : compact string
    ///   topic_id   : UUID (16 bytes)
    ///   tag_buffer : uvarint count (fields encoding TBD)
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        buf.put_i8(self.version);
        write_compact_string(buf, &self.name);
        buf.put_slice(&self.topic_id);
        write_tagged_fields(buf, &self.tag_buffer);
        Ok(())
    }
}

// ── Partition ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Partition {
    pub version: i8,
    pub partition_id: i32,
    pub topic_id: [u8; 16],
    pub replicas: Vec<i32>,
    pub sync_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub leader: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub directories: Vec<[u8; 16]>, // compact array of UUIDs (version >= 1)
    pub tag_buffer: TaggedFields,
}

impl Decode for Partition {
    /// Layout (after frame_version + type consumed):
    ///   version           : i8
    ///   partition_id      : i32
    ///   topic_id          : UUID
    ///   replicas          : compact array i32
    ///   sync_replicas     : compact array i32
    ///   removing_replicas : compact array i32
    ///   adding_replicas   : compact array i32
    ///   leader            : i32
    ///   leader_epoch      : i32
    ///   partition_epoch   : i32
    ///   directories       : compact array UUID  (version >= 1 only)
    ///   tag_buffer        : tagged fields
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let version = read_i8(cur, "partition.version")?;
        let partition_id = read_i32(cur, "partition.partition_id")?;
        let topic_id = read_uuid(cur)?;
        let replicas = read_compact_array_i32(cur)?;
        let sync_replicas = read_compact_array_i32(cur)?;
        let removing_replicas = read_compact_array_i32(cur)?;
        let adding_replicas = read_compact_array_i32(cur)?;
        let leader = read_i32(cur, "partition.leader")?;
        let leader_epoch = read_i32(cur, "partition.leader_epoch")?;
        let partition_epoch = read_i32(cur, "partition.partition_epoch")?;
        let directories = if version >= 1 {
            read_compact_array_uuid(cur)?
        } else {
            Vec::new()
        };
        let tag_buffer = read_tagged_fields(cur)?;

        Ok(Partition {
            version,
            partition_id,
            topic_id,
            replicas,
            sync_replicas,
            removing_replicas,
            adding_replicas,
            leader,
            leader_epoch,
            partition_epoch,
            directories,
            tag_buffer,
        })
    }
}

impl Encode for Partition {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.put_i8(self.version);
        out.put_i32(self.partition_id);
        out.extend_from_slice(&self.topic_id);
        write_compact_array_i32(out, &self.replicas);
        write_compact_array_i32(out, &self.sync_replicas);
        write_compact_array_i32(out, &self.removing_replicas);
        write_compact_array_i32(out, &self.adding_replicas);
        out.put_i32(self.leader);
        out.put_i32(self.leader_epoch);
        out.put_i32(self.partition_epoch);
        if self.version >= 1 {
            write_compact_array_uuid(out, &self.directories);
        }
        write_tagged_fields(out, &self.tag_buffer);
        Ok(())
    }
}

// ── Header ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

impl Encode for Header {
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        write_compact_string(out, &self.key);
        match &self.value {
            Some(value) => {
                write_varint(out, value.len() as i32);
                out.extend_from_slice(value);
            }
            None => write_varint(out, -1),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_batch_round_trips_unknown_record_exactly() {
        let batch = RecordBatch {
            base_offset: 42,
            partition_leader_epoch: 3,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 1234,
            max_timestamp: 1234,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 17,
                key: Some(vec![1, 2, 3]),
                timestamp_delta: 7,
                offset_delta: 0,
                frame_version: 1,
                value_type: 99,
                value: RecordValue::Unknown {
                    frame_version: 1,
                    type_: 99,
                    payload: vec![9, 8, 7, 6],
                },
                value_bytes: Some(vec![1, 99, 9, 8, 7, 6]),
                headers: vec![Header {
                    key: "trace-id".into(),
                    value: Some(vec![0xaa, 0xbb]),
                }],
            }],
        };

        let mut encoded = Vec::new();
        batch.encode(&mut encoded).unwrap();

        let decoded = parse_metadata_image(&encoded).unwrap();
        let mut reencoded = Vec::new();
        decoded[0].encode(&mut reencoded).unwrap();

        assert_eq!(reencoded, encoded);
    }

    #[test]
    fn record_batch_round_trips_null_value_exactly() {
        let batch = RecordBatch {
            base_offset: 7,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                key: None,
                timestamp_delta: 0,
                offset_delta: 0,
                frame_version: 0,
                value_type: 0,
                value: RecordValue::Unknown {
                    frame_version: 0,
                    type_: 0,
                    payload: Vec::new(),
                },
                value_bytes: None,
                headers: vec![Header {
                    key: "empty".into(),
                    value: None,
                }],
            }],
        };

        let mut encoded = Vec::new();
        batch.encode(&mut encoded).unwrap();

        let decoded = parse_metadata_image(&encoded).unwrap();
        let mut reencoded = Vec::new();
        decoded[0].encode(&mut reencoded).unwrap();

        assert_eq!(reencoded, encoded);
    }
}
