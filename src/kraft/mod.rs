use std::fs;
use std::io::Cursor;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Context;
use bytes::{Buf, BufMut};
use tracing::{debug, trace};

use crate::binary::{
    ensure_remaining, read_compact_array_i32, read_compact_array_uuid, read_compact_string,
    read_i16, read_i32, read_i64, read_i8, read_tagged_fields, read_u8, read_uuid, read_uvarint,
    read_varint, write_uvarint, DecodeError, TaggedField, TaggedFields,
};

const TOPIC_RECORD_TYPE: i8 = 2;
const PARTITION_RECORD_TYPE: i8 = 3;
const FEATURE_LEVEL: i8 = 12;

// ── Decode / Encode traits ────────────────────────────────────────────────────

trait Decode: Sized {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError>;
}

trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

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
        let (frame_version, value_type, value) = if let Some(bytes) = value_bytes {
            let mut vc = Cursor::new(bytes.as_slice());
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
                },
            };
            (frame_version, type_, record_value)
        } else {
            (
                0,
                0,
                RecordValue::Unknown {
                    frame_version: 0,
                    type_: 0,
                },
            )
        };

        // headers
        let headers_count = read_uvarint(cur)? as usize;
        let mut headers = Vec::with_capacity(headers_count);
        for _ in 0..headers_count {
            let _key = read_compact_string(cur)?;
            let val_len = read_varint(cur)?;
            if val_len > 0 {
                cur.advance(val_len as usize);
            }
            headers.push(Header);
        }

        Ok(Record {
            attributes,
            key,
            timestamp_delta,
            offset_delta,
            frame_version,
            value_type,
            value,
            headers,
        })
    }
}

// ── RecordValue ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum RecordValue {
    FeatureLevel(FeatureLevel),
    Topic(Topic),
    Partition(Partition),
    Unknown { frame_version: i8, type_: i8 },
}

// ── FeatureLevel ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FeatureLevel {
    pub version: i8,
    pub name: String,
    pub feature_level: i16,
    pub tag_buffer: TaggedFields,
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
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_i8(self.version);
        let name_bytes = self.name.as_bytes();
        write_uvarint(buf, name_bytes.len() as u32 + 1);
        buf.put_slice(name_bytes);
        buf.put_slice(&self.topic_id);
        write_uvarint(buf, self.tag_buffer.len() as u32);
        // individual tagged fields encoding omitted for now
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

// ── Header ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Header;
