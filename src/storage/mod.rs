use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tracing::{debug, trace};

use crate::binary;
use crate::binary::{read_uvarint, read_varint, write_uvarint};

const TOPIC_RECORD_TYPE: i8 = 2;
const PARTITION_RECORD_TYPE: i8 = 3;
const FEATURE_LEVEL: i8 = 12;

#[derive(Debug, Clone)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Vec<u8>,
}

pub type TaggedFields = Vec<TaggedField>;

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub eligible_leader_replicas: Vec<i32>,
    pub last_known_elr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
    pub tag_buffer: TaggedFields,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
    pub topic_authorized_operations: i32,
    pub tag_buffer: TaggedFields,
}

#[derive(Debug, Clone)]
pub struct MetadataImage {
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("i/o error: {0}")]
    Io(std::io::Error),
    #[error("truncated data")]
    Truncated(String),
    #[error("invalid length")]
    InvalidLength,
    #[error("invalid utf-8")]
    InvalidUtf8,
    #[error("{0}")]
    Other(String),
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<anyhow::Error> for StorageError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value.to_string())
    }
}

/// Deserialize a value from a cursor over raw bytes.
pub trait Decode: Sized {
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, StorageError>;
}

/// Serialize a value into a `BytesMut` buffer.
pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

#[derive(Debug, Clone)]
struct TopicState {
    topic_name: Option<String>,
    topic_id: [u8; 16],
    is_internal: bool,
    topic_tag_buffer: TaggedFields,
    partitions: HashMap<i32, PartitionMetadata>,
}

impl TopicState {
    fn new(topic_id: [u8; 16]) -> Self {
        Self {
            topic_name: None,
            topic_id,
            is_internal: false,
            topic_tag_buffer: Vec::new(),
            partitions: HashMap::new(),
        }
    }
}

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

fn parse_metadata_image(bytes: &[u8]) -> Result<Vec<RecordBatch>, StorageError> {
    let mut cur = Cursor::new(bytes);
    let mut batch_records = vec![];

    while cur.remaining() >= 12 {
        // 12 = base_offset (8) + batch_length (4) minimum
        let batch = RecordBatch::decode(&mut cur)?;
        batch_records.push(batch);
    }
    Ok(batch_records)
}

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
    /// Decodes one full record batch from the cursor.
    ///
    /// On-disk layout:
    ///   base_offset              : i64
    ///   batch_length             : i32   (covers everything that follows)
    ///   partition_leader_epoch   : i32
    ///   magic                    : u8
    ///   crc                      : i32
    ///   attributes               : i16
    ///   last_offset_delta        : i32
    ///   base_timestamp           : i64
    ///   max_timestamp            : i64
    ///   producer_id              : i64
    ///   producer_epoch           : i16
    ///   base_sequence            : i32
    ///   records_count            : i32
    ///   records                  : Record[records_count]
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, StorageError> {
        if cur.remaining() < 12 {
            return Err(StorageError::Truncated(format!(
                "not enough bytes for batch header: remaining={}",
                cur.remaining()
            )));
        }

        let base_offset = cur.get_i64();
        let batch_len = cur.get_i32();
        tracing::info!("base_offset={base_offset}, batch_len={batch_len}");

        if batch_len < 0 {
            return Err(StorageError::InvalidLength);
        }
        let batch_len = batch_len as usize;
        if cur.remaining() < batch_len {
            return Err(StorageError::Truncated(format!(
                "batch body truncated: need={batch_len} remaining={}",
                cur.remaining()
            )));
        }

        let partition_leader_epoch = cur.get_i32();
        let magic = cur.get_u8();
        let crc = cur.get_i32();
        let attributes = cur.get_i16();
        let last_offset_delta = cur.get_i32();
        let base_timestamp = cur.get_i64();
        let max_timestamp = cur.get_i64();
        let producer_id = cur.get_i64();
        let producer_epoch = cur.get_i16();
        let base_sequence = cur.get_i32();
        let records_count = cur.get_i32();

        if records_count < 0 {
            return Err(StorageError::InvalidLength);
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
    /// Decodes one record from the cursor.
    ///
    /// On-disk layout:
    ///   length           : signed varint  (total bytes that follow *this* length field)
    ///   attributes       : i8
    ///   timestamp_delta  : signed varint
    ///   offset_delta     : signed varint
    ///   key_length       : signed varint  (-1 = null)
    ///   key              : bytes[key_length]
    ///   value_length     : signed varint  (-1 = null)
    ///   value            : bytes[value_length]
    ///     value[0]       : frame_version (i8)
    ///     value[1]       : type (i8)  →  dispatched to RecordValue
    ///     value[2..]     : type-specific payload
    ///   headers_count    : unsigned varint
    ///   headers          : Header[headers_count]  (skipped for now)
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, StorageError> {
        // length varint (we validate but don't slice — we trust the fields)
        let record_len = read_varint(cur)?;
        if record_len < 0 {
            return Err(StorageError::InvalidLength);
        }
        if cur.remaining() < record_len as usize {
            return Err(StorageError::Truncated(format!(
                "record body truncated: need={record_len} remaining={}",
                cur.remaining()
            )));
        }

        let attributes = cur.get_i8();
        let timestamp_delta = read_varint(cur)?;
        let offset_delta = read_varint(cur)?;

        // key
        let key_len = read_varint(cur)?;
        let key = if key_len < 0 {
            None
        } else {
            let len = key_len as usize;
            let mut buf = vec![0u8; len];
            cur.read_exact(&mut buf)?;
            Some(buf)
        };

        // value bytes
        let val_len = read_varint(cur)?;
        let value_bytes: Option<Vec<u8>> = if val_len < 0 {
            None
        } else {
            let len = val_len as usize;
            let mut buf = vec![0u8; len];
            cur.read_exact(&mut buf)?;
            Some(buf)
        };

        // parse the value payload
        let (frame_version, value_type, value) = if let Some(bytes) = value_bytes {
            let mut vc = Cursor::new(bytes.as_slice());
            let frame_version = vc.get_i8();
            let type_ = vc.get_i8();
            let record_value = match type_ {
                FEATURE_LEVEL => {
                    let version = vc.get_i8();
                    let name = read_compact_string(&mut vc)?;
                    let feature_level = vc.get_i16();
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
                _ => {
                    // unknown type — treat as opaque; drain remaining bytes
                    RecordValue::Unknown {
                        frame_version,
                        type_,
                    }
                }
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
            // each header: key (compact string) + value (compact bytes)
            // For now we just skip them by reading past the bytes.
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

#[derive(Debug, Clone)]
pub enum RecordValue {
    FeatureLevel(FeatureLevel),
    Topic(Topic),
    Partition(Partition),
    Unknown { frame_version: i8, type_: i8 },
}

#[derive(Debug, Clone)]
pub struct FeatureLevel {
    pub version: i8,
    pub name: String,
    pub feature_level: i16,
    pub tag_buffer: TaggedFields,
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub version: i8,
    pub name: String,
    pub topic_id: [u8; 16],
    pub tag_buffer: TaggedFields,
}

impl Decode for Topic {
    /// Decodes a Topic record value from a cursor.
    ///
    /// Expected layout (after `frame_version` and `type` have already been
    /// consumed by the caller):
    ///   version      : i8
    ///   name         : compact string  (uvarint len+1, then UTF-8 bytes)
    ///   topic_id     : 16-byte UUID
    ///   tag_buffer   : tagged fields
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, StorageError> {
        let version = cur.get_i8();
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
    /// Encodes a Topic record value into `buf`.
    ///
    /// Written layout (caller is responsible for writing `frame_version` and
    /// `type` before calling this):
    ///   version      : i8
    ///   name         : compact string  (uvarint len+1, then UTF-8 bytes)
    ///   topic_id     : 16-byte UUID
    ///   tag_buffer   : tagged fields  (uvarint count, then tag+size+data per field)
    fn encode(&self, buf: &mut Vec<u8>) {
        // version
        buf.put_i8(self.version);

        // compact string: length+1 as uvarint, then bytes
        let name_bytes = self.name.as_bytes();
        write_uvarint(buf, name_bytes.len() as u32 + 1);
        buf.put_slice(name_bytes);

        // topic_id (UUID, 16 bytes)
        buf.put_slice(&self.topic_id);

        // tagged fields
        write_uvarint(buf, self.tag_buffer.len() as u32);
        // NOTE: skipping for now.
        // for field in &self.tag_buffer {
        //     write_uvarint(buf, field.tag as u32);
        //     write_uvarint(buf, field.data.len() as u32);
        //     buf.put_slice(&field.data);
        // }
    }
}

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
    /// Decodes a Partition record value from a cursor.
    ///
    /// Expected layout (after `frame_version` and `type` have already been
    /// consumed by the caller):
    ///   version           : i8
    ///   partition_id      : i32
    ///   topic_id          : 16-byte UUID
    ///   replicas          : compact array of i32
    ///   sync_replicas     : compact array of i32
    ///   removing_replicas : compact array of i32
    ///   adding_replicas   : compact array of i32
    ///   leader            : i32
    ///   leader_epoch      : i32
    ///   partition_epoch   : i32
    ///   directories       : compact array of UUID  (present when version >= 1)
    ///   tag_buffer        : tagged fields
    fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, StorageError> {
        let version = cur.get_i8();
        let partition_id = cur.get_i32();
        let topic_id = read_uuid(cur)?;
        let replicas = read_compact_array_i32(cur)?;
        let sync_replicas = read_compact_array_i32(cur)?;
        let removing_replicas = read_compact_array_i32(cur)?;
        let adding_replicas = read_compact_array_i32(cur)?;
        let leader = cur.get_i32();
        let leader_epoch = cur.get_i32();
        let partition_epoch = cur.get_i32();
        // directories field was added in version 1
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

#[derive(Debug, Clone)]
pub struct Header;

fn read_uuid(cur: &mut Cursor<&[u8]>) -> Result<[u8; 16], StorageError> {
    if cur.remaining() < 16 {
        return Err(StorageError::Truncated(
            "cur does not have enough bytes for uuid".into(),
        ));
    }
    let mut uuid = [0u8; 16];
    cur.copy_to_slice(&mut uuid);
    Ok(uuid)
}

fn read_compact_string(cur: &mut Cursor<&[u8]>) -> Result<String, StorageError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Err(StorageError::InvalidLength);
    }
    let len = (len_plus_one - 1) as usize;
    if cur.remaining() < len {
        return Err(StorageError::Truncated(
            "cur does not have enough bytes".into(),
        ));
    }

    let mut bytes = vec![0u8; len];
    cur.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|_| StorageError::InvalidUtf8)
}

fn read_compact_array_i32(cur: &mut Cursor<&[u8]>) -> Result<Vec<i32>, StorageError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Ok(Vec::new());
    }

    let len = (len_plus_one - 1) as usize;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        if cur.remaining() < 4 {
            return Err(StorageError::Truncated(
                "not enough bytes to decode i32 in compact array".into(),
            ));
        }
        out.push(cur.get_i32());
    }
    Ok(out)
}

fn read_compact_array_uuid(cur: &mut Cursor<&[u8]>) -> Result<Vec<[u8; 16]>, StorageError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Ok(Vec::new());
    }

    let len = (len_plus_one - 1) as usize;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(read_uuid(cur)?);
    }
    Ok(out)
}

fn read_tagged_fields(cur: &mut Cursor<&[u8]>) -> Result<TaggedFields, StorageError> {
    let tagged_field_count = read_uvarint(cur)? as usize;
    let mut fields = Vec::with_capacity(tagged_field_count);
    for _ in 0..tagged_field_count {
        let tag = read_uvarint(cur)?;
        let size = read_uvarint(cur)? as usize;
        if cur.remaining() < size {
            return Err(StorageError::Truncated(
                "not enough bytes for tagged field data".into(),
            ));
        }
        let mut data = vec![0u8; size];
        cur.copy_to_slice(&mut data);
        fields.push(TaggedField { tag, data });
    }
    Ok(fields)
}
