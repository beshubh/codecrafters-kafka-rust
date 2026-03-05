use bytes::Buf;
use std::io::Cursor;
use thiserror::Error;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("truncated data")]
    Truncated,
    #[error("invalid length")]
    InvalidLength,
    #[error("invalid utf-8")]
    InvalidUtf8,
    #[error("unknown api key: {0}")]
    UnknownApiKey(i16),
    /// Catch-all for I/O errors (e.g. `read_exact` failures).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

// ── Varint primitives ─────────────────────────────────────────────────────────

/// Reads an unsigned varint (base-128, little-endian) from `cur`.
pub fn read_uvarint(cur: &mut Cursor<&[u8]>) -> Result<u32, DecodeError> {
    let mut value: u32 = 0;
    let mut shift = 0u32;

    loop {
        if cur.remaining() < 1 {
            return Err(DecodeError::Truncated);
        }
        let byte = cur.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 28 {
            return Err(DecodeError::InvalidLength);
        }
    }
}

/// Reads a signed varint (zigzag-encoded) from `cur`.
pub fn read_varint(cur: &mut Cursor<&[u8]>) -> Result<i32, DecodeError> {
    let raw = read_uvarint(cur)?;
    Ok(((raw >> 1) as i32) ^ (-((raw & 1) as i32)))
}

/// Writes an unsigned varint to `out`.
pub fn write_uvarint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

// ── Compound read helpers ─────────────────────────────────────────────────────

/// Reads a 16-byte UUID.
pub fn read_uuid(cur: &mut Cursor<&[u8]>) -> Result<[u8; 16], DecodeError> {
    if cur.remaining() < 16 {
        return Err(DecodeError::Truncated);
    }
    let mut uuid = [0u8; 16];
    cur.copy_to_slice(&mut uuid);
    Ok(uuid)
}

/// Reads a Kafka *compact string* (uvarint length+1, then UTF-8 bytes).
pub fn read_compact_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Err(DecodeError::InvalidLength);
    }
    let len = (len_plus_one - 1) as usize;
    if cur.remaining() < len {
        return Err(DecodeError::Truncated);
    }
    let mut bytes = vec![0u8; len];
    cur.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)
}

/// Reads a Kafka *compact nullable string*
pub fn read_compact_nullable_string(
    cur: &mut Cursor<&[u8]>,
) -> Result<Option<String>, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Ok(None);
    }
    let len = (len_plus_one - 1) as usize;
    if cur.remaining() < len {
        return Err(DecodeError::Truncated);
    }
    let mut bytes = vec![0u8; len];
    cur.copy_to_slice(&mut bytes);
    Ok(Some(
        String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?,
    ))
}

/// Reads a Kafka *compact array* length (uvarint − 1).
/// Returns the number of elements, or `Err(InvalidLength)` for a null array (varint == 0).
pub fn read_compact_array_len(cur: &mut Cursor<&[u8]>) -> Result<usize, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        // null array — treated as empty in flexible versions
        return Ok(0);
    }
    Ok((len_plus_one - 1) as usize)
}

/// Reads a Kafka *compact array* of `i32` values.
pub fn read_compact_array_i32(cur: &mut Cursor<&[u8]>) -> Result<Vec<i32>, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Ok(Vec::new());
    }
    let len = (len_plus_one - 1) as usize;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        if cur.remaining() < 4 {
            return Err(DecodeError::Truncated);
        }
        out.push(cur.get_i32());
    }
    Ok(out)
}

/// Reads a Kafka *compact array* of 16-byte UUIDs.
pub fn read_compact_array_uuid(cur: &mut Cursor<&[u8]>) -> Result<Vec<[u8; 16]>, DecodeError> {
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

/// Tagged field carried inside a Kafka record value.
#[derive(Debug, Clone)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Vec<u8>,
}

pub type TaggedFields = Vec<TaggedField>;

/// Reads a Kafka tagged-field section.
pub fn read_tagged_fields(cur: &mut Cursor<&[u8]>) -> Result<TaggedFields, DecodeError> {
    let count = read_uvarint(cur)? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = read_uvarint(cur)?;
        let size = read_uvarint(cur)? as usize;
        if cur.remaining() < size {
            return Err(DecodeError::Truncated);
        }
        let mut data = vec![0u8; size];
        cur.copy_to_slice(&mut data);
        fields.push(TaggedField { tag, data });
    }
    Ok(fields)
}

// ── TagBuffer (wire-level) ────────────────────────────────────────────────────

/// A Kafka *flexible version* tag buffer on the wire.
///
/// In the current implementation this is a skip-only placeholder:
/// decoding reads and discards any tagged fields; encoding writes a
/// single `0x00` byte ("zero tagged fields").
#[derive(Debug, Clone, Default)]
pub struct TagBuffer;

impl TagBuffer {
    /// Decode a tag buffer from the cursor, skipping all fields.
    pub fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        // The count is a uvarint; skip each field's tag + size + data.
        let count = read_uvarint(cur)? as usize;
        for _ in 0..count {
            let _tag = read_uvarint(cur)?;
            let size = read_uvarint(cur)? as usize;
            if cur.remaining() < size {
                return Err(DecodeError::Truncated);
            }
            cur.advance(size);
        }
        Ok(TagBuffer)
    }

    /// Encode an empty tag buffer (single zero byte).
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(0);
    }
}
