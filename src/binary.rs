use bytes::Buf;
use std::io::Cursor;

pub use crate::errors::DecodeError;

pub fn ensure_remaining(
    cur: &Cursor<&[u8]>,
    needed: usize,
    field: &'static str,
) -> Result<(), DecodeError> {
    if cur.remaining() < needed {
        return Err(crate::truncated!(cur, field));
    }
    Ok(())
}

pub fn read_u8(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<u8, DecodeError> {
    ensure_remaining(cur, 1, field)?;
    Ok(cur.get_u8())
}

pub fn read_i8(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<i8, DecodeError> {
    ensure_remaining(cur, 1, field)?;
    Ok(cur.get_i8())
}

pub fn read_u16(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<u16, DecodeError> {
    ensure_remaining(cur, 2, field)?;
    Ok(cur.get_u16())
}

pub fn read_i16(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<i16, DecodeError> {
    ensure_remaining(cur, 2, field)?;
    Ok(cur.get_i16())
}

pub fn read_u32(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<u32, DecodeError> {
    ensure_remaining(cur, 4, field)?;
    Ok(cur.get_u32())
}

pub fn read_i32(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<i32, DecodeError> {
    ensure_remaining(cur, 4, field)?;
    Ok(cur.get_i32())
}

pub fn read_i64(cur: &mut Cursor<&[u8]>, field: &'static str) -> Result<i64, DecodeError> {
    ensure_remaining(cur, 8, field)?;
    Ok(cur.get_i64())
}

/// Reads an unsigned varint (base-128, little-endian) from `cur`.
pub fn read_uvarint(cur: &mut Cursor<&[u8]>) -> Result<u32, DecodeError> {
    let mut value: u32 = 0;
    let mut shift = 0u32;

    loop {
        if cur.remaining() < 1 {
            return Err(crate::truncated!(cur, "uvarint byte"));
        }

        let byte = cur.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;

        if (byte & 0x80) == 0 {
            return Ok(value);
        }

        shift += 7;
        if shift > 28 {
            return Err(crate::invalid_length!(cur, "uvarint", value));
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

/// Reads a 16-byte UUID.
pub fn read_uuid(cur: &mut Cursor<&[u8]>) -> Result<[u8; 16], DecodeError> {
    ensure_remaining(cur, 16, "uuid")?;
    let mut uuid = [0u8; 16];
    cur.copy_to_slice(&mut uuid);
    Ok(uuid)
}

/// Reads a Kafka *compact string* (uvarint length+1, then UTF-8 bytes).
pub fn read_compact_string(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Err(crate::invalid_length!(
            cur,
            "compact string length",
            len_plus_one
        ));
    }

    let len = (len_plus_one - 1) as usize;
    if cur.remaining() < len {
        return Err(crate::truncated!(cur, "compact string bytes"));
    }

    let mut bytes = vec![0u8; len];
    cur.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|_| crate::invalid_utf8!(cur, "compact string"))
}

/// Reads a Kafka *compact nullable string*.
pub fn read_compact_nullable_string(
    cur: &mut Cursor<&[u8]>,
) -> Result<Option<String>, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
        return Ok(None);
    }

    let len = (len_plus_one - 1) as usize;
    if cur.remaining() < len {
        return Err(crate::truncated!(cur, "compact nullable string bytes"));
    }

    let mut bytes = vec![0u8; len];
    cur.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| crate::invalid_utf8!(cur, "compact nullable string"))
}

/// Reads a Kafka *compact array* length (uvarint - 1).
pub fn read_compact_array_len(cur: &mut Cursor<&[u8]>) -> Result<usize, DecodeError> {
    let len_plus_one = read_uvarint(cur)?;
    if len_plus_one == 0 {
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
        out.push(read_i32(cur, "compact array i32 element")?);
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
            return Err(crate::truncated!(cur, "tagged field data"));
        }

        let mut data = vec![0u8; size];
        cur.copy_to_slice(&mut data);
        fields.push(TaggedField { tag, data });
    }

    Ok(fields)
}

#[derive(Debug, Clone, Default)]
pub struct TagBuffer;

impl TagBuffer {
    pub fn decode(cur: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let count = read_uvarint(cur)? as usize;
        for _ in 0..count {
            let _tag = read_uvarint(cur)?;
            let size = read_uvarint(cur)? as usize;
            if cur.remaining() < size {
                return Err(crate::truncated!(cur, "tag buffer field data"));
            }
            cur.advance(size);
        }
        Ok(TagBuffer)
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(0);
    }
}
