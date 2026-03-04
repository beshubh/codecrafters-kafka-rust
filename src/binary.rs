use anyhow::{Error, Result};
use bytes::Buf;
use std::{error, io::Cursor};

pub fn read_uvarint(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value: u32 = 0;
    let mut shift = 0;

    loop {
        if cur.remaining() < 1 {
            return Err(anyhow::anyhow!("not enough bytes to read"));
        }
        let byte = cur.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }

        shift += 7;
        if shift > 28 {
            return Err(anyhow::anyhow!("varint is too long"));
        }
    }
}

pub fn read_varint(cur: &mut Cursor<&[u8]>) -> Result<i32> {
    let raw = read_uvarint(cur)?;
    Ok(((raw >> 1) as i32) ^ (-((raw & 1) as i32)))
}

pub fn write_uvarint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
