use bytes::Buf;
use std::io::Cursor;

use crate::wire::{Decode, DecodeError, Encode, EncodeError};

pub struct FetchRequest {
    pub topic_name: String,
    pub partition: i32,
    pub offset: i64,
    pub max_bytes: i32,
}
