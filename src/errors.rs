#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("truncated data while reading {field} at offset {offset} ({file}:{line})")]
    Truncated {
        field: &'static str,
        offset: usize,
        file: &'static str,
        line: u32,
    },

    #[error("invalid length for {field}: {value} at offset {offset} ({file}:{line})")]
    InvalidLength {
        field: &'static str,
        value: i64,
        offset: usize,
        file: &'static str,
        line: u32,
    },

    #[error("invalid utf-8 in {field} at offset {offset} ({file}:{line})")]
    InvalidUtf8 {
        field: &'static str,
        offset: usize,
        file: &'static str,
        line: u32,
    },

    #[error("unknown api key {api_key} at offset {offset} ({file}:{line})")]
    UnknownApiKey {
        api_key: i16,
        offset: usize,
        file: &'static str,
        line: u32,
    },

    #[error("i/o error while reading {field} at offset {offset} ({file}:{line}): {source}")]
    Io {
        field: &'static str,
        offset: usize,
        file: &'static str,
        line: u32,
        #[source]
        source: std::io::Error,
    },
}

#[macro_export]
macro_rules! truncated {
    ($cur:expr, $field:expr) => {
        $crate::errors::DecodeError::Truncated {
            field: $field,
            offset: $cur.position() as usize,
            file: file!(),
            line: line!(),
        }
    };
}

#[macro_export]
macro_rules! invalid_length {
    ($cur:expr, $field:expr, $value:expr) => {
        $crate::errors::DecodeError::InvalidLength {
            field: $field,
            value: $value as i64,
            offset: $cur.position() as usize,
            file: file!(),
            line: line!(),
        }
    };
}

#[macro_export]
macro_rules! invalid_utf8 {
    ($cur:expr, $field:expr) => {
        $crate::errors::DecodeError::InvalidUtf8 {
            field: $field,
            offset: $cur.position() as usize,
            file: file!(),
            line: line!(),
        }
    };
}

#[macro_export]
macro_rules! unknown_api_key {
    ($cur:expr, $api_key:expr) => {
        $crate::errors::DecodeError::UnknownApiKey {
            api_key: $api_key,
            offset: $cur.position() as usize,
            file: file!(),
            line: line!(),
        }
    };
}

#[macro_export]
macro_rules! io_err {
    ($cur:expr, $field:expr, $source:expr) => {
        $crate::errors::DecodeError::Io {
            field: $field,
            offset: $cur.position() as usize,
            file: file!(),
            line: line!(),
            source: $source,
        }
    };
}
