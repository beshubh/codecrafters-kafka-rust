

pub struct Message {
    pub message_size: u32,
    pub header: u32
}

impl Message {

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&self.message_size.to_be_bytes());
        buf.extend_from_slice(&self.header.to_be_bytes());
        buf
    }
}
