

pub struct ReqHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    // pub client_id: String,
    // pub tag_buffer: Vec<u8>,
}

impl ReqHeader {
    // TODO: add variable length and string support
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&self.request_api_key.to_be_bytes());
        buf.extend_from_slice(&self.request_api_version.to_be_bytes());
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
        // buf.extend_from_slice(self.client_id.as_bytes());
        // buf.extend_from_slice(&self.tag_buffer);
        buf
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut request_api_key_bytes: [u8; 2] = [0; 2];
        let mut request_api_version_bytes: [u8; 2] = [0; 2];

        let mut correlation_id_bytes: [u8; 4] = [0; 4];

        // let mut client_id_bytes: [u8; 64] = [0; 64];
        // let mut tag_buffer_bytes: [u8; 64] = [0; 64];

        request_api_key_bytes.copy_from_slice(&bytes[0..2]);
        request_api_version_bytes.copy_from_slice(&bytes[2..4]);
        correlation_id_bytes.copy_from_slice(&bytes[4..8]);
        // client_id_bytes.copy_from_slice(&bytes[8..72]);
        // tag_buffer_bytes.copy_from_slice(&bytes[72..]);

        Self {
            request_api_key: i16::from_be_bytes(request_api_key_bytes),
            request_api_version: i16::from_be_bytes(request_api_version_bytes),
            correlation_id: i32::from_be_bytes(correlation_id_bytes),
            // client_id: String::from_utf8(client_id_bytes.to_vec()).unwrap(),
            // tag_buffer: tag_buffer_bytes.to_vec(),
        }
    }
}

pub struct ReqMessage {
    pub message_size: u32,
    pub header: ReqHeader,
}

impl ReqMessage {

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&self.message_size.to_be_bytes());
        buf.extend_from_slice(&&self.header.to_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut message_size_bytes: [u8; 4] = [0; 4];
        let header = ReqHeader::from_bytes(&bytes[4..]);
        message_size_bytes.copy_from_slice(&bytes[0..4]);
        Self {
            message_size: u32::from_be_bytes(message_size_bytes),
            header: header,
        }
    }
}


pub struct ResHeader {
    pub correlation_id: i32
}



impl ResHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
        buf
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut correlation_id_bytes: [u8; 4] = [0; 4];
        correlation_id_bytes.copy_from_slice(&bytes[0..4]);
        Self {
            correlation_id: i32::from_be_bytes(correlation_id_bytes),
        }
    }
}

pub struct ResMessage {
    pub message_size: u32,
    pub header: ResHeader,
}

impl ResMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&self.message_size.to_be_bytes());
        buf.extend_from_slice(&&self.header.to_bytes());
        buf
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let message_size_bytes: [u8; 4] = [0; 4];
        let header = ResHeader::from_bytes(&bytes[4..]);
        Self {
            message_size: u32::from_be_bytes(message_size_bytes),
            header: header,
        }
    }
}
