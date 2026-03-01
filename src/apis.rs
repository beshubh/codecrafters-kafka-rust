use crate::wire::{ReqHeader, ReqMessage, ResBody, ResHeader, ResMessage};


pub const API_VERSION: i16 = 18;
pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const OFFSET_FETCH: i16 = 9;

#[derive(Debug, Clone)]
pub struct ApiVersionsHandler {
    pub request: ReqMessage,
}

pub trait ApiHandler {

    fn handle(&self) -> ResMessage;
}

impl ApiVersionsHandler {
    pub fn new(request: ReqMessage) -> Self {
        Self { request }
    }
}

impl ApiHandler for ApiVersionsHandler {
    fn handle(&self) -> ResMessage {
        if self.request.header.request_api_version < 0 || self.request.header.request_api_version > 4 {
            println!("request api verions: {:?}", self.request.header.request_api_version);
            return ResMessage {
                message_size: 0,
                header: ResHeader { correlation_id: self.request.header.correlation_id },
                body: ResBody { error_code: 35 }
            }
        }
        ResMessage {
            message_size: self.request.message_size,
            header: ResHeader {
                correlation_id: self.request.header.correlation_id,
            },
            body: ResBody::default()
        }
    }
}
