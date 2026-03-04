use crate::apis::{self, ReqBody, ResBody};
use crate::wire::{ReqMessage, ResHeader, ResMessage};

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub body: ReqBody,
}

impl From<ReqMessage> for RequestContext {
    fn from(message: ReqMessage) -> Self {
        Self {
            api_key: message.header.request_api_key,
            api_version: message.header.request_api_version,
            correlation_id: message.header.correlation_id,
            body: message.body,
        }
    }
}

pub fn handle_request(ctx: RequestContext) -> ResMessage {
    let body = match &ctx.body {
        ReqBody::ApiVersions(request) => {
            let response = apis::api_versions::handle(request, ctx.api_version);
            ResBody::ApiVersions(response)
        }
    };

    ResMessage {
        api_key: ctx.api_key,
        header: ResHeader {
            correlation_id: ctx.correlation_id,
        },
        body,
    }
}
