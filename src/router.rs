use crate::apis::{self, ReqBody, ResBody};
use crate::wire::{self, ReqMessage, ResHeader, ResMessage};

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
    match &ctx.body {
        ReqBody::ApiVersions(request) => {
            let response = apis::api_versions::handle(request, ctx.api_version);
            let body = ResBody::ApiVersions(response);
            ResMessage {
                header: ResHeader::v0(ctx.correlation_id),
                body,
            }
        }
        ReqBody::DescribeTopics(request) => {
            let response = apis::describe_topic_partitions::handle(&request, ctx.api_version);
            let body = ResBody::DescribeTopics(response);
            ResMessage {
                header: ResHeader::v1(ctx.correlation_id, wire::TagBuffer),
                body,
            }
        }
        _ => {
            unimplemented!()
        }
    }
}
