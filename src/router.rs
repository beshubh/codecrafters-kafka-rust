use anyhow::Result;

use crate::apis::{self, ReqBody, ResBody};
use crate::storage::query_engine::QueryEngine;
use crate::storage::SharedClusterMetadata;
use crate::wire::{self, ReqMessage, ResHeader, ResMessage};

pub struct RequestContext<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub body: ReqBody,
    pub cluster_metadata: SharedClusterMetadata,
    pub query_engine: &'a mut QueryEngine,
}

impl<'a> RequestContext<'a> {
    pub fn from_req_message(
        message: ReqMessage,
        cluster_metadata: SharedClusterMetadata,
        query_engine: &'a mut QueryEngine,
    ) -> Self {
        Self {
            api_key: message.header.request_api_key,
            api_version: message.header.request_api_version,
            correlation_id: message.header.correlation_id,
            body: message.body,
            cluster_metadata,
            query_engine,
        }
    }
}

pub fn handle_request(mut ctx: RequestContext) -> Result<ResMessage> {
    match &ctx.body.clone() {
        ReqBody::ApiVersions(request) => {
            let response = apis::api_versions::handle(request, &ctx);
            let body = ResBody::ApiVersions(response);
            Ok(ResMessage {
                header: ResHeader::v0(ctx.correlation_id),
                body,
            })
        }
        ReqBody::DescribeTopics(request) => {
            let response = apis::describe_topic_partitions::handle(request, &ctx)?;
            let body = ResBody::DescribeTopics(response);
            Ok(ResMessage {
                header: ResHeader::v1(ctx.correlation_id, wire::TagBuffer),
                body,
            })
        }
        ReqBody::Fetch(request) => {
            let response = apis::fetch::handle(request, &mut ctx)?;
            let body = ResBody::Fetch(response);
            Ok(ResMessage {
                header: ResHeader::v1(ctx.correlation_id, wire::TagBuffer),
                body,
            })
        }
    }
}
