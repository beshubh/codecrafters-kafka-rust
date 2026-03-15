#![allow(unused_imports)]
use anyhow::{Context, Result};
use bytes::Buf;
use std::{
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, trace};
use tracing_subscriber::EnvFilter;

mod apis;
mod binary;
mod errors;
mod kraft;
mod router;
mod storage;
mod wire;

use router::{handle_request, RequestContext};
use storage::query_engine::QueryEngine;
use storage::ClusterMetadata;
use wire::{Decode, ReqMessage};

fn main() -> Result<()> {
    init_tracing();
    info!("starting kafka server");

    let cluster_metadata =
        ClusterMetadata::load_shared().context("failed to load cluster metadata")?;

    let listener = TcpListener::bind("127.0.0.1:9092")
        .context("failed to bind TCP listener on 127.0.0.1:9092")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!(peer = ?stream.peer_addr().ok(), "accepted new connection");
                let cluster_metadata = cluster_metadata.clone();
                std::thread::spawn(move || {
                    if let Err(err) = handle_client(stream, cluster_metadata) {
                        println!("client handler faield: {:#}", err);
                    }
                });
            }
            Err(e) => {
                error!(error = ?e, "failed to accept incoming connection");
            }
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    cluster_metadata: storage::SharedClusterMetadata,
) -> Result<()> {
    let mut query_engine =
        QueryEngine::init(cluster_metadata.clone()).context("failed to initialize query engine")?;

    loop {
        let mut buf = [0u8; 1024];
        let n = stream
            .read(&mut buf)
            .context("failed reading from client stream")?;

        if n == 0 {
            debug!("client disconnected");
            return Ok(());
        }

        trace!(bytes_read = n, "received request bytes");

        let mut cur = Cursor::new(&buf[..n]);
        let request = ReqMessage::decode(&mut cur).map_err(anyhow::Error::new)?;
        debug!(
            bytes_read = n,
            decoded_bytes = cur.position(),
            remaining_bytes = cur.remaining(),
            "decoded request"
        );

        let response = handle_request(RequestContext::from_req_message(
            request,
            cluster_metadata.clone(),
            &mut query_engine,
        ))
        .context("failed to handle request")?;
        let response_bytes = response
            .to_bytes()
            .map_err(|err| anyhow::anyhow!("failed to encode response: {err:?}"))?;
        stream
            .write_all(&response_bytes)
            .context("failed writing response to client stream")?;
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
