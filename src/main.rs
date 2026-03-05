#![allow(unused_imports)]
use anyhow::{Context, Result};
use std::{
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, trace};
use tracing_subscriber::EnvFilter;

mod apis;
mod binary;
mod kraft;
mod router;
mod wire;

use router::{RequestContext, handle_request};
use wire::{Decode, ReqMessage};

fn main() -> Result<()> {
    init_tracing();
    info!("starting kafka server");

    let listener = TcpListener::bind("127.0.0.1:9092")
        .context("failed to bind TCP listener on 127.0.0.1:9092")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!(peer = ?stream.peer_addr().ok(), "accepted new connection");
                std::thread::spawn(move || {
                    if let Err(err) = handle_client(stream) {
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

fn handle_client(mut stream: TcpStream) -> Result<()> {
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
        let request = ReqMessage::decode(&mut cur)
            .map_err(|err| anyhow::anyhow!("failed to decode request: {err:?}"))?;

        let response = handle_request(RequestContext::from(request));
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
