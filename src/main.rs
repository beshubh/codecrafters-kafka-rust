#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

mod wire;
mod apis;

use crate::{apis::{ApiHandler, ApiVersionsHandler}, wire::{ReqHeader, ReqMessage, ResBody, ResHeader, ResMessage}};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_client(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn serialize(buf: &[u8]) -> ReqMessage {
    let message = ReqMessage::from_bytes(buf);
    message
}

fn handle_client(mut stream: TcpStream) {
    let mut buf  = [0u8; 1024];
    let _
        = stream.read(&mut buf).unwrap();
    let request = ReqMessage::from_bytes(&buf);

    println!("request api verions: {:?}", request.header.request_api_version);
    let response = match request.header.request_api_key {
        apis::API_VERSION => ApiVersionsHandler::new(request).handle(),
        _ => {
            ResMessage {
                message_size: request.message_size,
                header: ResHeader {
                    correlation_id: request.header.correlation_id,
                },
                body: ResBody::default()
            }
        }
    };
    stream.write_all(&response.to_bytes()).unwrap();
}
