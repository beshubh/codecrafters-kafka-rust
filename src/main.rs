#![allow(unused_imports)]
use std::{
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
};

mod apis;
mod router;
mod wire;

use router::{handle_request, RequestContext};
use wire::{Decode, ReqMessage};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                std::thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    loop {
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).unwrap();

        if n == 0 {
            println!("client disconnected");
            return;
        }
        let mut cur = Cursor::new(&buf[..n]);
        let request = ReqMessage::decode(&mut cur).unwrap();

        let response = handle_request(RequestContext::from(request));
        stream.write_all(&response.to_bytes().unwrap()).unwrap();
    }
}
