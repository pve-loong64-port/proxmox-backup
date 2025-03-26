use anyhow::Error;
use bytes::Bytes;
use futures::*;
use http_body_util::Full;
use hyper::{body::Incoming, Request, Response};

use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::{TcpListener, TcpStream};

fn main() -> Result<(), Error> {
    proxmox_async::runtime::main(run())
}

async fn run() -> Result<(), Error> {
    let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 8008))).await?;

    println!("listening on {:?}", listener.local_addr());

    loop {
        let (socket, _addr) = listener.accept().await?;
        tokio::spawn(handle_connection(socket).map(|res| {
            if let Err(err) = res {
                eprintln!("Error: {}", err);
            }
        }));
    }
}

async fn handle_connection(socket: TcpStream) -> Result<(), Error> {
    socket.set_nodelay(true).unwrap();

    let mut http = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    // increase window size: todo - find optiomal size
    let max_window_size = (1 << 31) - 2;
    http.initial_stream_window_size(max_window_size);
    http.initial_connection_window_size(max_window_size);

    let service = hyper::service::service_fn(|_req: Request<Incoming>| {
        println!("Got request");
        let buffer = vec![65u8; 4 * 1024 * 1024]; // nonsense [A,A,A,A...]
        let body = Full::<Bytes>::from(buffer);

        let response = Response::builder()
            .status(hyper::http::StatusCode::OK)
            .header(
                hyper::http::header::CONTENT_TYPE,
                "application/octet-stream",
            )
            .body(body)
            .unwrap();
        future::ok::<_, Error>(response)
    });

    http.serve_connection(TokioIo::new(socket), service)
        .map_err(Error::from)
        .await?;

    println!("H2 connection CLOSE !");
    Ok(())
}
