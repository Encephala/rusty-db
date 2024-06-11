#![allow(clippy::needless_return)]
use dbms::server::server;

#[tokio::main]
async fn main() {
    let address = "localhost:42069";

    server(address).await;
}
