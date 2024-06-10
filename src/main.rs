use std::net::TcpListener;

use tokio::{spawn, task::JoinHandle};
use futures::future::join_all;

use dbms::handle_connection;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("localhost:42069").unwrap();
    println!("Listening on localhost:42069 (of course)");

    let mut join_handles = vec![];

    for stream in listener.incoming() {
        join_handles.retain(|handle: &JoinHandle<_>| {
            !handle.is_finished()
        });

        match stream {
            Ok(stream) => {
                println!("New connection established from {}", stream.peer_addr().unwrap());
                println!("Now have {} connections", join_handles.len() + 1);

                join_handles.push(spawn(async move {
                    handle_connection(stream).await
                }));
            },
            Err(error) => panic!("{error}"),
        }
    }

    join_all(join_handles).await
        .into_iter()
        .collect::<Result<Result<Vec<_>, _>, _>>().unwrap().unwrap();

    println!("Main thread exiting");
}
