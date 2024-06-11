#![allow(clippy::needless_return)]
use std::net::SocketAddr;

use async_std::net::{
    TcpListener,
    TcpStream,
};
use tokio::{
    signal::ctrl_c,
    spawn,
    sync::broadcast::{
        channel,
        Receiver
    },
    task::JoinHandle,
};
use futures::future::{select_all, join_all, OptionFuture};

use dbms::server::handle_connection;

fn spawn_new_handler(
    listen_result: Result<(TcpStream, SocketAddr), std::io::Error>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<Result<(), dbms::SqlError>> {
    match listen_result {
        Ok((stream, address)) => {
            println!("New connection established from {address}");

            return spawn(async move {
                handle_connection(stream, shutdown_receiver).await
            });
        },
        Err(error) => panic!("{error:?}"),
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("localhost:42069").await.unwrap();
    println!("Listening on localhost:42069 (of course)");

    let mut join_handles = vec![];

    let (shutdown_sender, mut shutdown_receiver_main) = channel::<()>(1);

    let shutdown_sender_main = shutdown_sender.clone();

    // Catch ctrl+c signal
    spawn(async move {
        let result = ctrl_c().await;

        if let Err(error) = result {
            eprintln!("Error while catching SIGINT: {error}");
            eprintln!("Sending shutdown signal to all threads anyway");
        } else {
            println!("Sending shutdown signal to all threads");
        }

        let transmit_result = shutdown_sender_main.send(());

        if transmit_result.is_err() {
            eprintln!("Failed to send shutdown signal")
        }
    });

    loop {
        let join_all_future = OptionFuture::from(
            match join_handles.len() {
                0 => None,
                _ => Some(select_all(&mut join_handles)),
            }
        );

        tokio::select! {
            _ = shutdown_receiver_main.recv() => {
                println!("Main thread received exit signal");
                break;
            },

            result = listener.accept() => {
                join_handles.push(spawn_new_handler(result, shutdown_sender.subscribe()));
            },

            Some((result, resolved_index, _)) = join_all_future => {
                println!("Thread number {resolved_index} stopped: {result:?}");

                join_handles.remove(resolved_index);
            }
        }
    }

    println!("Waiting for all worker threads to exit");

    // TODO: unwrap is not way to go here
    println!("results: {:?}", join_all(join_handles).await
        .into_iter()
        .collect::<Result<Result<Vec<_>, _>, _>>().unwrap().unwrap());

    println!("Main thread exiting");
}
