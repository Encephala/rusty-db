mod connection;
mod protocol;

pub use protocol::Packet;

use std::net::SocketAddr;

use tokio::{
    net::{
        TcpListener,
        TcpStream,
        ToSocketAddrs,
    },
    signal::ctrl_c,
    spawn,
    sync::broadcast::*,
    task::{JoinError, JoinHandle}
};
use futures::future::{select_all, join_all, OptionFuture};

use connection::handle_connection;

use crate::SqlError;

pub async fn server(listen_address: impl ToSocketAddrs) {
    let listener = TcpListener::bind(listen_address).await.unwrap();
    println!("Listening on {:?}", listener.local_addr().unwrap());

    let mut join_handles = vec![];

    let (shutdown_sender, mut shutdown_receiver_main) = channel::<()>(1);

    let shutdown_sender_main = shutdown_sender.clone();

    // Catch ctrl+c signal
    // TODO: catch double ctrl+c for force quit (how does one force quit all tokio tasks?)
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
        let join_all_future: OptionFuture<_> = match join_handles.len() {
            0 => None,
            _ => Some(select_all(&mut join_handles)),
        }.into();

        tokio::select! {
            _ = shutdown_receiver_main.recv() => {
                println!("Main thread received exit signal");
                break;
            },

            result = listener.accept() => {
                match result {
                    Err(error) => {
                        eprintln!("Failed to accept connection: {error}");
                    },
                    Ok((stream, address)) => {
                        join_handles.push(spawn_new_handler(stream, address, shutdown_sender.subscribe()));
                    }
                };
            },

            Some((result, resolved_index, _)) = join_all_future => {
                print_join_error(result);

                join_handles.remove(resolved_index);
            }
        }
    }

    println!("Waiting for all worker threads to exit");

    // TODO: collecting into result is lazy, and unwrap, not way to go here
    for result in join_all(join_handles).await {
        print_join_error(result);
    }

    println!("Main thread exiting");
}

fn spawn_new_handler(
    stream: TcpStream,
    address: SocketAddr,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<Result<(), SqlError>> {
    println!("New connection established from {address:?}");

    return spawn(async move {
        handle_connection(stream, shutdown_receiver).await
    });
}

fn print_join_error(result: Result<Result<(), SqlError>, JoinError>) {
    match result {
        Err(error) => eprintln!("Failed to join: {error}"),
        result => {
            if let Err(result) = result.unwrap() {
                eprintln!("Task failed: {result:?}")
            }
        }
    };
}
