use anyhow::{bail, Result};
use clap::Parser;
use firefly::{
    FireflyCardanoClient, FireflyWebSocketEventBatch, FireflyWebSocketRequest, ListenerFilter,
    ListenerSettings, ListenerType, StreamSettings,
};
use futures::{SinkExt, StreamExt};

mod firefly;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    addr_from: String,
    #[arg(long)]
    addr_to: String,
    #[arg(long)]
    amount: u64,
    #[arg(long, default_value = "http://localhost:5018")]
    firefly_cardano_url: String,
    #[arg(long, default_value = "4")]
    rollback_horizon: u64,
}

async fn create_or_get_stream(firefly: &FireflyCardanoClient, name: &str) -> Result<String> {
    let streams = firefly.list_streams().await?;
    if let Some(s) = streams.iter().find(|s| s.name == name) {
        println!("reusing existing stream {name}");
        Ok(s.id.clone())
    } else {
        println!("creating stream {name}");
        firefly
            .create_stream(&StreamSettings {
                name: name.to_string(),
                batch_size: 50,
                batch_timeout_ms: 500,
            })
            .await
    }
}

async fn recreate_listener(
    firefly: &FireflyCardanoClient,
    stream_id: &str,
    name: &str,
    from_block: &str,
    filters: Vec<ListenerFilter>,
) -> Result<String> {
    let existing_listeners = firefly.list_listeners(stream_id).await?;
    if let Some(listener) = existing_listeners.iter().find(|l| l.name == name) {
        println!("deleting existing listener {name}");
        firefly.delete_listener(stream_id, &listener.id).await?;
    }
    println!("creating listener {name}");
    firefly
        .create_listener(
            stream_id,
            &ListenerSettings {
                name: name.to_string(),
                type_: ListenerType::Events,
                from_block: from_block.to_string(),
                filters,
            },
        )
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let firefly = FireflyCardanoClient::new(&args.firefly_cardano_url);

    // Before we submit our transaction, find the latest block.
    // If a transaction is submitted, it'll be included in a _later_ block,
    // so this lets us skip history
    let tip = firefly.get_chain_tip().await?;

    let Some(txid) = firefly
        .invoke_contract(
            &args.addr_from,
            "simple-tx",
            "send_ada",
            [
                ("fromAddress", args.addr_from.clone().into()),
                ("toAddress", args.addr_to.clone().into()),
                ("amount", args.amount.into()),
            ],
        )
        .await?
    else {
        bail!("contract did not return transaction")
    };
    println!("submitted transaction {txid}");

    let stream_topic = "firefly-cardano-demo";

    // A "stream" is a communication channel for events.
    // Each application will create one and reuse it indefinitely.
    let stream_id = create_or_get_stream(&firefly, stream_topic).await?;

    // from_block tells the stream which block to start listening from.
    // It can be "earliest", "latest", or a kupo-style "slot.hash" string.
    let from_block = format!("{}.{}", tip.block_slot, tip.block_hash);

    // A "listener" represents a logical process consuming events from this stream.
    // Listeners use filters to listen for specific events.
    // Here we create one listener just for our new transaction.
    let tx_listener_id = recreate_listener(
        &firefly,
        &stream_id,
        &format!("listener-{txid}"),
        &from_block,
        vec![ListenerFilter::TransactionId(txid.clone())],
    )
    .await?;

    // Here we create another that listens to every transaction, so we can track the block height
    let all_txs_listener_id = recreate_listener(
        &firefly,
        &stream_id,
        "listener-all",
        &from_block,
        vec![ListenerFilter::TransactionId("any".into())],
    )
    .await?;

    // now let's actually listen to messages
    println!("connecting to websocket");
    let mut ws = firefly.connect_ws().await?;
    ws.send(
        FireflyWebSocketRequest::Listen {
            topic: stream_topic.to_string(),
        }
        .into(),
    )
    .await?;

    println!("listening for events");
    let mut block_height_when_final = None;
    while let Some(msg) = ws.next().await {
        let message = msg?;
        let batch: FireflyWebSocketEventBatch = match message.try_into() {
            Ok(b) => b,
            Err(error) => {
                eprintln!("{error}");
                continue;
            }
        };
        println!("received message batch {batch:?}");
        for event in batch.events {
            if event.listener_id == tx_listener_id {
                assert_eq!(event.transaction_hash, txid);
                if event.signature.starts_with("TransactionAccepted") {
                    println!("our transaction was accepted in block #{}! waiting for {} more blocks to be sure it won't roll back...", event.block_number, args.rollback_horizon);
                    block_height_when_final = Some(event.block_number + args.rollback_horizon);
                }
                if event.signature.starts_with("TransactionRolledBack") {
                    bail!("our transaction was rolled back...");
                }
            }
            #[allow(clippy::collapsible_if)]
            if event.listener_id == all_txs_listener_id {
                if block_height_when_final.is_some_and(|h| event.block_number >= h) {
                    println!(
                        "alright it's been long enough, this transaction is probably final by now"
                    );
                    return Ok(());
                }
            }
        }
        ws.send(
            FireflyWebSocketRequest::Ack {
                topic: stream_topic.to_string(),
                batch_number: batch.batch_number,
            }
            .into(),
        )
        .await?;
    }

    Ok(())
}
