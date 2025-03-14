use anyhow::{bail, Result};
use clap::Parser;
use firefly::{
    FireflyCardanoClient, FireflyWebSocketEvent, FireflyWebSocketEventBatch,
    FireflyWebSocketRequest, ListenerFilter, ListenerSettings, ListenerType, StreamSettings,
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

async fn clear_listeners(firefly: &FireflyCardanoClient, stream_id: &str) -> Result<()> {
    let existing_listeners = firefly.list_listeners(stream_id).await?;
    for listener in existing_listeners {
        println!("deleting existing listener {}", listener.name);
        firefly.delete_listener(stream_id, &listener.id).await?;
    }
    Ok(())
}

async fn create_listener(
    firefly: &FireflyCardanoClient,
    stream_id: &str,
    name: &str,
    from_block: &str,
    filters: Vec<ListenerFilter>,
) -> Result<String> {
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
    clear_listeners(&firefly, &stream_id).await?;

    // from_block tells the stream which block to start listening from.
    // It can be "earliest", "latest", or a kupo-style "slot.hash" string.
    let from_block = format!("{}.{}", tip.block_slot, tip.block_hash);

    // A "listener" represents a logical process consuming events from this stream.
    // Listeners use filters to listen for specific events.
    // Here we create a listener for each event we care about
    let tx_listener_id = create_listener(
        &firefly,
        &stream_id,
        &format!("listener-{txid}"),
        &from_block,
        vec![
            // The contract emits specific events at different parts of the transaction lifecycle
            ListenerFilter::Event {
                contract: "simple-tx".into(),
                event_path: "TransactionAccepted(string)".into(),
            },
            ListenerFilter::Event {
                contract: "simple-tx".into(),
                event_path: "TransactionRolledBack(string)".into(),
            },
            ListenerFilter::Event {
                contract: "simple-tx".into(),
                event_path: "TransactionFinalized(string)".into(),
            },
        ],
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
            let FireflyWebSocketEvent::ContractEvent(event) = event else {
                println!("ignoring {event:?}");
                continue;
            };
            if event.listener_id != tx_listener_id {
                continue;
            }

            if event.signature.starts_with("TransactionAccepted") && event.transaction_hash == txid
            {
                println!("our transaction was accepted in block #{}! waiting for the contract to decide it was finalized", event.block_number);
            }
            if event.signature.starts_with("TransactionRolledBack")
                && event.transaction_hash == txid
            {
                bail!("our transaction was rolled back...");
            }
            #[allow(clippy::collapsible_if)]
            if event.signature.starts_with("TransactionFinalized") {
                if event
                    .data
                    .as_object()
                    .and_then(|o| o.get("transactionId"))
                    .and_then(|id| id.as_str())
                    .is_none_or(|id| id != txid)
                {
                    continue;
                }
                println!(
                    "as of block {}, our transaction is finalized!",
                    event.block_number
                );
                return Ok(());
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
