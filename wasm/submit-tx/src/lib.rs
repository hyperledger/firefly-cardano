use balius_sdk::{Config, FnHandler, Json, Params, Worker, WorkerResult};
use firefly::Event;
use serde::{Deserialize, Serialize};

mod firefly;

#[derive(Deserialize)]
struct NoConfig {}

#[derive(Deserialize)]
struct TransferRequest {
}

#[derive(Serialize)]
struct TransferResponse {
}

fn transfer_funds(_: Config<NoConfig>, _: Params<TransferRequest>) -> WorkerResult<Json<TransferResponse>> {
    firefly::emit_event(Event { signature: "Fun".into() });
    Ok(Json(TransferResponse {  }))
}

#[balius_sdk::main]
fn main() -> Worker {
    firefly::register(Worker::new())
        .with_request_handler("transfer_funds", FnHandler::from(transfer_funds))
}