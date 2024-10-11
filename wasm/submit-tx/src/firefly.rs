use std::{mem, sync::{LazyLock, Mutex}};

use balius_sdk::{Config, FnHandler, Json, Params, Worker, WorkerResult};
use serde::Serialize;

#[derive(Serialize)]
pub struct Event {
    pub signature: String,
}

static EVENTS: LazyLock<Mutex<Vec<Event>>> = LazyLock::new(|| Mutex::new(vec![]));

#[derive(Serialize)]
struct CollectEventsResponse {
    events: Vec<Event>,
}

pub fn emit_event(event: Event) {
    let mut events = EVENTS.lock().unwrap();
    events.push(event);
}

fn collect_events(_: Config<()>, _: Params<()>) -> WorkerResult<Json<CollectEventsResponse>> {
    let mut all_events = EVENTS.lock().unwrap();
    let events: Vec<Event> = mem::take(&mut all_events);
    Ok(Json(CollectEventsResponse { events }))
}

pub fn register(worker: Worker) -> Worker {
    worker.with_request_handler("_collect_events", FnHandler::from(collect_events))
}