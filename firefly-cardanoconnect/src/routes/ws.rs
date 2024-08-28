use axum::{
    extract::{
        ws::{Message, WebSocket},
        WebSocketUpgrade,
    },
    response::Response,
};

async fn handle_socket(mut socket: WebSocket) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(msg) => {
                if let Some(response) = process_message(msg) {
                    if let Err(error) = socket.send(response).await {
                        println!("client disconnected: {}", error);
                        return;
                    }
                }
            }
            Err(error) => {
                println!("client disconnected: {}", error);
                return;
            }
        }
    }
}

fn process_message(msg: Message) -> Option<Message> {
    match msg {
        Message::Text(text) => {
            println!("WS received message {}", text);
            Some(Message::Text(text))
        }
        Message::Binary(bytes) => {
            println!("WS received message {:?}", bytes);
            Some(Message::Binary(bytes))
        }
        _ => None,
    }
}

pub async fn handle_socket_upgrade(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}
