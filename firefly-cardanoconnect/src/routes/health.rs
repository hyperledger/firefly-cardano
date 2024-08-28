use aide::axum::IntoApiResponse;
use axum::Json;

pub async fn health() -> impl IntoApiResponse {
    Json("Hello, world!")
}
