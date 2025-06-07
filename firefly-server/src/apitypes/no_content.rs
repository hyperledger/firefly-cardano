use aide::{OperationOutput, openapi};
use axum::{body::Body, http::Response, response::IntoResponse};
use reqwest::StatusCode;

pub struct NoContent;
impl IntoResponse for NoContent {
    fn into_response(self) -> Response<Body> {
        let mut res = Response::new(Body::empty());
        *res.status_mut() = StatusCode::NO_CONTENT;
        res
    }
}

impl OperationOutput for NoContent {
    type Inner = Self;

    fn inferred_responses(
        _ctx: &mut aide::generate::GenContext,
        _operation: &mut openapi::Operation,
    ) -> Vec<(Option<u16>, openapi::Response)> {
        vec![(Some(204), openapi::Response::default())]
    }
}
