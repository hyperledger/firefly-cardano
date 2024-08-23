use aide::OperationOutput;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn new(status: impl Into<StatusCode>, message: impl Into<String>) -> Self {
        Self {
            status: status.into(),
            message: message.into(),
        }
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: value.into().to_string(),
        }
    }
}

impl OperationOutput for ApiError {
    type Inner = Self;
}

pub type ApiResult<T, E = ApiError> = std::result::Result<T, E>;
