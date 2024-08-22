use aide::{
    axum::{routing::get, ApiRouter, IntoApiResponse},
    openapi::OpenApi,
};
use anyhow::Result;
use axum::{response::Html, Extension, Json};
use axum_swagger_ui::swagger_ui;
use tokio::net::TcpListener;

use crate::config::ServerConfig;

async fn serve_api(Extension(api): Extension<OpenApi>) -> impl IntoApiResponse {
    Json(api)
}

pub async fn serve(config: &ServerConfig, router: ApiRouter) -> Result<()> {
    // add common routes
    let app = router.route("/api/openapi.json", get(serve_api)).route(
        "/api",
        get(|| async { Html(swagger_ui("/api/openapi.json")) }),
    );

    let mut api = OpenApi {
        info: config.info.clone().unwrap_or_default(),
        ..OpenApi::default()
    };

    let listener = TcpListener::bind(format!("{}:{}", config.address, config.port)).await?;

    axum::serve(
        listener,
        app.finish_api(&mut api)
            .layer(Extension(api))
            .into_make_service(),
    )
    .await?;

    Ok(())
}
