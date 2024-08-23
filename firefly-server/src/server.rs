use aide::{
    axum::{routing::get, ApiRouter, IntoApiResponse},
    openapi::OpenApi,
};
use anyhow::Result;
use axum::{response::Html, Extension, Json};
use axum_swagger_ui::swagger_ui;
use tokio::{net::TcpListener, signal};

use crate::config::ApiConfig;

async fn serve_api(Extension(api): Extension<OpenApi>) -> impl IntoApiResponse {
    Json(api)
}

pub async fn serve(config: &ApiConfig, router: ApiRouter) -> Result<()> {
    // add common routes
    let app = router.route("/api/openapi.json", get(serve_api)).route(
        "/api",
        get(|| async { Html(swagger_ui("/api/openapi.json")) }),
    );

    let mut api = OpenApi {
        info: config.info.clone().unwrap_or_default(),
        ..OpenApi::default()
    };
    if api.info.version.is_empty() {
        api.info.version = env!("CARGO_PKG_VERSION").to_string();
    }

    let listener = TcpListener::bind(format!("{}:{}", config.address, config.port)).await?;

    axum::serve(
        listener,
        app.finish_api(&mut api)
            .layer(Extension(api))
            .into_make_service(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
