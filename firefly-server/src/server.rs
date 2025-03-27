use aide::{
    NoApi,
    axum::{ApiRouter, IntoApiResponse, routing::get},
    openapi::{Info, OpenApi},
};
use anyhow::Result;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use tokio::{net::TcpListener, signal};
use tower_http::trace::TraceLayer;
use tracing::info;
use utoipa_swagger_ui::{Config, SwaggerUi};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiConfig {
    pub address: String,
    pub port: u16,
    #[serde(default)]
    pub info: Info,
}

async fn serve_api(Extension(api): Extension<OpenApi>) -> impl IntoApiResponse {
    NoApi(Json(api))
}

pub async fn serve(config: &ApiConfig, router: ApiRouter) -> Result<()> {
    // add common routes
    let swagger_ui_router: Router = SwaggerUi::new("/api")
        .config(Config::new(["/api/openapi.json"]))
        .into();
    let app = router
        .route("/api/openapi.json", get(serve_api))
        .merge(swagger_ui_router);

    let mut api = OpenApi {
        info: config.info.clone(),
        ..OpenApi::default()
    };
    if api.info.version.is_empty() {
        api.info.version = env!("CARGO_PKG_VERSION").to_string();
    }

    let listener = TcpListener::bind(format!("{}:{}", config.address, config.port)).await?;

    info!("{} is now running on port {}", api.info.title, config.port);

    axum::serve(
        listener,
        app.finish_api(&mut api)
            .layer(Extension(api))
            .layer(TraceLayer::new_for_http())
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
