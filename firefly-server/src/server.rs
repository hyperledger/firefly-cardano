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

pub async fn serve(
    config: &ApiConfig,
    versions: impl IntoIterator<Item = (&str, ApiRouter)>,
) -> Result<()> {
    let mut router = Router::<()>::new();

    let base_api = OpenApi {
        info: config.info.clone(),
        ..OpenApi::default()
    };
    let mut urls = vec![];
    for (version, api_router) in versions {
        let prefix = format!("/api/{version}");
        let api_router = api_router.route("/openapi.json", get(serve_api));

        urls.push(format!("{prefix}/openapi.json"));

        let mut api = base_api.clone();
        api.info.version = version.to_string();
        let api_router = ApiRouter::new()
            .nest(&prefix, api_router)
            .finish_api(&mut api)
            .layer(Extension(api));
        router = router.merge(api_router);
    }

    let swagger_ui_router: Router = SwaggerUi::new("/api").config(Config::new(urls)).into();
    router = router.merge(swagger_ui_router);

    let listener = TcpListener::bind(format!("{}:{}", config.address, config.port)).await?;
    info!(
        "{} is now running on port {}",
        config.info.title, config.port
    );
    axum::serve(
        listener,
        router.layer(TraceLayer::new_for_http()).into_make_service(),
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
