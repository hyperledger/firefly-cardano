use std::time::Duration;

use anyhow::Result;
use duration_str::deserialize_option_duration;
use reqwest::{Client, IntoUrl};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::apitypes::{ApiError, ApiResult};

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpClientConfig {
    #[serde(deserialize_with = "deserialize_option_duration")]
    pub connection_timeout: Option<Duration>,
    #[serde(deserialize_with = "deserialize_option_duration")]
    pub request_timeout: Option<Duration>,
}

#[derive(Clone)]
pub struct HttpClient(Client);

impl HttpClient {
    pub fn new(config: &HttpClientConfig) -> Result<Self> {
        let connection_timeout = config.connection_timeout.unwrap_or(Duration::from_secs(30));
        let request_timeout = config.request_timeout.unwrap_or(Duration::from_secs(30));
        let client = Client::builder()
            .connect_timeout(connection_timeout)
            .timeout(request_timeout)
            .build()?;
        Ok(Self(client))
    }

    pub async fn post<U, Req, Res>(&self, url: U, req: &Req) -> ApiResult<Res>
    where
        U: IntoUrl,
        Req: Serialize,
        Res: for<'a> Deserialize<'a>,
    {
        self.0.post(url).json(req).send().await.parse().await
    }
}

trait HttpResponseExt: Sized {
    async fn parse<T: DeserializeOwned>(self) -> ApiResult<T>;
}

impl HttpResponseExt for std::result::Result<reqwest::Response, reqwest::Error> {
    async fn parse<T: DeserializeOwned>(self) -> ApiResult<T> {
        let res = self.map_err(ApiError::from_reqwest)?;
        let status = res.status();
        if !status.is_success() {
            let message = res.text().await.unwrap_or(status.to_string());
            return Err(ApiError::new(status, message));
        }
        Ok(res.json().await?)
    }
}
