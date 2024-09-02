use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct ApiDuration(Duration);
impl From<Duration> for ApiDuration {
    fn from(value: Duration) -> Self {
        Self(value)
    }
}
impl From<ApiDuration> for Duration {
    fn from(value: ApiDuration) -> Self {
        value.0
    }
}
impl Serialize for ApiDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format!("{}ms", self.0.as_millis()).serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ApiDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = duration_str::deserialize_duration(deserializer)?;
        Ok(Self(value))
    }
}
impl JsonSchema for ApiDuration {
    fn schema_name() -> String {
        "Duration".into()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = gen.subschema_for::<String>().into_object();
        schema.metadata().default = Some("30000ms".into());
        schema.into()
    }
}
