use std::env::VarError;

use tokio::net::TcpListener;

const ISSUER_REQUIRED: &str = "OIDC_ISSUER is required and must be an http or https origin, for example http://127.0.0.1:8123";
const QUEUES_REQUIRED: &str = "QUEUES is required and lists full queue names separated by commas, for example projects/p/locations/l/queues/q, or is empty for none";
const APP_ENGINE_INVALID: &str = "APP_ENGINE_URL must be an http or https origin, for example http://127.0.0.1:8080, or unset when no project has an App Engine app";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = std::env::var("LISTEN_ADDR")
        .map_err(|_| "LISTEN_ADDR is required, for example 127.0.0.1:8123")?;
    let issuer = std::env::var("OIDC_ISSUER").map_err(|_| ISSUER_REQUIRED)?;
    let issuer = origin(&issuer).ok_or(ISSUER_REQUIRED)?;
    let app_engine = match std::env::var("APP_ENGINE_URL") {
        Ok(url) => Some(origin(&url).ok_or(APP_ENGINE_INVALID)?),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => return Err(APP_ENGINE_INVALID.into()),
    };
    let queues = std::env::var("QUEUES").map_err(|_| QUEUES_REQUIRED)?;
    let queues = match queues.as_str() {
        "" => Vec::new(),
        queues => queues.split(',').map(str::to_owned).collect(),
    };
    let listener = TcpListener::bind(&address).await?;
    eprintln!("Cloud Tasks emulator listening on {address}");
    cloud_tasks_emulator::serve(listener, issuer, app_engine, queues).await?;
    Ok(())
}

/// The origin of an http or https URL that has nothing after it
fn origin(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).ok()?;
    let origin = url.origin().ascii_serialization();
    (matches!(url.scheme(), "http" | "https") && format!("{origin}/") == url.as_str())
        .then_some(origin)
}
