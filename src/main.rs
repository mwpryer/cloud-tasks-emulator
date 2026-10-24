use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};

const ISSUER_REQUIRED: &str = "OIDC_ISSUER is required and must be an http or https origin, for example http://127.0.0.1:8123";
const QUEUES_REQUIRED: &str = "QUEUES is required and lists full queue names separated by commas, for example projects/p/locations/l/queues/q, or is empty for none";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = std::env::var("LISTEN_ADDR")
        .map_err(|_| "LISTEN_ADDR is required, for example 127.0.0.1:8123")?;
    let issuer = std::env::var("OIDC_ISSUER").map_err(|_| ISSUER_REQUIRED)?;
    let issuer = origin(&issuer).ok_or(ISSUER_REQUIRED)?;
    let queues = std::env::var("QUEUES").map_err(|_| QUEUES_REQUIRED)?;
    let queues = match queues.as_str() {
        "" => Vec::new(),
        queues => queues.split(',').map(str::to_owned).collect(),
    };
    // Process running as PID 1 in container ignores signals it has no handler for
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let listener = TcpListener::bind(&address).await?;
    eprintln!("Cloud Tasks emulator listening on {address}");
    tokio::select! {
        result = cloud_tasks_emulator::serve(listener, issuer, queues) => result?,
        _ = interrupt.recv() => {}
        _ = terminate.recv() => {}
    }
    Ok(())
}

/// None unless `url` is bare http or https origin
fn origin(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).ok()?;
    let origin = url.origin().ascii_serialization();
    (matches!(url.scheme(), "http" | "https") && format!("{origin}/") == url.as_str())
        .then_some(origin)
}
