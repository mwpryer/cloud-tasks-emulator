use tokio::net::TcpListener;

const ISSUER_REQUIRED: &str = "OIDC_ISSUER is required and must be an http or https origin, for example http://127.0.0.1:8123";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("LISTEN_ADDR")
        .map_err(|_| "LISTEN_ADDR is required, for example 127.0.0.1:8123")?;
    let issuer = std::env::var("OIDC_ISSUER").map_err(|_| ISSUER_REQUIRED)?;
    let issuer = reqwest::Url::parse(&issuer).map_err(|_| ISSUER_REQUIRED)?;
    let origin = issuer.origin().ascii_serialization();
    if !matches!(issuer.scheme(), "http" | "https") || format!("{origin}/") != issuer.as_str() {
        return Err(ISSUER_REQUIRED.into());
    }
    let listener = TcpListener::bind(&address).await?;
    eprintln!("Cloud Tasks emulator listening on {address}");
    cloud_tasks_emulator::serve(listener, origin).await?;
    Ok(())
}
