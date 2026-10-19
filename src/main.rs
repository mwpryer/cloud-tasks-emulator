use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("LISTEN_ADDR")
        .map_err(|_| "LISTEN_ADDR is required, for example 127.0.0.1:8123")?;
    let listener = TcpListener::bind(&address).await?;
    eprintln!("Cloud Tasks emulator listening on {address}");
    cloud_tasks_emulator::serve(listener).await?;
    Ok(())
}
