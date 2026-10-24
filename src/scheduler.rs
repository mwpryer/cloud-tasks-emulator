use std::sync::Arc;
use std::time::SystemTime;

use reqwest::Method;

use crate::oidc::Issuer;
use crate::state::{Delivery, Emulator, HttpMethod, Outcome, Request, Token};

pub(crate) async fn run(emulator: Arc<Emulator>, issuer: Arc<Issuer>) {
    let client = reqwest::Client::new();
    loop {
        let (deliveries, wait) = emulator.state.lock().unwrap().start_due(SystemTime::now());
        for delivery in deliveries {
            tokio::spawn(deliver(
                emulator.clone(),
                issuer.clone(),
                client.clone(),
                delivery,
            ));
        }
        match wait {
            Some(wait) => {
                tokio::select! {
                    () = emulator.wake.notified() => {}
                    () = tokio::time::sleep(wait) => {}
                }
            }
            None => emulator.wake.notified().await,
        }
    }
}

async fn deliver(
    emulator: Arc<Emulator>,
    issuer: Arc<Issuer>,
    client: reqwest::Client,
    delivery: Delivery,
) {
    let outcome = match &delivery.request {
        Some(request) => send(&issuer, &client, request).await,
        None => Outcome::Unreachable,
    };
    let result = match outcome {
        Outcome::Status(status) => status.to_string(),
        Outcome::DeadlineExceeded => "deadline exceeded".to_owned(),
        Outcome::Unreachable => "unreachable".to_owned(),
    };
    match &delivery.request {
        Some(request) => eprintln!("{} {}: {result}", delivery.task, request.url),
        None => eprintln!("{}: {result}", delivery.task),
    }

    emulator
        .state
        .lock()
        .unwrap()
        .finish_attempt(&delivery, outcome, SystemTime::now());
    emulator.wake.notify_one();
}

async fn send(issuer: &Issuer, client: &reqwest::Client, request: &Request) -> Outcome {
    let method = match request.http_method {
        HttpMethod::Post => Method::POST,
        HttpMethod::Get => Method::GET,
        HttpMethod::Head => Method::HEAD,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
        HttpMethod::Patch => Method::PATCH,
        HttpMethod::Options => Method::OPTIONS,
    };
    let mut builder = client
        .request(method, request.url.clone())
        .timeout(request.deadline)
        .body(request.body.clone());
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    if let Some(token) = &request.token {
        let token = match token {
            // Opaque, as only Google APIs could check real access token
            Token::OAuth => format!("{:032x}", rand::random::<u128>()),
            Token::Oidc {
                service_account_email,
                audience,
            } => issuer.id_token(service_account_email, audience, SystemTime::now()),
        };
        builder = builder.bearer_auth(token);
    }
    match builder.send().await {
        Ok(response) => Outcome::Status(response.status().as_u16()),
        Err(error) if error.is_timeout() => Outcome::DeadlineExceeded,
        Err(_) => Outcome::Unreachable,
    }
}
