use std::sync::Arc;
use std::time::SystemTime;

use reqwest::Method;

use crate::oidc::Issuer;
use crate::state::{AuthorizationHeader, Delivery, Emulator, HttpMethod, Outcome};

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
    let method = match delivery.http_method {
        HttpMethod::Post => Method::POST,
        HttpMethod::Get => Method::GET,
        HttpMethod::Head => Method::HEAD,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
        HttpMethod::Patch => Method::PATCH,
        HttpMethod::Options => Method::OPTIONS,
    };
    let mut request = client
        .request(method, &delivery.url)
        .timeout(delivery.deadline)
        .body(delivery.body.clone());
    for (name, value) in &delivery.headers {
        request = request.header(name, value);
    }
    if let Some(authorization_header) = &delivery.authorization_header {
        let token = match authorization_header {
            AuthorizationHeader::OAuthToken { .. } => {
                // Opaque, as only Google APIs could check a real access token
                format!("{:032x}", rand::random::<u128>())
            }
            AuthorizationHeader::OidcToken {
                service_account_email,
                audience,
            } => issuer.id_token(service_account_email, audience, SystemTime::now()),
        };
        request = request.bearer_auth(token);
    }
    let outcome = match request.send().await {
        Ok(response) => Outcome::Status(response.status().as_u16()),
        Err(error) if error.is_timeout() => Outcome::DeadlineExceeded,
        Err(_) => Outcome::Unreachable,
    };
    let result = match outcome {
        Outcome::Status(status) => status.to_string(),
        Outcome::DeadlineExceeded => "deadline exceeded".to_owned(),
        Outcome::Unreachable => "unreachable".to_owned(),
    };
    eprintln!("{} {}: {result}", delivery.task, delivery.url);

    emulator
        .state
        .lock()
        .unwrap()
        .finish_attempt(&delivery, outcome, SystemTime::now());
    emulator.wake.notify_one();
}
