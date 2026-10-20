mod support;

use std::time::{Duration, SystemTime};

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    CreateQueueRequest, CreateTaskRequest, HttpRequest, OidcToken, Queue, RetryConfig, Task,
    http_request, task,
};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use prost_types::Timestamp;
use serde_json::Value;
use tonic::Code;

fn queue(retry_config: Option<RetryConfig>) -> CreateQueueRequest {
    CreateQueueRequest {
        parent: "projects/p/locations/l".to_owned(),
        queue: Some(Queue {
            name: "projects/p/locations/l/queues/q".to_owned(),
            retry_config,
            ..Default::default()
        }),
    }
}

fn create(url: &str) -> CreateTaskRequest {
    CreateTaskRequest {
        parent: "projects/p/locations/l/queues/q".to_owned(),
        task: Some(Task {
            name: "projects/p/locations/l/queues/q/tasks/t".to_owned(),
            message_type: Some(task::MessageType::HttpRequest(HttpRequest {
                url: url.to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    }
}

fn elapsed(from: Option<Timestamp>, to: Option<Timestamp>) -> Duration {
    let from = SystemTime::try_from(from.unwrap()).unwrap();
    SystemTime::try_from(to.unwrap())
        .unwrap()
        .duration_since(from)
        .unwrap()
}

#[tokio::test]
async fn waits_for_schedule_time() {
    let mut client = support::start_emulator().await;
    let receiver = support::Receiver::start().await;
    client.create_queue(queue(None)).await.unwrap();
    let schedule_time = SystemTime::now() + Duration::from_millis(300);
    let mut request = create(&receiver.url_for("t", "200"));
    request.task.as_mut().unwrap().schedule_time = Some(Timestamp::from(schedule_time));

    client.create_task(request).await.unwrap();

    assert!(receiver.wait_for("t", 1).await[0].time >= schedule_time);
}

#[tokio::test]
async fn removes_task_after_success() {
    let mut client = support::start_emulator().await;
    let receiver = support::Receiver::start().await;
    client.create_queue(queue(None)).await.unwrap();
    client
        .create_task(create(&receiver.url_for("t", "200")))
        .await
        .unwrap();

    let gone = support::poll_task(
        &mut client,
        "projects/p/locations/l/queues/q/tasks/t",
        |_| false,
    )
    .await;

    assert_eq!(gone.unwrap_err().code(), Code::NotFound);
}

#[tokio::test]
async fn first_retry_waits_min_backoff() {
    let mut client = support::start_emulator().await;
    let receiver = support::Receiver::start().await;
    client.create_queue(queue(None)).await.unwrap();
    client
        .create_task(create(&receiver.url_for("t", "404")))
        .await
        .unwrap();

    let task = support::poll_task(
        &mut client,
        "projects/p/locations/l/queues/q/tasks/t",
        |task| task.response_count == 1,
    )
    .await
    .unwrap();

    assert_eq!(
        elapsed(task.last_attempt.unwrap().response_time, task.schedule_time),
        Duration::from_millis(100)
    );
}

#[tokio::test]
async fn oidc_token_verifies_with_discovered_keys() {
    let (mut client, url) = support::start_emulator_with_url().await;
    let receiver = support::Receiver::start().await;
    client.create_queue(queue(None)).await.unwrap();
    let request = CreateTaskRequest {
        parent: "projects/p/locations/l/queues/q".to_owned(),
        task: Some(Task {
            message_type: Some(task::MessageType::HttpRequest(HttpRequest {
                url: receiver.url_for("t", "200"),
                authorization_header: Some(http_request::AuthorizationHeader::OidcToken(
                    OidcToken {
                        service_account_email: "tasks@p.iam.gserviceaccount.com".to_owned(),
                        audience: "https://example.com/audience".to_owned(),
                    },
                )),
                ..Default::default()
            })),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    };
    client.create_task(request).await.unwrap();
    let authorization = receiver.wait_for("t", 1).await[0].headers["authorization"].clone();
    let token = authorization
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap();
    let get = async |url: &str| reqwest::get(url).await.unwrap().bytes().await.unwrap();
    let discovery: Value =
        serde_json::from_slice(&get(&format!("{url}/.well-known/openid-configuration")).await)
            .unwrap();
    let keys: JwkSet =
        serde_json::from_slice(&get(discovery["jwks_uri"].as_str().unwrap()).await).unwrap();
    let kid = jsonwebtoken::decode_header(token).unwrap().kid.unwrap();
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[&url]);
    validation.set_audience(&["https://example.com/audience"]);

    let claims = jsonwebtoken::decode::<Value>(
        token,
        &DecodingKey::from_jwk(keys.find(&kid).unwrap()).unwrap(),
        &validation,
    );

    assert_eq!(
        claims.unwrap().claims["email"],
        "tasks@p.iam.gserviceaccount.com"
    );
}
