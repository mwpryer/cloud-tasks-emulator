mod support;

use std::time::{Duration, SystemTime};

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    AppEngineHttpRequest, AppEngineRouting, CreateQueueRequest, CreateTaskRequest, HttpMethod,
    HttpRequest, OidcToken, PauseQueueRequest, Queue, RetryConfig, Task, http_request, task,
};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use prost_types::Timestamp;
use reqwest::StatusCode;
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

fn create_app_engine(request: AppEngineHttpRequest) -> CreateTaskRequest {
    CreateTaskRequest {
        parent: "projects/p/locations/l/queues/q".to_owned(),
        task: Some(Task {
            name: "projects/p/locations/l/queues/q/tasks/t".to_owned(),
            message_type: Some(task::MessageType::AppEngineHttpRequest(request)),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    }
}

fn relative(relative_uri: &str) -> AppEngineHttpRequest {
    AppEngineHttpRequest {
        relative_uri: relative_uri.to_owned(),
        ..Default::default()
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
    let (mut client, url) = support::start_emulator_with(None).await;
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

#[tokio::test]
async fn app_engine_task_goes_to_app_engine_url() {
    let receiver = support::Receiver::start().await;
    let (mut client, _) = support::start_emulator_with(Some(&receiver.url)).await;
    client.create_queue(queue(None)).await.unwrap();
    let request = create_app_engine(AppEngineHttpRequest {
        http_method: HttpMethod::Put.into(),
        app_engine_routing: Some(AppEngineRouting {
            service: "worker".to_owned(),
            ..Default::default()
        }),
        relative_uri: "/t/200?x=1".to_owned(),
        headers: [("X-Custom".to_owned(), "value".to_owned())].into(),
        body: b"hi".to_vec(),
    });

    client.create_task(request).await.unwrap();

    let received = &receiver.wait_for("t", 1).await[0];
    let header = |name: &str| received.headers[name].to_str().unwrap();
    assert_eq!(
        (
            received.method.as_str(),
            received.path.as_str(),
            received.query.as_deref(),
            [
                header("x-appengine-queuename"),
                header("x-appengine-taskname"),
                header("x-appengine-taskretrycount"),
                header("x-appengine-taskexecutioncount"),
                header("user-agent"),
                header("content-type"),
                header("x-custom"),
            ],
            &received.body[..],
        ),
        (
            "PUT",
            "/t/200",
            Some("x=1"),
            [
                "q",
                "t",
                "0",
                "0",
                "AppEngine-Google; (+http://code.google.com/appengine)",
                "application/octet-stream",
                "value",
            ],
            &b"hi"[..],
        )
    );
}

#[tokio::test]
async fn app_engine_task_does_not_follow_redirects() {
    let receiver = support::Receiver::start().await;
    let (mut client, _) = support::start_emulator_with(Some(&receiver.url)).await;
    // One-hour backoff keeps task from second attempt
    client
        .create_queue(queue(Some(RetryConfig {
            min_backoff: Some(prost_types::Duration {
                seconds: 3600,
                nanos: 0,
            }),
            ..Default::default()
        })))
        .await
        .unwrap();
    client
        .create_task(create_app_engine(relative("/t/redirect")))
        .await
        .unwrap();

    support::poll_task(
        &mut client,
        "projects/p/locations/l/queues/q/tasks/t",
        |task| {
            task.dispatch_count == 1
                && task
                    .last_attempt
                    .as_ref()
                    .unwrap()
                    .response_status
                    .is_some()
        },
    )
    .await
    .unwrap();

    assert_eq!(receiver.received("t").len(), 1);
}

#[tokio::test]
async fn app_engine_task_needs_app_engine_url() {
    let mut client = support::start_emulator().await;
    client.create_queue(queue(None)).await.unwrap();

    let status = client
        .create_task(create_app_engine(relative("/work")))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn wait_idle_answers_once_queue_has_no_tasks() {
    let receiver = support::Receiver::start().await;
    let (mut client, url) = support::start_emulator_with(None).await;
    client.create_queue(queue(None)).await.unwrap();
    client
        .create_task(create(&receiver.url_for("t", "500,200")))
        .await
        .unwrap();

    let response = reqwest::get(format!(
        "{url}/emulator/projects/p/locations/l/queues/q:waitIdle"
    ))
    .await
    .unwrap();

    assert_eq!(
        (response.status(), receiver.received("t").len()),
        (StatusCode::NO_CONTENT, 2)
    );
}

#[tokio::test]
async fn wait_idle_rejects_unknown_queue() {
    let (_, url) = support::start_emulator_with(None).await;

    let response = reqwest::get(format!(
        "{url}/emulator/projects/p/locations/l/queues/q:waitIdle"
    ))
    .await
    .unwrap();

    assert_eq!(
        (response.status(), response.text().await.unwrap()),
        (
            StatusCode::NOT_FOUND,
            "Queue projects/p/locations/l/queues/q does not exist".to_owned()
        )
    );
}

#[tokio::test]
async fn wait_idle_rejects_paused_queue_with_tasks() {
    let (mut client, url) = support::start_emulator_with(None).await;
    client.create_queue(queue(None)).await.unwrap();
    client
        .pause_queue(PauseQueueRequest {
            name: "projects/p/locations/l/queues/q".to_owned(),
        })
        .await
        .unwrap();
    client
        .create_task(create("http://127.0.0.1:1/t"))
        .await
        .unwrap();

    let response = reqwest::get(format!(
        "{url}/emulator/projects/p/locations/l/queues/q:waitIdle"
    ))
    .await
    .unwrap();

    assert_eq!(
        (response.status(), response.text().await.unwrap()),
        (
            StatusCode::CONFLICT,
            "Queue projects/p/locations/l/queues/q is paused with tasks left".to_owned()
        )
    );
}
