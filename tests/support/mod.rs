#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::cloud_tasks_client::CloudTasksClient;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{GetTaskRequest, Task, task};
use tokio::net::TcpListener;
use tonic::Status;
use tonic::transport::Channel;

pub async fn start_emulator() -> CloudTasksClient<Channel> {
    start_emulator_with_url().await.0
}

/// Starts emulator whose OIDC issuer is its own URL, returned with client
pub async fn start_emulator_with_url() -> (CloudTasksClient<Channel>, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(cloud_tasks_emulator::serve(
        listener,
        url.clone(),
        Vec::new(),
    ));
    let client = CloudTasksClient::connect(url.clone()).await.unwrap();
    (client, url)
}

#[derive(Clone)]
pub struct Receiver {
    pub url: String,
    requests: Arc<Mutex<Vec<Received>>>,
}

#[derive(Clone)]
pub struct Received {
    pub path: String,
    pub query: Option<String>,
    pub time: SystemTime,
    pub method: Method,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Receiver {
    pub async fn start() -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback(respond).with_state(requests.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, requests }
    }

    /// URL whose requests get each comma-separated step in turn, repeating last
    pub fn url_for(&self, key: &str, steps: &str) -> String {
        format!("{}/{key}/{steps}", self.url)
    }

    /// Requests so far to paths under `/<key>/`
    pub fn received(&self, key: &str) -> Vec<Received> {
        let prefix = format!("/{key}/");
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path.starts_with(&prefix))
            .cloned()
            .collect()
    }

    pub async fn wait_for(&self, key: &str, count: usize) -> Vec<Received> {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let requests = self.received(key);
                if requests.len() >= count {
                    return requests;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{count} requests to {key} in time"))
    }
}

async fn respond(
    State(requests): State<Arc<Mutex<Vec<Received>>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_owned();
    let attempt = {
        let mut requests = requests.lock().unwrap();
        let attempt = requests
            .iter()
            .filter(|request| request.path == path)
            .count();
        requests.push(Received {
            path: path.clone(),
            query: uri.query().map(str::to_owned),
            time: SystemTime::now(),
            method,
            headers,
            body,
        });
        attempt
    };
    let (key, steps) = path.rsplit_once('/').unwrap();
    let steps: Vec<&str> = steps.split(',').collect();
    match steps[attempt.min(steps.len() - 1)] {
        // Outlasts 15s dispatch deadline that slow cases set
        "slow" => {
            tokio::time::sleep(Duration::from_secs(20)).await;
            StatusCode::OK.into_response()
        }
        "redirect" => (
            StatusCode::FOUND,
            [(header::LOCATION, format!("{key}/200"))],
        )
            .into_response(),
        status => StatusCode::from_u16(status.parse().unwrap())
            .unwrap()
            .into_response(),
    }
}

/// Polls task until `done` holds, or returns error once it is gone
pub async fn poll_task(
    client: &mut CloudTasksClient<Channel>,
    name: &str,
    done: impl Fn(&Task) -> bool,
) -> Result<Task, Status> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let request = GetTaskRequest {
                name: name.to_owned(),
                response_view: task::View::Full.into(),
            };
            let task = client.get_task(request).await?.into_inner();
            if done(&task) {
                return Ok(task);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("task reached the expected state in time")
}
