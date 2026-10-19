//! Compares the emulator with transcripts recorded from real Cloud Tasks in `tests/recordings`
//! To record, install `cloudflared` and run `CLOUD_TASKS_PROJECT=<id> CLOUD_TASKS_LOCATION=<id> CLOUD_TASKS_TOKEN=$(gcloud auth print-access-token) cargo test --test conformance -- --ignored record`

mod support;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::process::Stdio;
use std::time::SystemTime;

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::cloud_tasks_client::CloudTasksClient;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    CreateQueueRequest, CreateTaskRequest, DeleteQueueRequest, DeleteTaskRequest, GetQueueRequest,
    GetTaskRequest, HttpMethod, HttpRequest, ListQueuesRequest, ListQueuesResponse,
    ListTasksRequest, ListTasksResponse, Queue, RateLimits, RetryConfig, Task, task,
};
use prost_types::{Duration, Timestamp};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::support::{Received, Receiver};

const RECORDINGS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/recordings");

#[tokio::test]
async fn emulator_matches_recordings() {
    let target = Target::new(
        support::start_emulator().await,
        MetadataMap::new(),
        "emulator-project",
        "emulator-location",
        Receiver::start().await,
    );

    let results = run_cases(&target).await;

    let differences: Vec<String> = results
        .iter()
        .filter_map(|(case, transcript, _)| {
            let recording = fs::read_to_string(format!("{RECORDINGS}/{case}.txt"))
                .unwrap_or_else(|_| panic!("{case} has no recording"));
            (recording != *transcript)
                .then(|| format!("{case}\n--- recorded\n{recording}--- emulator\n{transcript}"))
        })
        .collect();
    assert_eq!(
        fs::read_dir(RECORDINGS).unwrap().count(),
        results.len(),
        "every recording has a case"
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

#[tokio::test]
#[ignore = "records real Cloud Tasks"]
async fn record() {
    let env = |key: &str| std::env::var(key).unwrap_or_else(|_| panic!("{key} is required"));
    let mut metadata = MetadataMap::new();
    metadata.insert(
        "authorization",
        format!("Bearer {}", env("CLOUD_TASKS_TOKEN"))
            .parse()
            .unwrap(),
    );
    let client = CloudTasksClient::connect("https://cloudtasks.googleapis.com")
        .await
        .unwrap();
    let mut receiver = Receiver::start().await;
    let (_tunnel, url) = open_tunnel(&receiver.url).await;
    receiver.url = url;
    let target = Target::new(
        client,
        metadata,
        &env("CLOUD_TASKS_PROJECT"),
        &env("CLOUD_TASKS_LOCATION"),
        receiver,
    );

    let results = run_cases(&target).await;
    for queue in results.iter().flat_map(|(_, _, queues)| queues) {
        target
            .clone()
            .delete_queue(DeleteQueueRequest {
                name: queue.clone(),
            })
            .await
            .unwrap();
    }

    fs::create_dir_all(RECORDINGS).unwrap();
    for entry in fs::read_dir(RECORDINGS).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    for (case, transcript, _) in results {
        fs::write(format!("{RECORDINGS}/{case}.txt"), transcript).unwrap();
    }
}

/// Makes a local URL public through a Cloudflare quick tunnel, which lasts as long as the returned process
async fn open_tunnel(local_url: &str) -> (Child, String) {
    let mut tunnel = Command::new("cloudflared")
        .args(["tunnel", "--no-autoupdate", "--url", local_url])
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("cloudflared is installed");
    let mut lines = BufReader::new(tunnel.stderr.take().unwrap()).lines();
    let url = loop {
        let line = lines
            .next_line()
            .await
            .unwrap()
            .expect("cloudflared prints the tunnel URL");
        if let Some(url) = line
            .split_whitespace()
            .find(|word| word.starts_with("https://") && word.ends_with(".trycloudflare.com"))
        {
            break url.to_owned();
        }
    };
    while !lines
        .next_line()
        .await
        .unwrap()
        .expect("cloudflared registers the tunnel")
        .contains("Registered tunnel connection")
    {}
    // Drains the log so cloudflared never blocks on a full pipe
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    // The host resolves a few seconds after registering, and a lookup before then is cached as missing for 60s
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    let client = reqwest::Client::new();
    for _ in 0..60 {
        if client
            .get(format!("{url}/ready/200"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return (tunnel, url);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    panic!("tunnel {url} never answered");
}

#[derive(Clone)]
struct Target {
    client: CloudTasksClient<Channel>,
    /// Sent with every request, so empty for the emulator
    metadata: MetadataMap,
    project: String,
    parent: String,
    /// Prefixes queue IDs, as real Cloud Tasks blocks a deleted queue's name for days
    run: String,
    /// A day out, so no task dispatches
    tomorrow: Timestamp,
    receiver: Receiver,
    placeholders: Vec<(String, &'static str)>,
    queues: Vec<String>,
    transcript: String,
}

impl Target {
    fn new(
        client: CloudTasksClient<Channel>,
        metadata: MetadataMap,
        project: &str,
        location: &str,
        receiver: Receiver,
    ) -> Self {
        let run = format!("r{:08x}", rand::random::<u32>());
        let tomorrow = seconds_from_now(24 * 60 * 60);
        Self {
            client,
            metadata,
            project: format!("projects/{project}"),
            parent: format!("projects/{project}/locations/{location}"),
            placeholders: vec![
                (receiver.url.clone(), "{receiver}"),
                (project.to_owned(), "{project}"),
                (location.to_owned(), "{location}"),
                (run.clone(), "{run}"),
                (tomorrow.to_string(), "{tomorrow}"),
            ],
            run,
            tomorrow: Timestamp {
                seconds: tomorrow,
                nanos: 123_456_789,
            },
            receiver,
            queues: Vec::new(),
            transcript: String::new(),
        }
    }

    fn queue_name(&self, id: &str) -> String {
        format!("{}/queues/{}-{id}", self.parent, self.run)
    }

    /// Creates a queue with the default config and returns its name
    async fn queue(&mut self, id: &str) -> String {
        self.queue_with(id, Queue::default()).await
    }

    /// Creates a queue with the given config and returns its name
    async fn queue_with(&mut self, id: &str, queue: Queue) -> String {
        let name = self.queue_name(id);
        self.create_queue(CreateQueueRequest {
            parent: self.parent.clone(),
            queue: Some(Queue {
                name: name.clone(),
                ..queue
            }),
        })
        .await
        .unwrap();
        name
    }

    fn task(&self, name: &str, http_request: HttpRequest) -> Task {
        Task {
            name: name.to_owned(),
            message_type: Some(task::MessageType::HttpRequest(http_request)),
            schedule_time: Some(self.tomorrow),
            ..Default::default()
        }
    }

    /// Replaces `value` with `placeholder` in this case's transcript
    fn placeholder(&mut self, value: String, placeholder: &'static str) {
        self.placeholders.push((value, placeholder));
    }

    fn record(&mut self, label: &str, result: Result<impl Debug, Status>) {
        let entry = match result {
            Ok(value) => format!("{value:#?}"),
            Err(status) => format!("{:?}: {}", status.code(), status.message()),
        };
        self.transcript += &format!("{label} -> {entry}\n");
    }

    /// Returns the transcript with run-specific values replaced, most specific first
    fn finish(self) -> (String, Vec<String>) {
        let mut transcript = self.transcript;
        for (value, placeholder) in self.placeholders.iter().rev() {
            transcript = transcript.replace(value, placeholder);
        }
        (transcript, self.queues)
    }

    /// Polls the task in full view until `done` holds for the result
    async fn poll_task(
        &mut self,
        name: &str,
        done: impl Fn(&Result<Task, Status>) -> bool,
    ) -> Result<Task, Status> {
        tokio::time::timeout(std::time::Duration::from_secs(120), async {
            loop {
                let result = self
                    .get_task(GetTaskRequest {
                        name: name.to_owned(),
                        response_view: task::View::Full.into(),
                    })
                    .await;
                if done(&result) {
                    return result;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{name} reached the expected state in time"))
    }

    /// Adds the routing header that official clients send and real Cloud Tasks requires
    fn request<T>(&self, routing: String, message: T) -> Request<T> {
        let mut request = Request::new(message);
        *request.metadata_mut() = self.metadata.clone();
        request
            .metadata_mut()
            .insert("x-goog-request-params", routing.parse().unwrap());
        request
    }

    async fn create_queue(&mut self, request: CreateQueueRequest) -> Result<Queue, Status> {
        let request = self.request(format!("parent={}", request.parent), request);
        let queue = self.client.create_queue(request).await?.into_inner();
        self.queues.push(queue.name.clone());
        Ok(queue)
    }

    async fn get_queue(&mut self, request: GetQueueRequest) -> Result<Queue, Status> {
        let request = self.request(format!("name={}", request.name), request);
        Ok(self.client.get_queue(request).await?.into_inner())
    }

    async fn list_queues(
        &mut self,
        request: ListQueuesRequest,
    ) -> Result<ListQueuesResponse, Status> {
        let request = self.request(format!("parent={}", request.parent), request);
        Ok(self.client.list_queues(request).await?.into_inner())
    }

    async fn delete_queue(&mut self, request: DeleteQueueRequest) -> Result<(), Status> {
        let name = request.name.clone();
        let request = self.request(format!("name={name}"), request);
        self.client.delete_queue(request).await?;
        self.queues.retain(|queue| *queue != name);
        Ok(())
    }

    async fn create_task(&mut self, request: CreateTaskRequest) -> Result<Task, Status> {
        let request = self.request(format!("parent={}", request.parent), request);
        let task = self.client.create_task(request).await?.into_inner();
        self.placeholder(
            task.create_time.unwrap().seconds.to_string(),
            "{create_time}",
        );
        Ok(task)
    }

    async fn get_task(&mut self, request: GetTaskRequest) -> Result<Task, Status> {
        let request = self.request(format!("name={}", request.name), request);
        let task = self.client.get_task(request).await?.into_inner();
        self.placeholder(
            task.create_time.unwrap().seconds.to_string(),
            "{create_time}",
        );
        Ok(task)
    }

    async fn list_tasks(&mut self, request: ListTasksRequest) -> Result<ListTasksResponse, Status> {
        let request = self.request(format!("parent={}", request.parent), request);
        let response = self.client.list_tasks(request).await?.into_inner();
        for task in &response.tasks {
            self.placeholder(
                task.create_time.unwrap().seconds.to_string(),
                "{create_time}",
            );
        }
        Ok(response)
    }

    async fn delete_task(&mut self, request: DeleteTaskRequest) -> Result<(), Status> {
        let request = self.request(format!("name={}", request.name), request);
        self.client.delete_task(request).await?;
        Ok(())
    }
}

fn seconds_from_now(seconds: i64) -> i64 {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    now.as_secs() as i64 + seconds
}

macro_rules! cases {
    ($($case:ident),* $(,)?) => {
        /// Runs every case at once and returns each one's transcript and leftover queues
        async fn run_cases(target: &Target) -> Vec<(&'static str, String, Vec<String>)> {
            let cases = vec![$({
                let mut target = target.clone();
                tokio::spawn(async move {
                    $case(&mut target).await;
                    let (transcript, queues) = target.finish();
                    (stringify!($case), transcript, queues)
                })
            }),*];
            let mut results = Vec::new();
            for case in cases {
                results.push(case.await.unwrap());
            }
            results
        }
    };
}

cases!(
    create_queue_returns_defaults,
    create_queue_returns_config,
    create_queue_keeps_backoff_precision,
    create_queue_derives_burst_size,
    create_queue_with_unlimited_retries,
    create_queue_treats_zero_config_as_unset,
    create_queue_rejects_invalid_config,
    create_queue_rejects_invalid_names,
    get_queue_rejects_invalid_names,
    list_queues_returns_every_queue,
    list_queues_pages_by_page_size,
    list_queues_rejects_invalid_requests,
    delete_queue_removes_queue,
    delete_queue_rejects_invalid_names,
    create_task_returns_basic_view,
    create_task_returns_full_view,
    create_task_drops_reserved_headers,
    create_task_sanitises_headers,
    create_task_limits_header_length,
    create_task_truncates_deadline,
    create_task_generates_numeric_id,
    create_task_rejects_invalid_requests,
    create_task_rejects_invalid_names,
    get_task_returns_basic_and_full_views,
    get_task_rejects_invalid_names,
    list_tasks_returns_every_task,
    list_tasks_pages_by_page_size,
    list_tasks_rejects_invalid_requests,
    delete_task_removes_task,
    delete_task_rejects_invalid_names,
    unknown_enums_fall_back_to_defaults,
    dispatch_sends_cloud_tasks_headers,
    dispatch_reports_previous_responses,
    dispatch_reports_504_after_timeout,
    dispatch_follows_redirects,
    dispatch_records_response_status,
    retries_stop_at_max_attempts,
    retries_stop_at_max_retry_duration,
    retries_stop_once_both_limits_pass,
    retries_back_off_exponentially_then_linearly,
);

fn http(url: &str) -> HttpRequest {
    HttpRequest {
        url: url.to_owned(),
        ..Default::default()
    }
}

fn with_headers(headers: &[(&str, &str)]) -> HttpRequest {
    HttpRequest {
        headers: headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        ..http("https://example.com/work")
    }
}

async fn create_queue_returns_defaults(t: &mut Target) {
    let request = CreateQueueRequest {
        parent: t.parent.clone(),
        queue: Some(Queue {
            name: t.queue_name("defaults"),
            ..Default::default()
        }),
    };

    let result = t.create_queue(request).await;

    t.record("create_queue", result);
}

async fn create_queue_returns_config(t: &mut Target) {
    let request = CreateQueueRequest {
        parent: t.parent.clone(),
        queue: Some(Queue {
            name: t.queue_name("config"),
            rate_limits: Some(RateLimits {
                max_dispatches_per_second: 10.0,
                max_burst_size: 7,
                max_concurrent_dispatches: 5,
            }),
            retry_config: Some(RetryConfig {
                max_attempts: 5,
                max_retry_duration: Some(Duration {
                    seconds: 60,
                    nanos: 0,
                }),
                min_backoff: Some(Duration {
                    seconds: 1,
                    nanos: 0,
                }),
                max_backoff: Some(Duration {
                    seconds: 10,
                    nanos: 0,
                }),
                max_doublings: 3,
            }),
            ..Default::default()
        }),
    };

    let result = t.create_queue(request).await;

    t.record("create_queue", result);
}

async fn create_queue_keeps_backoff_precision(t: &mut Target) {
    let request = CreateQueueRequest {
        parent: t.parent.clone(),
        queue: Some(Queue {
            name: t.queue_name("precision"),
            retry_config: Some(RetryConfig {
                min_backoff: Some(Duration {
                    seconds: 1,
                    nanos: 234_567_891,
                }),
                max_backoff: Some(Duration {
                    seconds: 10,
                    nanos: 987_654_321,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };

    let result = t.create_queue(request).await;

    t.record("create_queue", result.map(|queue| queue.retry_config));
}

async fn create_queue_derives_burst_size(t: &mut Target) {
    for (id, rate) in [
        ("burst-half", 0.5),
        ("burst-54", 54.0),
        ("burst-77", 77.0),
        ("burst-333", 333.0),
        ("burst-500", 500.0),
    ] {
        let request = CreateQueueRequest {
            parent: t.parent.clone(),
            queue: Some(Queue {
                name: t.queue_name(id),
                rate_limits: Some(RateLimits {
                    max_dispatches_per_second: rate,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let result = t.create_queue(request).await;

        t.record(
            &format!("rate {rate}"),
            result.map(|queue| queue.rate_limits.unwrap().max_burst_size),
        );
    }
}

async fn create_queue_with_unlimited_retries(t: &mut Target) {
    let request = CreateQueueRequest {
        parent: t.parent.clone(),
        queue: Some(Queue {
            name: t.queue_name("unlimited"),
            retry_config: Some(RetryConfig {
                max_attempts: -1,
                max_retry_duration: Some(Duration {
                    seconds: 0,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };

    let result = t.create_queue(request).await;

    t.record("create_queue", result.map(|queue| queue.retry_config));
}

async fn create_queue_treats_zero_config_as_unset(t: &mut Target) {
    let request = CreateQueueRequest {
        parent: t.parent.clone(),
        queue: Some(Queue {
            name: t.queue_name("zero"),
            rate_limits: Some(RateLimits::default()),
            retry_config: Some(RetryConfig {
                min_backoff: Some(Duration::default()),
                max_backoff: Some(Duration::default()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };

    let result = t.create_queue(request).await;

    t.record("create_queue", result);
}

async fn create_queue_rejects_invalid_config(t: &mut Target) {
    let negative = Some(Duration {
        seconds: -1,
        nanos: 0,
    });
    for (label, rate_limits, retry_config) in [
        (
            "rate 501",
            RateLimits {
                max_dispatches_per_second: 501.0,
                ..Default::default()
            },
            RetryConfig::default(),
        ),
        (
            "rate -1",
            RateLimits {
                max_dispatches_per_second: -1.0,
                ..Default::default()
            },
            RetryConfig::default(),
        ),
        (
            "concurrency 5001",
            RateLimits {
                max_concurrent_dispatches: 5001,
                ..Default::default()
            },
            RetryConfig::default(),
        ),
        (
            "concurrency -1",
            RateLimits {
                max_concurrent_dispatches: -1,
                ..Default::default()
            },
            RetryConfig::default(),
        ),
        (
            "max attempts -2",
            RateLimits::default(),
            RetryConfig {
                max_attempts: -2,
                ..Default::default()
            },
        ),
        (
            "max doublings -1",
            RateLimits::default(),
            RetryConfig {
                max_doublings: -1,
                ..Default::default()
            },
        ),
        (
            "min backoff -1s",
            RateLimits::default(),
            RetryConfig {
                min_backoff: negative,
                ..Default::default()
            },
        ),
        (
            "min backoff -5ns",
            RateLimits::default(),
            RetryConfig {
                min_backoff: Some(Duration {
                    seconds: 0,
                    nanos: -5,
                }),
                ..Default::default()
            },
        ),
        (
            "max backoff -1s",
            RateLimits::default(),
            RetryConfig {
                max_backoff: negative,
                ..Default::default()
            },
        ),
        (
            "max retry duration -1s",
            RateLimits::default(),
            RetryConfig {
                max_retry_duration: negative,
                ..Default::default()
            },
        ),
        (
            "max retry duration 30.9s",
            RateLimits::default(),
            RetryConfig {
                max_retry_duration: Some(Duration {
                    seconds: 30,
                    nanos: 900_000_000,
                }),
                ..Default::default()
            },
        ),
        (
            "min backoff above max backoff",
            RateLimits::default(),
            RetryConfig {
                min_backoff: Some(Duration {
                    seconds: 10,
                    nanos: 0,
                }),
                max_backoff: Some(Duration {
                    seconds: 5,
                    nanos: 0,
                }),
                ..Default::default()
            },
        ),
    ] {
        let request = CreateQueueRequest {
            parent: t.parent.clone(),
            queue: Some(Queue {
                name: t.queue_name(&label.replace([' ', '.'], "-")),
                rate_limits: Some(rate_limits),
                retry_config: Some(retry_config),
                ..Default::default()
            }),
        };

        let result = t.create_queue(request).await;

        t.record(label, result);
    }
}

async fn create_queue_rejects_invalid_names(t: &mut Target) {
    let existing = t.queue("existing").await;
    let named = |name: String| {
        Some(Queue {
            name,
            ..Default::default()
        })
    };
    for (label, parent, queue) in [
        ("no queue", t.parent.clone(), None),
        ("malformed name", t.parent.clone(), named("bad".to_owned())),
        (
            "invalid queue id",
            t.parent.clone(),
            named(format!("{}/queues/not_valid", t.parent)),
        ),
        (
            "queue id over 100 characters",
            t.parent.clone(),
            named(format!("{}/queues/{}", t.parent, "a".repeat(101))),
        ),
        (
            "name in other location",
            t.parent.clone(),
            named(format!(
                "{}/locations/us-central1/queues/{}-elsewhere",
                t.project, t.run
            )),
        ),
        ("existing name", t.parent.clone(), named(existing.clone())),
        (
            "parent not in a project",
            "not-a-valid-parent".to_owned(),
            named("not-a-valid-parent/queues/q".to_owned()),
        ),
        (
            "parent without location",
            t.project.clone(),
            named(format!("{}/queues/q", t.project)),
        ),
    ] {
        let result = t.create_queue(CreateQueueRequest { parent, queue }).await;

        t.record(label, result);
    }
}

async fn get_queue_rejects_invalid_names(t: &mut Target) {
    for (label, name) in [
        ("unknown queue", t.queue_name("missing")),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without queue id", format!("{}/queues", t.parent)),
        ("invalid queue id", format!("{}/queues/not_valid", t.parent)),
    ] {
        let result = t.get_queue(GetQueueRequest { name }).await;

        t.record(label, result);
    }
}

async fn list_queues_returns_every_queue(t: &mut Target) {
    t.queue("list-b").await;
    t.queue("list-a").await;

    let result = t
        .list_queues(ListQueuesRequest {
            parent: t.parent.clone(),
            ..Default::default()
        })
        .await;

    // Other cases' queues share the location
    t.record(
        "list_queues",
        result.map(|response| {
            let mut names: Vec<String> = response
                .queues
                .into_iter()
                .map(|queue| queue.name)
                .filter(|name| name.contains("-list-"))
                .collect();
            names.sort();
            names
        }),
    );
}

async fn list_queues_pages_by_page_size(t: &mut Target) {
    t.queue("page-a").await;
    t.queue("page-b").await;

    let result = t
        .list_queues(ListQueuesRequest {
            parent: t.parent.clone(),
            page_size: 1,
            ..Default::default()
        })
        .await;

    t.record(
        "page size 1",
        result.map(|response| (response.queues.len(), !response.next_page_token.is_empty())),
    );
}

async fn list_queues_rejects_invalid_requests(t: &mut Target) {
    for (label, request) in [
        (
            "page size -1",
            ListQueuesRequest {
                parent: t.parent.clone(),
                page_size: -1,
                ..Default::default()
            },
        ),
        (
            "invalid page token",
            ListQueuesRequest {
                parent: t.parent.clone(),
                page_token: "bad".to_owned(),
                ..Default::default()
            },
        ),
        (
            "empty parent",
            ListQueuesRequest {
                parent: String::new(),
                ..Default::default()
            },
        ),
        (
            "parent not in a project",
            ListQueuesRequest {
                parent: "not-a-valid-parent".to_owned(),
                ..Default::default()
            },
        ),
        (
            "parent without location",
            ListQueuesRequest {
                parent: t.project.clone(),
                ..Default::default()
            },
        ),
        (
            "parent with extra segment",
            ListQueuesRequest {
                parent: format!("{}/extra", t.parent),
                ..Default::default()
            },
        ),
    ] {
        let result = t.list_queues(request).await;

        t.record(label, result);
    }
}

async fn delete_queue_removes_queue(t: &mut Target) {
    let name = t.queue("deleted").await;

    let result = t
        .delete_queue(DeleteQueueRequest { name: name.clone() })
        .await;

    t.record("delete_queue", result);
    let result = t.get_queue(GetQueueRequest { name }).await;
    t.record("get_queue", result);
}

async fn delete_queue_rejects_invalid_names(t: &mut Target) {
    for (label, name) in [
        ("unknown queue", t.queue_name("missing")),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without queue id", format!("{}/queues", t.parent)),
        ("invalid queue id", format!("{}/queues/not_valid", t.parent)),
    ] {
        let result = t.delete_queue(DeleteQueueRequest { name }).await;

        t.record(label, result);
    }
}

async fn create_task_returns_basic_view(t: &mut Target) {
    let queue = t.queue("basic").await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(t.task(
            &format!("{queue}/tasks/t"),
            HttpRequest {
                url: "https://example.com/work".to_owned(),
                headers: BTreeMap::from([
                    ("X-Mixed-Case".to_owned(), "kept".to_owned()),
                    ("content-type".to_owned(), "text/plain".to_owned()),
                ]),
                body: b"hi".to_vec(),
                ..Default::default()
            },
        )),
        response_view: task::View::Unspecified.into(),
    };

    let result = t.create_task(request).await;

    t.record("create_task", result);
}

async fn create_task_returns_full_view(t: &mut Target) {
    let queue = t.queue("full").await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(t.task(
            &format!("{queue}/tasks/t"),
            HttpRequest {
                url: "https://example.com/work".to_owned(),
                body: b"hi".to_vec(),
                ..Default::default()
            },
        )),
        response_view: task::View::Full.into(),
    };

    let result = t.create_task(request).await;

    t.record("create_task", result);
}

async fn create_task_drops_reserved_headers(t: &mut Target) {
    let queue = t.queue("reserved-headers").await;
    let request = CreateTaskRequest {
        parent: queue,
        task: Some(t.task(
            "",
            with_headers(&[
                ("user-agent", "custom"),
                ("Host", "example.org"),
                ("Content-Length", "99"),
                ("Connection", "close"),
                ("Keep-Alive", "timeout=5"),
                ("Proxy-Authorization", "secret"),
                ("Proxy-Connection", "close"),
                ("TE", "trailers"),
                ("Transfer-Encoding", "chunked"),
                ("Upgrade", "h2c"),
                ("Via", "proxy"),
                ("Accept-Encoding", "gzip"),
                ("X-Forwarded-For", "10.0.0.1"),
                ("From", "me@example.com"),
                ("X-Google", "value"),
                ("X-Google-Custom", "value"),
                ("x-appengine-custom", "value"),
                ("Authorization", "Bearer token"),
                ("Content-Type", "text/plain"),
                ("Trailer", "Expires"),
                ("X-Forwarded-Proto", "https"),
                ("X-Goog-Custom", "value"),
                ("X-CloudTasks-TaskName", "spoofed"),
            ]),
        )),
        response_view: task::View::Unspecified.into(),
    };

    let result = t.create_task(request).await;

    t.record("create_task", result.map(|task| task.message_type));
}

async fn create_task_sanitises_headers(t: &mut Target) {
    let queue = t.queue("sanitised-headers").await;
    let request = CreateTaskRequest {
        parent: queue,
        task: Some(t.task(
            "",
            with_headers(&[
                ("Bad Header", "value"),
                ("X\tTab", "value"),
                ("X:Colon", "value"),
                ("", "value"),
                ("X-Ü", "value"),
                ("X/Slash", "value"),
                ("X-Newline", "a\nb"),
                ("X-Crlf", "a\r\nb"),
                ("X-Tab", "a\tb"),
                ("X-Nul", "a\0b"),
                ("X-Control", "a\x01b"),
                ("X-Delete", "a\x7fb"),
                ("X-Unicode", "ü"),
                ("X-Empty", ""),
                ("X-Space", " "),
            ]),
        )),
        response_view: task::View::Unspecified.into(),
    };

    let result = t.create_task(request).await;

    t.record("create_task", result.map(|task| task.message_type));
}

async fn create_task_limits_header_length(t: &mut Target) {
    let queue = t.queue("header-length").await;
    let request = CreateTaskRequest {
        parent: queue,
        task: Some(t.task(
            "",
            with_headers(&[("X-Kept", &"v".repeat(8182)), ("X-Gone", &"v".repeat(8183))]),
        )),
        response_view: task::View::Unspecified.into(),
    };

    let result = t.create_task(request).await;

    t.record(
        "create_task header names",
        result.map(|task| match task.message_type {
            Some(task::MessageType::HttpRequest(http_request)) => {
                http_request.headers.into_keys().collect::<Vec<_>>()
            }
            _ => panic!("HTTP task"),
        }),
    );
}

async fn create_task_truncates_deadline(t: &mut Target) {
    let queue = t.queue("deadline").await;
    let request = CreateTaskRequest {
        parent: queue,
        task: Some(Task {
            dispatch_deadline: Some(Duration {
                seconds: 20,
                nanos: 123_456_789,
            }),
            ..t.task("", http("https://example.com/work"))
        }),
        response_view: task::View::Unspecified.into(),
    };

    let result = t.create_task(request).await;

    t.record("create_task", result.map(|task| task.dispatch_deadline));
}

async fn create_task_generates_numeric_id(t: &mut Target) {
    let queue = t.queue("generated").await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(t.task("", http("https://example.com/work"))),
        response_view: task::View::Unspecified.into(),
    };

    let result = t.create_task(request).await;

    t.record(
        "create_task id is digits",
        result.map(|task| {
            task.name
                .strip_prefix(&format!("{queue}/tasks/"))
                .is_some_and(|id| id.bytes().all(|byte| byte.is_ascii_digit()))
        }),
    );
}

async fn create_task_rejects_invalid_requests(t: &mut Target) {
    let queue = t.queue("invalid-tasks").await;
    let with_body = |method: HttpMethod| HttpRequest {
        http_method: method.into(),
        body: b"hi".to_vec(),
        ..http("https://example.com/work")
    };
    let with_deadline = |seconds: i64| Task {
        dispatch_deadline: Some(Duration { seconds, nanos: 0 }),
        ..t.task("", http("https://example.com/work"))
    };
    for (label, task) in [
        ("no task", None),
        (
            "no target",
            Some(Task {
                message_type: None,
                ..t.task("", http(""))
            }),
        ),
        ("empty url", Some(t.task("", http("")))),
        ("ftp url", Some(t.task("", http("ftp://example.com/work")))),
        (
            "url without scheme",
            Some(t.task("", http("example.com/work"))),
        ),
        ("body on GET", Some(t.task("", with_body(HttpMethod::Get)))),
        (
            "body on HEAD",
            Some(t.task("", with_body(HttpMethod::Head))),
        ),
        (
            "body on DELETE",
            Some(t.task("", with_body(HttpMethod::Delete))),
        ),
        (
            "body on OPTIONS",
            Some(t.task("", with_body(HttpMethod::Options))),
        ),
        ("deadline 14s", Some(with_deadline(14))),
        ("deadline 1801s", Some(with_deadline(1801))),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task,
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.name));
    }
}

async fn create_task_rejects_invalid_names(t: &mut Target) {
    let queue = t.queue("invalid-names").await;
    let existing = format!("{queue}/tasks/existing");
    t.create_task(CreateTaskRequest {
        parent: queue.clone(),
        task: Some(t.task(&existing, http("https://example.com/work"))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();
    for (label, parent, name) in [
        ("malformed name", queue.clone(), "bad".to_owned()),
        (
            "invalid task id",
            queue.clone(),
            format!("{queue}/tasks/not valid"),
        ),
        (
            "task id over 500 characters",
            queue.clone(),
            format!("{queue}/tasks/{}", "a".repeat(501)),
        ),
        (
            "name in other queue",
            queue.clone(),
            format!("{}/tasks/t", t.queue_name("other")),
        ),
        ("existing name", queue.clone(), existing.clone()),
        ("unknown queue", t.queue_name("missing"), String::new()),
        (
            "parent not in a project",
            "not-a-valid-parent".to_owned(),
            String::new(),
        ),
        (
            "parent without queue id",
            format!("{}/queues", t.parent),
            String::new(),
        ),
        (
            "parent with invalid queue id",
            format!("{}/queues/not_valid", t.parent),
            String::new(),
        ),
    ] {
        let request = CreateTaskRequest {
            parent,
            task: Some(t.task(&name, http("https://example.com/work"))),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.name));
    }
}

async fn get_task_returns_basic_and_full_views(t: &mut Target) {
    let queue = t.queue("views").await;
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(t.task(
            &name,
            HttpRequest {
                url: "https://example.com/work".to_owned(),
                body: b"hi".to_vec(),
                ..Default::default()
            },
        )),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    let basic = t
        .get_task(GetTaskRequest {
            name: name.clone(),
            response_view: task::View::Unspecified.into(),
        })
        .await;
    let full = t
        .get_task(GetTaskRequest {
            name,
            response_view: task::View::Full.into(),
        })
        .await;

    t.record("get_task basic", basic);
    t.record("get_task full", full);
}

async fn get_task_rejects_invalid_names(t: &mut Target) {
    let queue = t.queue("get-tasks").await;
    for (label, name) in [
        ("unknown task", format!("{queue}/tasks/missing")),
        (
            "unknown queue",
            format!("{}/tasks/t", t.queue_name("missing")),
        ),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without task id", format!("{queue}/tasks")),
        ("invalid task id", format!("{queue}/tasks/not valid")),
        (
            "invalid queue id",
            format!("{}/queues/not_valid/tasks/t", t.parent),
        ),
        ("name with extra segment", format!("{queue}/tasks/t/extra")),
    ] {
        let result = t
            .get_task(GetTaskRequest {
                name,
                response_view: task::View::Unspecified.into(),
            })
            .await;

        t.record(label, result.map(|task| task.name));
    }
}

async fn list_tasks_returns_every_task(t: &mut Target) {
    let queue = t.queue("list-tasks").await;
    for id in ["c", "a", "b"] {
        t.create_task(CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                &format!("{queue}/tasks/{id}"),
                http("https://example.com/work"),
            )),
            response_view: task::View::Unspecified.into(),
        })
        .await
        .unwrap();
    }

    let result = t
        .list_tasks(ListTasksRequest {
            parent: queue,
            ..Default::default()
        })
        .await;

    // Real Cloud Tasks lists in no documented order
    t.record(
        "list_tasks",
        result.map(|response| {
            let mut names: Vec<String> = response.tasks.into_iter().map(|task| task.name).collect();
            names.sort();
            names
        }),
    );
}

async fn list_tasks_pages_by_page_size(t: &mut Target) {
    let queue = t.queue("page-tasks").await;
    for id in ["a", "b", "c"] {
        t.create_task(CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                &format!("{queue}/tasks/{id}"),
                http("https://example.com/work"),
            )),
            response_view: task::View::Unspecified.into(),
        })
        .await
        .unwrap();
    }

    let first = t
        .list_tasks(ListTasksRequest {
            parent: queue.clone(),
            page_size: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    let second = t
        .list_tasks(ListTasksRequest {
            parent: queue.clone(),
            page_size: 2,
            page_token: first.next_page_token.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let third = t
        .list_tasks(ListTasksRequest {
            parent: queue,
            page_size: 2,
            page_token: second.next_page_token.clone(),
            ..Default::default()
        })
        .await;

    t.record(
        "first page",
        Ok::<_, Status>((first.tasks.len(), !first.next_page_token.is_empty())),
    );
    t.record(
        "second page",
        Ok::<_, Status>((second.tasks.len(), !second.next_page_token.is_empty())),
    );
    t.record(
        "third page",
        third.map(|response| (response.tasks.len(), !response.next_page_token.is_empty())),
    );
}

async fn list_tasks_rejects_invalid_requests(t: &mut Target) {
    let queue = t.queue("list-invalid").await;
    for (label, request) in [
        (
            "page size -1",
            ListTasksRequest {
                parent: queue.clone(),
                page_size: -1,
                ..Default::default()
            },
        ),
        (
            "invalid page token",
            ListTasksRequest {
                parent: queue.clone(),
                page_token: "bad".to_owned(),
                ..Default::default()
            },
        ),
        (
            "unknown queue",
            ListTasksRequest {
                parent: t.queue_name("missing"),
                ..Default::default()
            },
        ),
        (
            "empty parent",
            ListTasksRequest {
                parent: String::new(),
                ..Default::default()
            },
        ),
        (
            "parent not in a project",
            ListTasksRequest {
                parent: "not-a-valid-parent".to_owned(),
                ..Default::default()
            },
        ),
        (
            "parent without queue id",
            ListTasksRequest {
                parent: format!("{}/queues", t.parent),
                ..Default::default()
            },
        ),
        (
            "parent with invalid queue id",
            ListTasksRequest {
                parent: format!("{}/queues/not_valid", t.parent),
                ..Default::default()
            },
        ),
    ] {
        let result = t.list_tasks(request).await;

        t.record(label, result);
    }
}

async fn delete_task_removes_task(t: &mut Target) {
    let queue = t.queue("delete-task").await;
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(t.task(&name, http("https://example.com/work"))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    let result = t.delete_task(DeleteTaskRequest { name }).await;

    t.record("delete_task", result);
}

async fn delete_task_rejects_invalid_names(t: &mut Target) {
    let queue = t.queue("delete-invalid").await;
    for (label, name) in [
        ("unknown task", format!("{queue}/tasks/missing")),
        (
            "unknown queue",
            format!("{}/tasks/t", t.queue_name("missing")),
        ),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without task id", format!("{queue}/tasks")),
        ("invalid task id", format!("{queue}/tasks/not valid")),
    ] {
        let result = t.delete_task(DeleteTaskRequest { name }).await;

        t.record(label, result);
    }
}

async fn unknown_enums_fall_back_to_defaults(t: &mut Target) {
    let queue = t.queue("unknown-enums").await;
    let name = format!("{queue}/tasks/t");

    let method = t
        .create_task(CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                "",
                HttpRequest {
                    http_method: 99,
                    ..http("https://example.com/work")
                },
            )),
            response_view: task::View::Unspecified.into(),
        })
        .await;
    let created = t
        .create_task(CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                &name,
                HttpRequest {
                    body: b"hi".to_vec(),
                    ..http("https://example.com/work")
                },
            )),
            response_view: 99,
        })
        .await;
    let got = t
        .get_task(GetTaskRequest {
            name,
            response_view: 99,
        })
        .await;
    let listed = t
        .list_tasks(ListTasksRequest {
            parent: queue,
            response_view: 99,
            ..Default::default()
        })
        .await;

    t.record(
        "create_task method 99",
        method.map(|task| task.message_type),
    );
    t.record(
        "create_task view 99",
        created.map(|task| (task::View::try_from(task.view), task.message_type)),
    );
    t.record(
        "get_task view 99",
        got.map(|task| (task::View::try_from(task.view), task.message_type)),
    );
    t.record(
        "list_tasks view 99",
        listed.map(|response| {
            response
                .tasks
                .iter()
                .map(|task| task::View::try_from(task.view))
                .collect::<Vec<_>>()
        }),
    );
}

fn seconds(seconds: i64) -> Option<Duration> {
    Some(Duration { seconds, nanos: 0 })
}

/// Headers Cloud Tasks sets or passes on, leaving out those the tunnel or HTTP client add
fn dispatched_headers(request: &Received) -> BTreeMap<String, String> {
    request
        .headers
        .iter()
        .filter(|(name, _)| {
            name.as_str().starts_with("x-cloudtasks-")
                || ["authorization", "content-type", "user-agent", "x-custom"]
                    .contains(&name.as_str())
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
        .collect()
}

/// Creates a task that dispatches now, unlike `Target::task`
fn due_task(name: &str, url: String) -> Task {
    Task {
        name: name.to_owned(),
        message_type: Some(task::MessageType::HttpRequest(http(&url))),
        ..Default::default()
    }
}

async fn dispatch_sends_cloud_tasks_headers(t: &mut Target) {
    let queue = t.queue("dispatch-headers").await;
    let eta = seconds_from_now(2);
    t.placeholder(eta.to_string(), "{eta}");
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(Task {
            name: format!("{queue}/tasks/t"),
            schedule_time: Some(Timestamp {
                seconds: eta,
                nanos: 120_000_000,
            }),
            message_type: Some(task::MessageType::HttpRequest(HttpRequest {
                url: t.receiver.url_for("headers", "200"),
                http_method: HttpMethod::Put.into(),
                headers: BTreeMap::from([
                    ("X-Custom".to_owned(), "value".to_owned()),
                    ("Authorization".to_owned(), "Bearer token".to_owned()),
                    ("Content-Type".to_owned(), "text/plain".to_owned()),
                    ("X-CloudTasks-TaskName".to_owned(), "spoofed".to_owned()),
                ]),
                body: b"hi".to_vec(),
                authorization_header: None,
            })),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    };

    t.create_task(request).await.unwrap();

    let requests = t.receiver.wait_for("headers", 1).await;
    t.record(
        "request",
        Ok::<_, Status>((
            requests[0].method.clone(),
            dispatched_headers(&requests[0]),
            String::from_utf8_lossy(&requests[0].body).into_owned(),
        )),
    );
}

async fn dispatch_reports_previous_responses(t: &mut Target) {
    let queue = t
        .queue_with(
            "dispatch-retries",
            Queue {
                retry_config: Some(RetryConfig {
                    max_attempts: 10,
                    min_backoff: seconds(1),
                    max_backoff: seconds(1),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(due_task(
            &format!("{queue}/tasks/t"),
            t.receiver.url_for("retries", "503,404,429,500,302,200"),
        )),
        response_view: task::View::Unspecified.into(),
    };

    t.create_task(request).await.unwrap();

    for (attempt, request) in t.receiver.wait_for("retries", 6).await.iter().enumerate() {
        let mut headers = dispatched_headers(request);
        headers.remove("x-cloudtasks-tasketa");
        t.record(
            &format!("attempt {}", attempt + 1),
            Ok::<_, Status>(headers),
        );
    }
}

async fn dispatch_reports_504_after_timeout(t: &mut Target) {
    let queue = t
        .queue_with(
            "dispatch-timeout",
            Queue {
                retry_config: Some(RetryConfig {
                    max_attempts: 2,
                    min_backoff: seconds(1),
                    max_backoff: seconds(1),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(Task {
            dispatch_deadline: seconds(15),
            ..due_task(
                &format!("{queue}/tasks/t"),
                t.receiver.url_for("timeout", "slow,200"),
            )
        }),
        response_view: task::View::Unspecified.into(),
    };

    t.create_task(request).await.unwrap();

    let mut headers = dispatched_headers(&t.receiver.wait_for("timeout", 2).await[1]);
    headers.remove("x-cloudtasks-tasketa");
    t.record("attempt 2", Ok::<_, Status>(headers));
}

async fn dispatch_follows_redirects(t: &mut Target) {
    let queue = t.queue("dispatch-redirect").await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(due_task(
            &format!("{queue}/tasks/t"),
            t.receiver.url_for("redirect", "redirect"),
        )),
        response_view: task::View::Unspecified.into(),
    };

    t.create_task(request).await.unwrap();

    let requests = t.receiver.wait_for("redirect", 2).await;
    t.record(
        "requests",
        Ok::<_, Status>(
            requests
                .iter()
                .map(|request| (request.method.clone(), request.path.clone()))
                .collect::<Vec<_>>(),
        ),
    );
}

async fn dispatch_records_response_status(t: &mut Target) {
    // An hour's backoff keeps each task, and its last attempt, after the first failure
    let queue = t
        .queue_with(
            "dispatch-statuses",
            Queue {
                retry_config: Some(RetryConfig {
                    max_attempts: 2,
                    min_backoff: seconds(3600),
                    max_backoff: seconds(3600),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    let steps = [
        "301",
        "400",
        "401",
        "403",
        "404",
        "405",
        "409",
        "410",
        "418",
        "429",
        "499",
        "500",
        "501",
        "502",
        "503",
        "504",
        "505",
        "507",
        "slow",
        "unreachable",
    ];
    for step in steps {
        let url = match step {
            "unreachable" => "https://unreachable.invalid/work".to_owned(),
            step => t.receiver.url_for(&format!("status-{step}"), step),
        };
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                dispatch_deadline: seconds(15),
                ..due_task(&format!("{queue}/tasks/{step}"), url)
            }),
            response_view: task::View::Unspecified.into(),
        };
        t.create_task(request).await.unwrap();
    }

    for step in steps {
        let result = t
            .poll_task(&format!("{queue}/tasks/{step}"), |result| {
                result.as_ref().is_ok_and(|task| {
                    task.last_attempt
                        .as_ref()
                        .is_some_and(|attempt| attempt.response_status.is_some())
                })
            })
            .await;

        t.record(
            step,
            result.map(|task| {
                (
                    task.dispatch_count,
                    task.response_count,
                    task.last_attempt.unwrap().response_status,
                )
            }),
        );
    }
}

/// Fails every attempt and returns how many Cloud Tasks made before giving up
async fn attempts_until_given_up(t: &mut Target, key: &str, retry_config: RetryConfig) -> usize {
    let queue = t
        .queue_with(
            key,
            Queue {
                retry_config: Some(retry_config),
                ..Default::default()
            },
        )
        .await;
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(due_task(&name, t.receiver.url_for(key, "500"))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();
    t.poll_task(&name, Result::is_err).await.unwrap_err();
    t.receiver.received(key).len()
}

async fn retries_stop_at_max_attempts(t: &mut Target) {
    let attempts = attempts_until_given_up(
        t,
        "stop-attempts",
        RetryConfig {
            max_attempts: 3,
            min_backoff: seconds(1),
            max_backoff: seconds(1),
            ..Default::default()
        },
    )
    .await;

    t.record("3 attempts, unlimited time", Ok::<_, Status>(attempts));
}

async fn retries_stop_at_max_retry_duration(t: &mut Target) {
    let attempts = attempts_until_given_up(
        t,
        "stop-duration",
        RetryConfig {
            max_attempts: -1,
            max_retry_duration: seconds(3),
            min_backoff: seconds(2),
            max_backoff: seconds(2),
            ..Default::default()
        },
    )
    .await;

    t.record(
        "unlimited attempts, 3s, 2s backoff",
        Ok::<_, Status>(attempts),
    );
}

async fn retries_stop_once_both_limits_pass(t: &mut Target) {
    let time_last = attempts_until_given_up(
        t,
        "stop-time-last",
        RetryConfig {
            max_attempts: 2,
            max_retry_duration: seconds(3),
            min_backoff: seconds(2),
            max_backoff: seconds(2),
            ..Default::default()
        },
    )
    .await;
    let attempts_last = attempts_until_given_up(
        t,
        "stop-attempts-last",
        RetryConfig {
            max_attempts: 4,
            max_retry_duration: seconds(1),
            min_backoff: seconds(1),
            max_backoff: seconds(1),
            ..Default::default()
        },
    )
    .await;

    t.record("2 attempts, 3s, 2s backoff", Ok::<_, Status>(time_last));
    t.record("4 attempts, 1s, 1s backoff", Ok::<_, Status>(attempts_last));
}

async fn retries_back_off_exponentially_then_linearly(t: &mut Target) {
    let queue = t
        .queue_with(
            "backoff",
            Queue {
                retry_config: Some(RetryConfig {
                    max_attempts: 6,
                    min_backoff: seconds(1),
                    max_backoff: seconds(7),
                    max_doublings: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    let request = CreateTaskRequest {
        parent: queue.clone(),
        task: Some(due_task(
            &format!("{queue}/tasks/t"),
            t.receiver.url_for("backoff", "500"),
        )),
        response_view: task::View::Unspecified.into(),
    };

    t.create_task(request).await.unwrap();

    let requests = t.receiver.wait_for("backoff", 6).await;
    t.record(
        "seconds between attempts",
        Ok::<_, Status>(
            requests
                .windows(2)
                .map(|pair| {
                    pair[1]
                        .time
                        .duration_since(pair[0].time)
                        .unwrap()
                        .as_secs_f64()
                        .round() as u64
                })
                .collect::<Vec<_>>(),
        ),
    );
}
