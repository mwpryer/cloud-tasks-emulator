//! Compares emulator with transcripts recorded from real Cloud Tasks in `tests/recordings`

mod support;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::process::Stdio;
use std::time::SystemTime;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::cloud_tasks_client::CloudTasksClient;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    BatchCreateTasksMetadata, BatchCreateTasksRequest, BatchCreateTasksResponse,
    BatchDeleteTasksMetadata, BatchDeleteTasksRequest, CmekConfig, CreateQueueRequest,
    CreateTaskRequest, DeleteQueueRequest, DeleteTaskRequest, GetCmekConfigRequest,
    GetQueueRequest, GetTaskRequest, HttpMethod, HttpRequest, HttpTarget, ListQueuesRequest,
    ListQueuesResponse, ListTasksRequest, ListTasksResponse, OAuthToken, OidcToken, PathOverride,
    PauseQueueRequest, PurgeQueueRequest, QueryOverride, Queue, RateLimits, ResumeQueueRequest,
    RetryConfig, RunTaskRequest, StackdriverLoggingConfig, Task, UpdateCmekConfigRequest,
    UpdateQueueRequest, UriOverride, http_request, http_target, queue, task, uri_override,
};
use cloud_tasks_emulator::proto::google::iam::v1::{
    Binding, GetIamPolicyRequest, GetPolicyOptions, Policy, SetIamPolicyRequest,
    TestIamPermissionsRequest,
};
use cloud_tasks_emulator::proto::google::longrunning::operations_client::OperationsClient;
use cloud_tasks_emulator::proto::google::longrunning::{
    CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest,
    Operation, WaitOperationRequest, operation,
};
use cloud_tasks_emulator::proto::google::rpc;
use cloud_tasks_emulator::proto::google::r#type::Expr;
use prost::Message;
use prost_types::{Any, Duration, FieldMask, Timestamp};
use reqwest::Method;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::task::JoinError;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::support::{Received, Receiver};

const RECORDINGS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/recordings");

#[tokio::test]
async fn emulator_matches_recordings() {
    let receiver = Receiver::start().await;
    let (_, url) = support::start_emulator_with_url().await;
    let target = Target::new(
        &url,
        MetadataMap::new(),
        "emulator-project",
        "emulator-location",
        "tasks@emulator-project.iam.gserviceaccount.com",
        &url,
        receiver,
    )
    .await;

    let results: Vec<_> = run_cases(&target)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    let differences: Vec<String> = results
        .iter()
        .filter_map(|(case, transcript)| {
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
    let mut receiver = Receiver::start().await;
    let (_tunnel, url) = open_tunnel(&receiver.url).await;
    receiver.url = url;
    let mut target = Target::new(
        "https://cloudtasks.googleapis.com",
        metadata,
        &env("CLOUD_TASKS_PROJECT"),
        &env("CLOUD_TASKS_LOCATION"),
        &env("CLOUD_TASKS_SERVICE_ACCOUNT"),
        "https://accounts.google.com",
        receiver,
    )
    .await;

    let results = run_cases(&target).await;
    // Deletes by run's prefix, so case that panicked leaks nothing
    let prefix = target.queue_name("");
    let mut leftovers = Vec::new();
    let mut page_token = String::new();
    loop {
        let page = target
            .list_queues(ListQueuesRequest {
                parent: target.parent.clone(),
                page_token,
                ..Default::default()
            })
            .await
            .unwrap();
        leftovers.extend(
            page.queues
                .into_iter()
                .filter(|queue| queue.name.starts_with(&prefix)),
        );
        if page.next_page_token.is_empty() {
            break;
        }
        page_token = page.next_page_token;
    }
    for queue in leftovers {
        target
            .delete_queue(DeleteQueueRequest { name: queue.name })
            .await
            .unwrap();
    }
    let results: Vec<_> = results.into_iter().map(Result::unwrap).collect();

    fs::create_dir_all(RECORDINGS).unwrap();
    for entry in fs::read_dir(RECORDINGS).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    for (case, transcript) in results {
        fs::write(format!("{RECORDINGS}/{case}.txt"), transcript).unwrap();
    }
}

/// Makes local URL public through Cloudflare quick tunnel, which lasts as long as returned process
async fn open_tunnel(local_url: &str) -> (Child, String) {
    let mut tunnel = Command::new("cloudflared")
        .args(["tunnel", "--no-autoupdate", "--url", local_url])
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("cloudflared is installed");
    let mut lines = BufReader::new(tunnel.stderr.take().unwrap()).lines();
    let url = tokio::time::timeout(std::time::Duration::from_secs(60), async {
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
        url
    })
    .await
    .expect("cloudflared registers the tunnel within 60s, which needs outbound port 7844");
    // Drains log so cloudflared never blocks on full pipe
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    // Host resolves a few seconds after registering, and lookup before then is cached as missing for 60s
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

/// Repeats `call` each second until `done` holds, for answers real Cloud Tasks varies or delays
macro_rules! retry_until {
    ($call:expr, $done:expr) => {
        retry_until!($call, $done, 60)
    };
    ($call:expr, $done:expr, $seconds:expr) => {
        tokio::time::timeout(std::time::Duration::from_secs($seconds), async {
            loop {
                let result = $call.await;
                if $done(&result) {
                    break result;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .await
        .expect("real Cloud Tasks gives the expected answer in time")
    };
}

#[derive(Clone)]
struct Target {
    client: CloudTasksClient<Channel>,
    operations: OperationsClient<Channel>,
    rest: reqwest::Client,
    rest_url: String,
    /// Sent with every request, so empty for emulator
    metadata: MetadataMap,
    project: String,
    parent: String,
    service_account: String,
    /// Prefixes queue IDs, as real Cloud Tasks blocks deleted queue's name for days
    run: String,
    /// Keeps tasks from dispatching
    tomorrow: Timestamp,
    receiver: Receiver,
    placeholders: Vec<(String, &'static str)>,
    transcript: String,
}

impl Target {
    async fn new(
        url: &str,
        metadata: MetadataMap,
        project: &str,
        location: &str,
        service_account: &str,
        issuer: &str,
        receiver: Receiver,
    ) -> Self {
        let channel = Endpoint::new(url.to_owned())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let run = format!("r{:08x}", rand::random::<u32>());
        let tomorrow = seconds_from_now(24 * 60 * 60);
        // REST shows times in RFC 3339, so fraction after seconds stays in transcripts
        let tomorrow_time = Timestamp {
            seconds: tomorrow,
            nanos: 0,
        }
        .to_string();
        Self {
            client: CloudTasksClient::new(channel.clone()),
            operations: OperationsClient::new(channel),
            rest: reqwest::Client::new(),
            rest_url: url.to_owned(),
            metadata,
            project: format!("projects/{project}"),
            parent: format!("projects/{project}/locations/{location}"),
            service_account: service_account.to_owned(),
            placeholders: vec![
                (receiver.url.clone(), "{receiver}"),
                (project.to_owned(), "{project}"),
                (location.to_owned(), "{location}"),
                (run.clone(), "{run}"),
                (tomorrow.to_string(), "{tomorrow}"),
                (tomorrow_time.trim_end_matches('Z').to_owned(), "{tomorrow}"),
                (service_account.to_owned(), "{service_account}"),
                (issuer.to_owned(), "{issuer}"),
            ],
            run,
            tomorrow: Timestamp {
                seconds: tomorrow,
                nanos: 123_456_789,
            },
            receiver,
            transcript: String::new(),
        }
    }

    fn queue_name(&self, id: &str) -> String {
        format!("{}/queues/{}-{id}", self.parent, self.run)
    }

    async fn queue(&mut self, id: &str) -> String {
        self.queue_with(id, Queue::default()).await
    }

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

    fn placeholder(&mut self, value: String, placeholder: &'static str) {
        self.placeholders.push((value, placeholder));
    }

    fn record(&mut self, label: &str, result: Result<impl Debug, Status>) {
        let entry = match result {
            Ok(value) => format!("{value:#?}"),
            Err(status) => {
                let entry = format!("{:?}: {}", status.code(), status.message());
                let details: Vec<Box<dyn Debug>> = rpc::Status::decode(status.details())
                    .unwrap()
                    .details
                    .iter()
                    .map(detail)
                    .collect();
                if details.is_empty() {
                    entry
                } else {
                    format!("{entry} {details:#?}")
                }
            }
        };
        self.transcript += &format!("{label} -> {entry}\n");
    }

    /// Records status and body as received, retrying on exhaustion as `run_task` does
    async fn rest(&mut self, label: &str, method: Method, path: &str, body: &str) -> String {
        let (status, body) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let mut request = self
                    .rest
                    .request(method.clone(), format!("{}{path}", self.rest_url))
                    .headers(self.metadata.clone().into_headers());
                if !body.is_empty() {
                    request = request
                        .header("content-type", "application/json")
                        .body(body.to_owned());
                }
                let response = request.send().await.unwrap();
                if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
                    break (response.status().as_u16(), response.text().await.unwrap());
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .await
        .expect("REST requests stop reporting exhaustion in time");
        self.transcript += &format!("{label} -> {status} {body}\n");
        body
    }

    fn json_placeholder(&mut self, body: &str, pointer: &str, placeholder: &'static str) {
        let json: Value = serde_json::from_str(body).unwrap();
        let value = json
            .pointer(pointer)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{pointer} is a string in {body}"));
        self.placeholder(value.to_owned(), placeholder);
    }

    /// Returns transcript with run-specific values replaced, most specific first
    fn finish(self) -> String {
        let mut transcript = self.transcript;
        for (value, placeholder) in self.placeholders.iter().rev() {
            transcript = transcript.replace(value, placeholder);
        }
        transcript
    }

    /// Polls task in full view until `done` holds for result
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

    /// Adds routing header that official clients send and real Cloud Tasks requires
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
        Ok(self.client.create_queue(request).await?.into_inner())
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
        let request = self.request(format!("name={}", request.name), request);
        self.client.delete_queue(request).await?;
        Ok(())
    }

    async fn update_queue(&mut self, request: UpdateQueueRequest) -> Result<Queue, Status> {
        let name = request
            .queue
            .as_ref()
            .map_or_else(String::new, |queue| queue.name.clone());
        let request = self.request(format!("queue.name={name}"), request);
        Ok(self.client.update_queue(request).await?.into_inner())
    }

    async fn pause_queue(&mut self, request: PauseQueueRequest) -> Result<Queue, Status> {
        let request = self.request(format!("name={}", request.name), request);
        Ok(self.client.pause_queue(request).await?.into_inner())
    }

    async fn resume_queue(&mut self, request: ResumeQueueRequest) -> Result<Queue, Status> {
        let request = self.request(format!("name={}", request.name), request);
        Ok(self.client.resume_queue(request).await?.into_inner())
    }

    async fn purge_queue(&mut self, request: PurgeQueueRequest) -> Result<Queue, Status> {
        let request = self.request(format!("name={}", request.name), request);
        Ok(self.client.purge_queue(request).await?.into_inner())
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

    async fn batch_create_tasks(
        &mut self,
        request: BatchCreateTasksRequest,
    ) -> Result<Decoded<BatchCreateTasksMetadata, BatchCreateTasksResponse>, Status> {
        let named = !request.request_id.is_empty();
        let request = self.request(format!("parent={}", request.parent), request);
        let operation = self.client.batch_create_tasks(request).await?.into_inner();
        if !named {
            self.operation_id(&operation.name);
        }
        let operation: Decoded<BatchCreateTasksMetadata, BatchCreateTasksResponse> =
            decode(operation);
        if let Some((_, metadata)) = &operation.metadata {
            self.times(metadata.start_time, metadata.end_time);
        }
        if let Some(Ok((_, response))) = &operation.result {
            for task in &response.tasks {
                self.placeholder(
                    task.create_time.unwrap().seconds.to_string(),
                    "{create_time}",
                );
            }
        }
        Ok(operation)
    }

    async fn batch_delete_tasks(
        &mut self,
        request: BatchDeleteTasksRequest,
    ) -> Result<Decoded<BatchDeleteTasksMetadata, ()>, Status> {
        let named = !request.request_id.is_empty();
        let request = self.request(format!("parent={}", request.parent), request);
        let operation = self.client.batch_delete_tasks(request).await?.into_inner();
        if !named {
            self.operation_id(&operation.name);
        }
        let operation: Decoded<BatchDeleteTasksMetadata, ()> = decode(operation);
        if let Some((_, metadata)) = &operation.metadata {
            self.times(metadata.start_time, metadata.end_time);
        }
        Ok(operation)
    }

    async fn get_iam_policy(&mut self, request: GetIamPolicyRequest) -> Result<Etagged, Status> {
        let request = self.request(format!("resource={}", request.resource), request);
        let policy = self.client.get_iam_policy(request).await?.into_inner();
        Ok(self.etagged(policy))
    }

    async fn set_iam_policy(&mut self, request: SetIamPolicyRequest) -> Result<Etagged, Status> {
        let request = self.request(format!("resource={}", request.resource), request);
        let policy = self.client.set_iam_policy(request).await?.into_inner();
        Ok(self.etagged(policy))
    }

    async fn test_iam_permissions(
        &mut self,
        request: TestIamPermissionsRequest,
    ) -> Result<Vec<String>, Status> {
        let request = self.request(format!("resource={}", request.resource), request);
        let response = self
            .client
            .test_iam_permissions(request)
            .await?
            .into_inner();
        Ok(response.permissions)
    }

    /// Sends routing field that real Cloud Tasks wants, as it rejects `name` that official clients send
    async fn get_cmek_config(
        &mut self,
        request: GetCmekConfigRequest,
    ) -> Result<CmekConfig, Status> {
        let request = self.request(format!("cmek_config.name={}", request.name), request);
        Ok(self.client.get_cmek_config(request).await?.into_inner())
    }

    async fn update_cmek_config(
        &mut self,
        request: UpdateCmekConfigRequest,
    ) -> Result<CmekConfig, Status> {
        let name = request
            .cmek_config
            .as_ref()
            .map_or_else(String::new, |config| config.name.clone());
        let request = self.request(format!("cmek_config.name={name}"), request);
        Ok(self.client.update_cmek_config(request).await?.into_inner())
    }

    /// Replaces each changing etag, keeping fixed one of queue that never had policy
    fn etagged(&mut self, policy: Policy) -> Etagged {
        let etag = STANDARD.encode(&policy.etag);
        if etag != "ACAB" {
            self.placeholder(etag.clone(), "{etag}");
        }
        Etagged {
            version: policy.version,
            etag,
            bindings: policy.bindings,
        }
    }

    fn operation_id(&mut self, name: &str) {
        let (_, id) = name.rsplit_once("/operations/").unwrap();
        self.placeholder(id.to_owned(), "{operation}");
    }

    /// Gets operation whose times earlier call already replaced
    async fn get_operation<M: Message + Default, R: Message + Default>(
        &mut self,
        request: GetOperationRequest,
    ) -> Result<Decoded<M, R>, Status> {
        let request = self.request(format!("name={}", request.name), request);
        Ok(decode(
            self.operations.get_operation(request).await?.into_inner(),
        ))
    }

    async fn list_operations(
        &mut self,
        request: ListOperationsRequest,
    ) -> Result<Vec<String>, Status> {
        let request = self.request(format!("name={}", request.name), request);
        let response = self.operations.list_operations(request).await?.into_inner();
        Ok(response
            .operations
            .into_iter()
            .map(|operation| operation.name)
            .collect())
    }

    async fn delete_operation(&mut self, request: DeleteOperationRequest) -> Result<(), Status> {
        let request = self.request(format!("name={}", request.name), request);
        self.operations.delete_operation(request).await?;
        Ok(())
    }

    async fn cancel_operation(&mut self, request: CancelOperationRequest) -> Result<(), Status> {
        let request = self.request(format!("name={}", request.name), request);
        self.operations.cancel_operation(request).await?;
        Ok(())
    }

    async fn wait_operation<M: Message + Default, R: Message + Default>(
        &mut self,
        request: WaitOperationRequest,
    ) -> Result<Decoded<M, R>, Status> {
        let request = self.request(format!("name={}", request.name), request);
        Ok(decode(
            self.operations.wait_operation(request).await?.into_inner(),
        ))
    }

    /// Replaces operation's times, sharing create time placeholder as both fall in same second or two
    fn times(&mut self, start: Option<Timestamp>, end: Option<Timestamp>) {
        for (time, nanos) in [(start, "{start_nanos}"), (end, "{end_nanos}")] {
            let time = time.unwrap();
            self.placeholder(time.seconds.to_string(), "{create_time}");
            self.placeholder(time.nanos.to_string(), nanos);
        }
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

    /// Retries while real Cloud Tasks reports RunTask as exhausted, which it does for new queue's first seconds
    async fn run_task(&mut self, request: RunTaskRequest) -> Result<Task, Status> {
        let routing = format!("name={}", request.name);
        let task = retry_until!(
            self.client
                .run_task(self.request(routing.clone(), request.clone())),
            |result: &Result<_, Status>| !result
                .as_ref()
                .is_err_and(|status| status.code() == Code::ResourceExhausted)
        )?
        .into_inner();
        self.placeholder(
            task.create_time.unwrap().seconds.to_string(),
            "{create_time}",
        );
        Ok(task)
    }
}

/// Operation with its metadata and result unpacked, each beside its type URL
#[derive(Debug)]
struct Decoded<M, R> {
    name: String,
    metadata: Option<(String, M)>,
    #[expect(dead_code, reason = "read through Debug in transcripts")]
    done: bool,
    result: Option<Result<(String, R), rpc::Status>>,
}

/// Policy with its etag in base64, as REST shows it
#[derive(Debug)]
#[expect(dead_code, reason = "read through Debug in transcripts")]
struct Etagged {
    version: i32,
    etag: String,
    bindings: Vec<Binding>,
}

fn decode<M: Message + Default, R: Message + Default>(operation: Operation) -> Decoded<M, R> {
    Decoded {
        name: operation.name,
        metadata: operation.metadata.map(unpack),
        done: operation.done,
        result: operation.result.map(|result| match result {
            operation::Result::Error(status) => Err(status),
            operation::Result::Response(response) => Ok(unpack(response)),
        }),
    }
}

fn unpack<M: Message + Default>(any: Any) -> (String, M) {
    let message = M::decode(any.value.as_slice()).unwrap();
    (any.type_url, message)
}

fn detail(any: &Any) -> Box<dyn Debug> {
    let value = any.value.as_slice();
    match any.type_url.as_str() {
        "type.googleapis.com/google.rpc.ErrorInfo" => {
            Box::new(rpc::ErrorInfo::decode(value).unwrap())
        }
        "type.googleapis.com/google.rpc.Help" => Box::new(rpc::Help::decode(value).unwrap()),
        "type.googleapis.com/google.rpc.LocalizedMessage" => {
            Box::new(rpc::LocalizedMessage::decode(value).unwrap())
        }
        url => panic!("{url} is a known detail type"),
    }
}

fn seconds_from_now(seconds: i64) -> i64 {
    Timestamp::from(SystemTime::now()).seconds + seconds
}

macro_rules! cases {
    ($($case:ident),* $(,)?) => {
        /// Runs every case at once and returns each one's transcript, or how it panicked
        async fn run_cases(target: &Target) -> Vec<Result<(&'static str, String), JoinError>> {
            let cases = vec![$({
                let mut target = target.clone();
                tokio::spawn(async move {
                    $case(&mut target).await;
                    (stringify!($case), target.finish())
                })
            }),*];
            let mut results = Vec::new();
            for case in cases {
                results.push(case.await);
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
    list_queues_filters_by_state,
    list_queues_filters_by_field,
    list_queues_rejects_invalid_filters,
    update_queue_creates_missing_queue,
    update_queue_changes_masked_fields,
    update_queue_without_mask_replaces_every_field,
    update_queue_with_empty_mask_changes_nothing,
    update_queue_masks_whole_messages,
    update_queue_ignores_output_only_fields,
    update_queue_keeps_state,
    update_queue_rejects_invalid_requests,
    delete_queue_removes_queue,
    delete_queue_rejects_invalid_names,
    delete_queue_blocks_name,
    pause_queue_stops_dispatch,
    pause_and_resume_repeat,
    pause_queue_rejects_invalid_names,
    resume_queue_rejects_invalid_names,
    purge_queue_deletes_existing_tasks,
    purge_queue_keeps_state,
    purge_queue_rejects_invalid_names,
    create_task_returns_basic_view,
    create_task_returns_full_view,
    create_task_drops_reserved_headers,
    create_task_sanitises_headers,
    create_task_limits_header_length,
    create_task_truncates_deadline,
    create_task_generates_numeric_id,
    create_task_rejects_invalid_requests,
    create_task_rejects_invalid_names,
    create_task_returns_retry_config,
    create_task_rejects_invalid_retry_config,
    create_task_limits_task_size,
    create_task_limits_schedule_time,
    create_task_moves_past_schedule_time_to_now,
    get_task_returns_basic_and_full_views,
    get_task_rejects_invalid_names,
    list_tasks_returns_every_task,
    list_tasks_pages_by_page_size,
    list_tasks_rejects_invalid_requests,
    delete_task_removes_task,
    delete_task_rejects_invalid_names,
    delete_task_blocks_name,
    finished_task_blocks_name,
    run_task_dispatches_now,
    run_task_dispatches_from_paused_queue,
    run_task_backs_off_after_failure,
    run_task_rejects_invalid_names,
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
    retries_follow_task_retry_config,
    create_task_returns_oidc_token,
    create_task_returns_oauth_token,
    create_task_rejects_invalid_tokens,
    dispatch_sends_oidc_token,
    create_queue_limits_sampling_ratio,
    create_queue_returns_http_target,
    create_queue_rejects_invalid_http_target,
    create_queue_sanitises_header_overrides,
    update_queue_changes_http_target,
    create_task_takes_queue_token,
    dispatch_applies_http_target,
    dispatch_applies_http_target_to_existing_tasks,
    list_queues_filters_by_target,
    batch_create_tasks_creates_every_task,
    batch_create_tasks_reports_failed_requests,
    batch_create_tasks_fails_when_every_request_fails,
    batch_create_tasks_rejects_invalid_requests,
    batch_create_tasks_names_operation_by_request_id,
    batch_delete_tasks_deletes_every_task,
    batch_delete_tasks_reports_failed_requests,
    batch_delete_tasks_fails_when_every_request_fails,
    batch_delete_tasks_rejects_invalid_requests,
    batch_delete_tasks_shares_request_ids_with_batch_create,
    get_operation_returns_batch_operations,
    get_operation_rejects_invalid_names,
    only_get_operation_is_served,
    get_iam_policy_returns_empty_policy,
    get_iam_policy_rejects_invalid_names,
    set_iam_policy_stores_policy,
    set_iam_policy_checks_etag,
    set_iam_policy_rejects_invalid_requests,
    test_iam_permissions_returns_requested_permissions,
    test_iam_permissions_rejects_invalid_requests,
    get_cmek_config_returns_config,
    get_cmek_config_rejects_invalid_names,
    update_cmek_config_rejects_invalid_requests,
    update_cmek_config_refuses_queues_with_header_overrides,
    rest_manages_queues,
    rest_lists_queues,
    rest_manages_tasks,
    rest_runs_tasks,
    rest_runs_batches,
    rest_manages_iam_policies,
    rest_manages_cmek_config,
    rest_reports_errors,
    rest_rejects_invalid_requests,
    rest_serves_only_mapped_routes,
    rest_follows_system_parameters,
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

fn seconds(seconds: i64) -> Option<Duration> {
    Some(Duration { seconds, nanos: 0 })
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
                max_retry_duration: seconds(60),
                min_backoff: seconds(1),
                max_backoff: seconds(10),
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
                max_retry_duration: seconds(0),
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
                min_backoff: seconds(-1),
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
                max_backoff: seconds(-1),
                ..Default::default()
            },
        ),
        (
            "max retry duration -1s",
            RateLimits::default(),
            RetryConfig {
                max_retry_duration: seconds(-1),
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
                min_backoff: seconds(10),
                max_backoff: seconds(5),
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

/// Names that every queue RPC rejects, alongside queue that does not exist
fn invalid_queue_names(t: &Target) -> [(&'static str, String); 5] {
    [
        ("unknown queue", t.queue_name("missing")),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without queue id", format!("{}/queues", t.parent)),
        ("invalid queue id", format!("{}/queues/not_valid", t.parent)),
    ]
}

async fn get_queue_rejects_invalid_names(t: &mut Target) {
    for (label, name) in invalid_queue_names(t) {
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

    // Other cases' queues share location
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
    for (label, name) in invalid_queue_names(t) {
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
    let with_deadline = |deadline| Task {
        dispatch_deadline: seconds(deadline),
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

/// Names that every task RPC rejects, alongside task and queue that do not exist
fn invalid_task_names(t: &Target, queue: &str) -> [(&'static str, String); 6] {
    [
        ("unknown task", format!("{queue}/tasks/missing")),
        (
            "unknown queue",
            format!("{}/tasks/t", t.queue_name("missing")),
        ),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without task id", format!("{queue}/tasks")),
        ("invalid task id", format!("{queue}/tasks/not valid")),
    ]
}

async fn get_task_rejects_invalid_names(t: &mut Target) {
    let queue = t.queue("get-tasks").await;
    for (label, name) in invalid_task_names(t, &queue).into_iter().chain([
        (
            "invalid queue id",
            format!("{}/queues/not_valid/tasks/t", t.parent),
        ),
        ("name with extra segment", format!("{queue}/tasks/t/extra")),
    ]) {
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
    for (label, name) in invalid_task_names(t, &queue) {
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

/// Headers Cloud Tasks sets or passes on, leaving out those tunnel or HTTP client add
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

/// Creates task that dispatches now, unlike `Target::task`
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
    // One-hour backoff keeps each task, and its last attempt, after first failure
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

fn mask(paths: &[&str]) -> Option<FieldMask> {
    Some(FieldMask {
        paths: paths.iter().map(|path| path.to_string()).collect(),
    })
}

/// Rate limits and retry config unlike defaults, so update shows which fields it changed
fn configured() -> Queue {
    Queue {
        rate_limits: Some(RateLimits {
            max_dispatches_per_second: 10.0,
            max_burst_size: 0,
            max_concurrent_dispatches: 5,
        }),
        retry_config: Some(RetryConfig {
            max_attempts: 5,
            max_retry_duration: seconds(60),
            min_backoff: seconds(1),
            max_backoff: seconds(10),
            max_doublings: 3,
        }),
        ..Default::default()
    }
}

async fn update_queue_creates_missing_queue(t: &mut Target) {
    let unmasked = t.queue_name("upserted");
    let masked = t.queue_name("upserted-masked");

    let created = t
        .update_queue(UpdateQueueRequest {
            queue: Some(Queue {
                name: unmasked.clone(),
                ..configured()
            }),
            update_mask: None,
        })
        .await;
    let created_masked = t
        .update_queue(UpdateQueueRequest {
            queue: Some(Queue {
                name: masked,
                ..configured()
            }),
            update_mask: mask(&["rate_limits.max_concurrent_dispatches"]),
        })
        .await;
    let got = t.get_queue(GetQueueRequest { name: unmasked }).await;

    t.record("update_queue without mask", created);
    t.record("update_queue with mask", created_masked);
    t.record("get_queue", got);
}

async fn update_queue_changes_masked_fields(t: &mut Target) {
    let name = t.queue_with("masked", configured()).await;
    let request = UpdateQueueRequest {
        queue: Some(Queue {
            name,
            rate_limits: Some(RateLimits {
                max_dispatches_per_second: 100.0,
                ..Default::default()
            }),
            retry_config: Some(RetryConfig {
                max_attempts: 7,
                ..Default::default()
            }),
            ..Default::default()
        }),
        update_mask: mask(&[
            "rate_limits.max_dispatches_per_second",
            "retry_config.max_attempts",
        ]),
    };

    let result = t.update_queue(request).await;

    t.record("update_queue", result);
}

async fn update_queue_without_mask_replaces_every_field(t: &mut Target) {
    let name = t.queue_with("unmasked", configured()).await;
    let request = UpdateQueueRequest {
        queue: Some(Queue {
            name,
            rate_limits: Some(RateLimits {
                max_dispatches_per_second: 100.0,
                ..Default::default()
            }),
            ..Default::default()
        }),
        update_mask: None,
    };

    let result = t.update_queue(request).await;

    t.record("update_queue", result);
}

async fn update_queue_with_empty_mask_changes_nothing(t: &mut Target) {
    let name = t.queue("empty-mask").await;
    let request = UpdateQueueRequest {
        queue: Some(Queue {
            name,
            ..configured()
        }),
        update_mask: mask(&[]),
    };

    let result = t.update_queue(request).await;

    t.record("update_queue", result);
}

async fn update_queue_masks_whole_messages(t: &mut Target) {
    let name = t.queue_with("whole-messages", configured()).await;

    let rate_limits = t
        .update_queue(UpdateQueueRequest {
            queue: Some(Queue {
                name: name.clone(),
                rate_limits: Some(RateLimits {
                    max_dispatches_per_second: 100.0,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            update_mask: mask(&["rate_limits"]),
        })
        .await;
    let retry_config = t
        .update_queue(UpdateQueueRequest {
            queue: Some(Queue {
                name,
                ..Default::default()
            }),
            update_mask: mask(&["retry_config"]),
        })
        .await;

    t.record("rate_limits", rate_limits);
    t.record("retry_config", retry_config);
}

async fn update_queue_ignores_output_only_fields(t: &mut Target) {
    let name = t.queue_name("output-only");
    let created = t
        .create_queue(CreateQueueRequest {
            parent: t.parent.clone(),
            queue: Some(Queue {
                name: name.clone(),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    for path in ["name", "state", "purge_time", "rate_limits.max_burst_size"] {
        let request = UpdateQueueRequest {
            queue: Some(Queue {
                name: name.clone(),
                state: queue::State::Paused.into(),
                purge_time: Some(t.tomorrow),
                rate_limits: Some(RateLimits {
                    max_burst_size: 50,
                    ..RateLimits::default()
                }),
                ..Default::default()
            }),
            update_mask: mask(&[path]),
        };

        let result = t.update_queue(request).await;

        t.record(path, result.map(|queue| queue == created));
    }
}

async fn update_queue_keeps_state(t: &mut Target) {
    let name = t.queue("update-paused").await;
    t.pause_queue(PauseQueueRequest { name: name.clone() })
        .await
        .unwrap();
    let request = UpdateQueueRequest {
        queue: Some(Queue {
            name,
            state: queue::State::Running.into(),
            ..Default::default()
        }),
        update_mask: None,
    };

    let result = t.update_queue(request).await;

    t.record("update_queue state", result.map(|queue| queue.state()));
}

async fn update_queue_rejects_invalid_requests(t: &mut Target) {
    let existing = t.queue_with("update-invalid", configured()).await;
    let named = |name: String| {
        Some(Queue {
            name,
            ..Default::default()
        })
    };
    let update = |paths: &[&str], queue: Queue| UpdateQueueRequest {
        queue: Some(Queue {
            name: existing.clone(),
            ..queue
        }),
        update_mask: mask(paths),
    };
    for (label, request) in [
        (
            "no queue",
            UpdateQueueRequest {
                queue: None,
                update_mask: None,
            },
        ),
        (
            "empty name",
            UpdateQueueRequest {
                queue: named(String::new()),
                update_mask: None,
            },
        ),
        (
            "name not in a project",
            UpdateQueueRequest {
                queue: named("not-a-valid-name".to_owned()),
                update_mask: None,
            },
        ),
        (
            "name without queue id",
            UpdateQueueRequest {
                queue: named(format!("{}/queues", t.parent)),
                update_mask: None,
            },
        ),
        (
            "invalid queue id",
            UpdateQueueRequest {
                queue: named(format!("{}/queues/not_valid", t.parent)),
                update_mask: None,
            },
        ),
        (
            "unknown mask path on unknown queue",
            UpdateQueueRequest {
                queue: named(t.queue_name("missing")),
                update_mask: mask(&["bogus"]),
            },
        ),
        ("unknown mask path", update(&["bogus"], Queue::default())),
        (
            "unknown nested mask path",
            update(&["rate_limits.bogus"], Queue::default()),
        ),
        (
            "unknown http_target mask path",
            update(&["http_target.bogus"], Queue::default()),
        ),
        (
            "mask path below a scalar",
            update(
                &["rate_limits.max_dispatches_per_second.bogus"],
                Queue::default(),
            ),
        ),
        (
            "camel case mask path",
            update(&["rateLimits.maxDispatchesPerSecond"], Queue::default()),
        ),
        (
            "comma-separated mask paths",
            update(
                &["rate_limits.max_dispatches_per_second,retry_config.max_attempts"],
                Queue::default(),
            ),
        ),
        (
            "mask path with space",
            update(&[" state"], Queue::default()),
        ),
        ("empty mask path", update(&[""], Queue::default())),
        (
            "rate 501",
            update(
                &["rate_limits.max_dispatches_per_second"],
                Queue {
                    rate_limits: Some(RateLimits {
                        max_dispatches_per_second: 501.0,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ),
        (
            "invalid http_target outside mask",
            update(
                &["rate_limits"],
                Queue {
                    http_target: Some(HttpTarget {
                        uri_override: Some(UriOverride {
                            host: Some(String::new()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ),
        (
            "uri override emptied by mask",
            update(
                &["http_target.uri_override.host"],
                Queue {
                    http_target: Some(HttpTarget {
                        uri_override: Some(UriOverride {
                            port: Some(8080),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ),
        (
            "oidc email emptied by mask",
            update(
                &["http_target.oidc_token.service_account_email"],
                Queue {
                    http_target: Some(HttpTarget {
                        authorization_header: queue_oauth(&t.service_account, ""),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ),
        (
            "min backoff above stored max backoff",
            update(
                &["retry_config.min_backoff"],
                Queue {
                    retry_config: Some(RetryConfig {
                        min_backoff: seconds(20),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ),
    ] {
        let result = t.update_queue(request).await;

        t.record(label, result);
    }
}

async fn pause_queue_stops_dispatch(t: &mut Target) {
    let queue = t.queue("paused").await;
    let paused = t
        .pause_queue(PauseQueueRequest {
            name: queue.clone(),
        })
        .await;
    t.create_task(CreateTaskRequest {
        parent: queue.clone(),
        task: Some(due_task(
            &format!("{queue}/tasks/t"),
            t.receiver.url_for("paused", "200"),
        )),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let while_paused = t.receiver.received("paused").len();

    let resumed = t.resume_queue(ResumeQueueRequest { name: queue }).await;

    let after_resume = t.receiver.wait_for("paused", 1).await.len();
    t.record("pause_queue", paused);
    t.record("requests while paused", Ok::<_, Status>(while_paused));
    t.record("resume_queue", resumed);
    t.record("requests after resume", Ok::<_, Status>(after_resume));
}

async fn pause_and_resume_repeat(t: &mut Target) {
    let name = t.queue("repeat").await;

    let first_pause = t
        .pause_queue(PauseQueueRequest { name: name.clone() })
        .await;
    let second_pause = t
        .pause_queue(PauseQueueRequest { name: name.clone() })
        .await;
    let first_resume = t
        .resume_queue(ResumeQueueRequest { name: name.clone() })
        .await;
    let second_resume = t.resume_queue(ResumeQueueRequest { name }).await;

    t.record("first pause", first_pause.map(|queue| queue.state()));
    t.record("second pause", second_pause.map(|queue| queue.state()));
    t.record("first resume", first_resume.map(|queue| queue.state()));
    t.record("second resume", second_resume.map(|queue| queue.state()));
}

async fn pause_queue_rejects_invalid_names(t: &mut Target) {
    for (label, name) in invalid_queue_names(t) {
        let result = t.pause_queue(PauseQueueRequest { name }).await;

        t.record(label, result);
    }
}

async fn resume_queue_rejects_invalid_names(t: &mut Target) {
    for (label, name) in invalid_queue_names(t) {
        let result = t.resume_queue(ResumeQueueRequest { name }).await;

        t.record(label, result);
    }
}

async fn purge_queue_rejects_invalid_names(t: &mut Target) {
    for (label, name) in invalid_queue_names(t) {
        let result = t.purge_queue(PurgeQueueRequest { name }).await;

        t.record(label, result);
    }
}

async fn purge_queue_deletes_existing_tasks(t: &mut Target) {
    let queue = t.queue("purged").await;
    let purged_task = format!("{queue}/tasks/a");
    let create = |t: &Target, name: &str| CreateTaskRequest {
        parent: queue.clone(),
        task: Some(t.task(name, http("https://example.com/work"))),
        response_view: task::View::Unspecified.into(),
    };
    let task_names = async |t: &mut Target| {
        let mut names: Vec<String> = t
            .list_tasks(ListTasksRequest {
                parent: queue.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .tasks
            .into_iter()
            .map(|task| task.name)
            .collect();
        names.sort();
        names
    };
    t.create_task(create(t, &purged_task)).await.unwrap();

    let purged = t
        .purge_queue(PurgeQueueRequest {
            name: queue.clone(),
        })
        .await;

    // Purge takes a moment in real Cloud Tasks
    retry_until!(task_names(t), Vec::is_empty, 120);
    // Cloud Tasks keeps create_time in whole seconds, so task created in purge's second can count as before it
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    t.create_task(create(t, &format!("{queue}/tasks/b")))
        .await
        .unwrap();
    let listed = retry_until!(task_names(t), |names: &Vec<String>| !names.is_empty(), 120);
    let got = t
        .get_task(GetTaskRequest {
            name: purged_task.clone(),
            response_view: task::View::Unspecified.into(),
        })
        .await;
    let recreated = t.create_task(create(t, &purged_task)).await;
    let got_queue = t
        .get_queue(GetQueueRequest {
            name: queue.clone(),
        })
        .await;
    t.record(
        "purge_queue",
        purged.map(|queue| (queue.state(), queue.purge_time.is_some())),
    );
    t.record("tasks after purge", Ok::<_, Status>(listed));
    t.record("get_task purged", got);
    t.record("create_task purged", recreated.map(|task| task.name));
    t.record(
        "get_queue purge_time is set",
        got_queue.map(|queue| queue.purge_time.is_some()),
    );
}

async fn purge_queue_keeps_state(t: &mut Target) {
    let name = t.queue("purge-paused").await;
    t.pause_queue(PauseQueueRequest { name: name.clone() })
        .await
        .unwrap();

    let result = t.purge_queue(PurgeQueueRequest { name }).await;

    t.record("purge_queue state", result.map(|queue| queue.state()));
}

async fn run_task_dispatches_now(t: &mut Target) {
    let queue = t.queue("run").await;
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(t.task(&name, http(&t.receiver.url_for("run", "200")))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    let result = t
        .run_task(RunTaskRequest {
            name: name.clone(),
            response_view: task::View::Unspecified.into(),
        })
        .await;

    let requests = t.receiver.wait_for("run", 1).await;
    let gone = t.poll_task(&name, Result::is_err).await;
    t.record("run_task", result);
    t.record(
        "request headers",
        Ok::<_, Status>(dispatched_headers(&requests[0])),
    );
    t.record("get_task after success", gone);
}

async fn run_task_dispatches_from_paused_queue(t: &mut Target) {
    let queue = t.queue("run-paused").await;
    t.pause_queue(PauseQueueRequest {
        name: queue.clone(),
    })
    .await
    .unwrap();
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(t.task(&name, http(&t.receiver.url_for("run-paused", "200")))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    t.run_task(RunTaskRequest {
        name,
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    let requests = t.receiver.wait_for("run-paused", 1).await;
    t.record("requests", Ok::<_, Status>(requests.len()));
}

async fn run_task_backs_off_after_failure(t: &mut Target) {
    let queue = t
        .queue_with(
            "run-failed",
            Queue {
                retry_config: Some(RetryConfig {
                    min_backoff: seconds(60),
                    max_backoff: seconds(60),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(t.task(&name, http(&t.receiver.url_for("run-failed", "500")))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    t.run_task(RunTaskRequest {
        name: name.clone(),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    let result = t
        .poll_task(&name, |result| {
            result.as_ref().is_ok_and(|task| {
                task.last_attempt
                    .as_ref()
                    .is_some_and(|attempt| attempt.response_status.is_some())
            })
        })
        .await;
    t.record(
        "get_task",
        result.map(|task| {
            let attempt = task.last_attempt.unwrap();
            let responded = SystemTime::try_from(attempt.response_time.unwrap()).unwrap();
            let retry_at = SystemTime::try_from(task.schedule_time.unwrap()).unwrap();
            (
                task.dispatch_count,
                task.response_count,
                task.first_attempt.is_some(),
                attempt.dispatch_time.is_some(),
                attempt.response_status,
                retry_at
                    .duration_since(responded)
                    .unwrap()
                    .as_secs_f64()
                    .round() as u64,
            )
        }),
    );
}

async fn run_task_rejects_invalid_names(t: &mut Target) {
    let queue = t.queue("run-invalid").await;
    for (label, name) in invalid_task_names(t, &queue) {
        let result = t
            .run_task(RunTaskRequest {
                name,
                response_view: task::View::Unspecified.into(),
            })
            .await;

        t.record(label, result);
    }
}

/// Lists with filter, keeping only this case's queues as other cases share location
async fn filtered_queue_names(
    t: &mut Target,
    filter: &str,
    key: &str,
) -> Result<Vec<String>, Status> {
    let response = t
        .list_queues(ListQueuesRequest {
            parent: t.parent.clone(),
            filter: filter.to_owned(),
            ..Default::default()
        })
        .await?;
    let mut names: Vec<String> = response
        .queues
        .into_iter()
        .map(|queue| queue.name)
        .filter(|name| name.contains(key))
        .collect();
    names.sort();
    Ok(names)
}

async fn list_queues_filters_by_state(t: &mut Target) {
    t.queue("state-filter-running").await;
    let paused = t.queue("state-filter-paused").await;
    t.pause_queue(PauseQueueRequest { name: paused })
        .await
        .unwrap();
    for filter in [
        "state: PAUSED",
        "state:PAUSED",
        "state = PAUSED",
        "state != PAUSED",
        "state: paused",
        "State: Paused",
        "state: \"PAUSED\"",
        "state: DISABLED",
        "NOT state: PAUSED",
        "-state: PAUSED",
        "state: PAUSED OR state: RUNNING",
        "state: PAUSED AND state: RUNNING",
        "state: PAUSED state: RUNNING",
        "(state: PAUSED)",
        "state: PAUSED OR (state: RUNNING AND NOT state: RUNNING)",
        "state > RUNNING",
    ] {
        let result = filtered_queue_names(t, filter, "-state-filter-").await;

        t.record(filter, result);
    }
}

async fn list_queues_filters_by_field(t: &mut Target) {
    t.queue_with("field-filter-configured", configured()).await;
    let purged = t.queue("field-filter-purged").await;
    t.purge_queue(PurgeQueueRequest { name: purged })
        .await
        .unwrap();
    for filter in [
        "name: field-filter-purged",
        "name: PURGED",
        "name: *",
        "rate_limits.max_dispatches_per_second = 500",
        "rate_limits.max_dispatches_per_second < 100",
        "rate_limits.max_dispatches_per_second: 10",
        "rateLimits.maxDispatchesPerSecond <= 10",
        "rate_limits.max_concurrent_dispatches >= 1000",
        "rate_limits.max_burst_size != 100",
        "retry_config.max_attempts = 5",
        "retry_config.min_backoff = 1s",
        "retry_config.min_backoff > 0.5s",
        "retry_config.min_backoff: 0.1s",
        "retry_config.max_retry_duration: *",
        "retry_config.max_retry_duration < 1s",
        "purge_time: *",
        "purge_time > \"2020-01-01T00:00:00Z\"",
        "purge_time < \"2020-01-01T00:00:00Z\"",
        "rate_limits: *",
        "http_target: *",
    ] {
        let result = filtered_queue_names(t, filter, "-field-filter-").await;

        t.record(filter, result);
    }
}

async fn list_queues_rejects_invalid_filters(t: &mut Target) {
    for filter in [
        "bogus_field: x",
        "max_dispatches_per_second < 100",
        "state: BOGUS",
        "state: 2",
        "retry_config.max_backoff = 3600",
        "rate_limits.max_dispatches_per_second = fast",
        "rate_limits: 10",
    ] {
        let result = t
            .list_queues(ListQueuesRequest {
                parent: t.parent.clone(),
                filter: filter.to_owned(),
                ..Default::default()
            })
            .await;

        t.record(filter, result.map(|response| response.queues.len()));
    }
}

async fn create_task_returns_retry_config(t: &mut Target) {
    let queue = t.queue("task-retry").await;
    let name = format!("{queue}/tasks/t");
    let partial = format!("{queue}/tasks/partial");

    let created = t
        .create_task(CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                retry_config: Some(RetryConfig {
                    max_attempts: 3,
                    max_retry_duration: seconds(60),
                    min_backoff: seconds(2),
                    max_backoff: seconds(4),
                    max_doublings: 1,
                }),
                ..t.task(&name, http("https://example.com/work"))
            }),
            response_view: task::View::Unspecified.into(),
        })
        .await;
    let partial_created = t
        .create_task(CreateTaskRequest {
            parent: queue,
            task: Some(Task {
                retry_config: Some(RetryConfig {
                    max_attempts: 3,
                    ..Default::default()
                }),
                ..t.task(&partial, http("https://example.com/work"))
            }),
            response_view: task::View::Unspecified.into(),
        })
        .await;
    let got = t
        .get_task(GetTaskRequest {
            name,
            response_view: task::View::Unspecified.into(),
        })
        .await;

    t.record("create_task", created.map(|task| task.retry_config));
    t.record(
        "create_task partial",
        partial_created.map(|task| task.retry_config),
    );
    t.record("get_task", got.map(|task| task.retry_config));
}

async fn create_task_rejects_invalid_retry_config(t: &mut Target) {
    let queue = t.queue("task-retry-invalid").await;
    for (label, retry_config) in [
        (
            "max attempts -2",
            RetryConfig {
                max_attempts: -2,
                ..Default::default()
            },
        ),
        (
            "max doublings -1",
            RetryConfig {
                max_doublings: -1,
                ..Default::default()
            },
        ),
        (
            "min backoff -1s",
            RetryConfig {
                min_backoff: seconds(-1),
                ..Default::default()
            },
        ),
        (
            "max backoff -1s",
            RetryConfig {
                max_backoff: seconds(-1),
                ..Default::default()
            },
        ),
        (
            "max retry duration -1s",
            RetryConfig {
                max_retry_duration: seconds(-1),
                ..Default::default()
            },
        ),
        (
            "max retry duration 30.9s",
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
            RetryConfig {
                min_backoff: seconds(10),
                max_backoff: seconds(5),
                ..Default::default()
            },
        ),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                retry_config: Some(retry_config),
                ..t.task("", http("https://example.com/work"))
            }),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.retry_config));
    }
}

async fn retries_follow_task_retry_config(t: &mut Target) {
    let queue = t
        .queue_with(
            "task-retries",
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
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(Task {
            retry_config: Some(RetryConfig {
                max_attempts: 2,
                min_backoff: seconds(1),
                max_backoff: seconds(1),
                ..Default::default()
            }),
            ..due_task(&name, t.receiver.url_for("task-retries", "500"))
        }),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    t.poll_task(&name, Result::is_err).await.unwrap_err();

    let attempts = t.receiver.received("task-retries").len();
    t.record("2 attempts on a queue of 10", Ok::<_, Status>(attempts));
}

async fn create_task_limits_task_size(t: &mut Target) {
    let queue = t.queue("task-size").await;
    let name = format!("{queue}/tasks/t");
    // Searches for largest body accepted, which is below 2 MB
    let (mut accepted, mut rejected) = (0, 2_000_000);
    let mut error = None;
    let mut probe = 0;
    while rejected - accepted > 1 {
        let body = (accepted + rejected) / 2;
        probe += 1;
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                &format!("{name}{probe:02}"),
                HttpRequest {
                    body: vec![b'a'; body],
                    ..with_headers(&[("X-Header", &"h".repeat(1000))])
                },
            )),
            response_view: task::View::Unspecified.into(),
        };
        match t.create_task(request).await {
            Ok(_) => accepted = body,
            Err(status) => {
                rejected = body;
                error = Some(status);
            }
        }
    }

    // Name's length counts towards size, and differs between targets
    t.record(
        "largest body plus name length",
        Ok::<_, Status>(accepted + name.len()),
    );
    t.record("one byte more", Err::<(), _>(error.unwrap()));
}

async fn delete_task_blocks_name(t: &mut Target) {
    let queue = t.queue("deleted-task").await;
    let name = format!("{queue}/tasks/t");
    let create = |t: &Target| CreateTaskRequest {
        parent: queue.clone(),
        task: Some(t.task(&name, http("https://example.com/work"))),
        response_view: task::View::Unspecified.into(),
    };
    t.create_task(create(t)).await.unwrap();
    t.delete_task(DeleteTaskRequest { name: name.clone() })
        .await
        .unwrap();

    let got = t
        .get_task(GetTaskRequest {
            name: name.clone(),
            response_view: task::View::Unspecified.into(),
        })
        .await;
    let recreated = t.create_task(create(t)).await;
    let deleted = t
        .delete_task(DeleteTaskRequest { name: name.clone() })
        .await;
    let run = t
        .run_task(RunTaskRequest {
            name,
            response_view: task::View::Unspecified.into(),
        })
        .await;

    t.record("get_task", got);
    t.record("create_task", recreated.map(|task| task.name));
    t.record("delete_task", deleted);
    t.record("run_task", run);
}

async fn finished_task_blocks_name(t: &mut Target) {
    let queue = t
        .queue_with(
            "finished-task",
            Queue {
                retry_config: Some(RetryConfig {
                    max_attempts: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    for step in ["200", "500"] {
        let name = format!("{queue}/tasks/t{step}");
        let create = |t: &Target| CreateTaskRequest {
            parent: queue.clone(),
            task: Some(due_task(
                &name,
                t.receiver.url_for(&format!("finished-{step}"), step),
            )),
            response_view: task::View::Unspecified.into(),
        };
        t.create_task(create(t)).await.unwrap();

        let got = t.poll_task(&name, Result::is_err).await;
        let recreated = t.create_task(create(t)).await;

        t.record(&format!("{step} get_task"), got);
        t.record(
            &format!("{step} create_task"),
            recreated.map(|task| task.name),
        );
    }
}

async fn delete_queue_blocks_name(t: &mut Target) {
    let name = t.queue("deleted-queue").await;
    t.delete_queue(DeleteQueueRequest { name: name.clone() })
        .await
        .unwrap();
    let create = CreateQueueRequest {
        parent: t.parent.clone(),
        queue: Some(Queue {
            name: name.clone(),
            ..Default::default()
        }),
    };

    // Real Cloud Tasks accepts name for a moment after delete
    let created = retry_until!(t.create_queue(create.clone()), Result::is_err);
    // Real Cloud Tasks accepts about 1 in 50, though no queue appears
    let updated = retry_until!(
        t.update_queue(UpdateQueueRequest {
            queue: create.queue.clone(),
            update_mask: None,
        }),
        Result::is_err
    );
    let got = t.get_queue(GetQueueRequest { name: name.clone() }).await;
    // Real Cloud Tasks sometimes accepts delete again
    let deleted = retry_until!(
        t.delete_queue(DeleteQueueRequest { name: name.clone() }),
        Result::is_err
    );
    let paused = t
        .pause_queue(PauseQueueRequest { name: name.clone() })
        .await;
    // Real Cloud Tasks answers about 1 in 3 with FailedPrecondition, saying queue existed recently
    let task = retry_until!(
        t.create_task(CreateTaskRequest {
            parent: name.clone(),
            task: Some(t.task("", http("https://example.com/work"))),
            response_view: task::View::Unspecified.into(),
        }),
        |result: &Result<Task, Status>| result
            .as_ref()
            .is_err_and(|status| status.code() == Code::NotFound)
    );
    let listed = t
        .list_queues(ListQueuesRequest {
            parent: t.parent.clone(),
            ..Default::default()
        })
        .await;

    t.record("create_queue", created);
    t.record("update_queue", updated);
    t.record("get_queue", got);
    t.record("delete_queue", deleted);
    t.record("pause_queue", paused);
    t.record("create_task", task.map(|task| task.name));
    t.record(
        "list_queues includes it",
        listed.map(|response| response.queues.iter().any(|queue| queue.name == name)),
    );
}

async fn create_task_limits_schedule_time(t: &mut Target) {
    let queue = t.queue("schedule-limit").await;
    for (label, seconds, nanos) in [
        ("29 days out", seconds_from_now(29 * 24 * 60 * 60), 0),
        ("2100-01-01T00:00:00Z", 4_102_444_800, 0),
        ("2100-07-01T00:00:00Z", 4_118_083_200, 0),
        ("2100-01-01T00:00:00.5Z", 4_102_444_800, 500_000_000),
        ("2100-01-01T00:00:00.000000001Z", 4_102_444_800, 1),
        ("10000-01-01T00:00:00Z", 253_402_300_800, 0),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                schedule_time: Some(Timestamp { seconds, nanos }),
                ..t.task("", http("https://example.com/work"))
            }),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.name.starts_with(&queue)));
    }
}

async fn create_task_moves_past_schedule_time_to_now(t: &mut Target) {
    let queue = t.queue("schedule-past").await;
    t.pause_queue(PauseQueueRequest {
        name: queue.clone(),
    })
    .await
    .unwrap();
    for (label, seconds) in [
        ("an hour ago", seconds_from_now(-60 * 60)),
        ("1970-01-01T00:00:00Z", 0),
        ("1969-12-31T23:59:59Z", -1),
        ("0001-01-01T00:00:00Z", -62_135_596_800),
        ("0000-12-31T23:59:59Z", -62_135_596_801),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                schedule_time: Some(Timestamp { seconds, nanos: 0 }),
                ..t.task("", http("https://example.com/work"))
            }),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        // Whether task is due as it is created
        t.record(
            label,
            result.map(|task| {
                let lag = task.schedule_time.unwrap().seconds - task.create_time.unwrap().seconds;
                (0..=1).contains(&lag)
            }),
        );
    }
}

fn oidc(service_account: &str, audience: &str) -> Option<http_request::AuthorizationHeader> {
    Some(http_request::AuthorizationHeader::OidcToken(OidcToken {
        service_account_email: service_account.to_owned(),
        audience: audience.to_owned(),
    }))
}

fn oauth(service_account: &str, scope: &str) -> Option<http_request::AuthorizationHeader> {
    Some(http_request::AuthorizationHeader::OauthToken(OAuthToken {
        service_account_email: service_account.to_owned(),
        scope: scope.to_owned(),
    }))
}

async fn create_task_returns_oidc_token(t: &mut Target) {
    let queue = t.queue("oidc-token").await;
    for (label, audience) in [
        ("audience", "https://example.com/audience"),
        ("no audience", ""),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                "",
                HttpRequest {
                    authorization_header: oidc(&t.service_account, audience),
                    ..with_headers(&[("Authorization", "Bearer token")])
                },
            )),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.message_type));
    }
}

async fn create_task_returns_oauth_token(t: &mut Target) {
    let queue = t.queue("oauth-token").await;
    for (label, scope) in [
        ("scope", "https://www.googleapis.com/auth/userinfo.email"),
        ("no scope", ""),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                "",
                HttpRequest {
                    url: "https://example.googleapis.com/work".to_owned(),
                    authorization_header: oauth(&t.service_account, scope),
                    ..with_headers(&[("Authorization", "Bearer token")])
                },
            )),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.message_type));
    }
}

async fn create_task_rejects_invalid_tokens(t: &mut Target) {
    let queue = t.queue("invalid-tokens").await;
    let account = t.service_account.clone();
    let with_token = |url: &str, token| HttpRequest {
        authorization_header: token,
        ..http(url)
    };
    for (label, http_request) in [
        (
            "oidc without email",
            with_token("https://example.com/work", oidc("", "")),
        ),
        (
            "oidc with malformed email",
            with_token("https://example.com/work", oidc("not-an-email", "")),
        ),
        (
            "oidc with audience without scheme",
            with_token("https://example.com/work", oidc(&account, "audience")),
        ),
        (
            "oidc with email without domain",
            with_token("https://example.com/work", oidc("tasks@", "")),
        ),
        (
            "oidc with email without name",
            with_token("https://example.com/work", oidc("@example.com", "")),
        ),
        (
            "oidc with email with two @",
            with_token("https://example.com/work", oidc("tasks@a@example.com", "")),
        ),
        (
            "oauth without email",
            with_token("https://example.googleapis.com/work", oauth("", "")),
        ),
        (
            "oauth with malformed email",
            with_token(
                "https://example.googleapis.com/work",
                oauth("not-an-email", ""),
            ),
        ),
        (
            "oauth with scope without scheme",
            with_token(
                "https://example.googleapis.com/work",
                oauth(&account, "scope"),
            ),
        ),
        (
            "oauth to other host",
            with_token("https://example.com/work", oauth(&account, "")),
        ),
        (
            "oauth to bare googleapis.com",
            with_token("https://googleapis.com/work", oauth(&account, "")),
        ),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task("", http_request)),
            response_view: task::View::Unspecified.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.message_type));
    }
}

/// Decodes JWT's header and claims, with run-specific values as placeholders and `exp` as its lifetime
fn decode_jwt(t: &mut Target, token: &str) -> (BTreeMap<String, Value>, BTreeMap<String, Value>) {
    let decode = |part: &str| -> BTreeMap<String, Value> {
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(part).unwrap()).unwrap()
    };
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "{token} is a signed JWT");
    let header = decode(parts[0]);
    let mut claims = decode(parts[1]);
    t.placeholder(header["kid"].as_str().unwrap().to_owned(), "{kid}");
    t.placeholder(
        claims["sub"].as_str().unwrap().to_owned(),
        "{service_account_id}",
    );
    let issued = claims.remove("iat").unwrap().as_i64().unwrap();
    let expires = claims.remove("exp").unwrap().as_i64().unwrap();
    claims.insert("exp - iat".to_owned(), Value::from(expires - issued));
    (header, claims)
}

async fn dispatch_sends_oidc_token(t: &mut Target) {
    let queue = t.queue("dispatch-oidc").await;
    for (key, audience) in [
        ("oidc-audience", "https://example.com/audience"),
        ("oidc-url", ""),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                name: format!("{queue}/tasks/{key}"),
                message_type: Some(task::MessageType::HttpRequest(HttpRequest {
                    headers: BTreeMap::from([(
                        "Authorization".to_owned(),
                        "Bearer token".to_owned(),
                    )]),
                    authorization_header: oidc(&t.service_account, audience),
                    ..http(&format!("{}?query", t.receiver.url_for(key, "200")))
                })),
                ..Default::default()
            }),
            response_view: task::View::Unspecified.into(),
        };
        t.create_task(request).await.unwrap();
    }

    for key in ["oidc-audience", "oidc-url"] {
        let requests = t.receiver.wait_for(key, 1).await;
        let tokens: Vec<_> = requests[0]
            .headers
            .get_all("authorization")
            .iter()
            .map(|value| {
                let (scheme, token) = value.to_str().unwrap().split_once(' ').unwrap();
                (scheme.to_owned(), decode_jwt(t, token))
            })
            .collect();
        t.record(key, Ok::<_, Status>(tokens));
    }
}

async fn create_queue_limits_sampling_ratio(t: &mut Target) {
    for (label, sampling_ratio) in [
        ("0", 0.0),
        ("0.25", 0.25),
        ("1", 1.0),
        ("1.5", 1.5),
        ("-0.5", -0.5),
        ("NaN", f64::NAN),
    ] {
        let request = CreateQueueRequest {
            parent: t.parent.clone(),
            queue: Some(Queue {
                name: t.queue_name(&format!("sampling-{}", label.replace(['.', '-'], ""))),
                stackdriver_logging_config: Some(StackdriverLoggingConfig { sampling_ratio }),
                ..Default::default()
            }),
        };

        let result = t.create_queue(request).await;

        t.record(label, result.map(|queue| queue.stackdriver_logging_config));
    }
}

fn header_override(key: &str, value: &str) -> http_target::HeaderOverride {
    http_target::HeaderOverride {
        header: Some(http_target::Header {
            key: key.to_owned(),
            value: value.to_owned(),
        }),
    }
}

fn queue_oidc(service_account: &str, audience: &str) -> Option<http_target::AuthorizationHeader> {
    Some(http_target::AuthorizationHeader::OidcToken(OidcToken {
        service_account_email: service_account.to_owned(),
        audience: audience.to_owned(),
    }))
}

fn queue_oauth(service_account: &str, scope: &str) -> Option<http_target::AuthorizationHeader> {
    Some(http_target::AuthorizationHeader::OauthToken(OAuthToken {
        service_account_email: service_account.to_owned(),
        scope: scope.to_owned(),
    }))
}

async fn record_http_targets(t: &mut Target, prefix: &str, targets: Vec<(&str, HttpTarget)>) {
    for (label, http_target) in targets {
        let request = CreateQueueRequest {
            parent: t.parent.clone(),
            queue: Some(Queue {
                name: t.queue_name(&format!(
                    "{prefix}-{}",
                    label.replace(|c: char| !c.is_ascii_alphanumeric(), "-")
                )),
                http_target: Some(http_target),
                ..Default::default()
            }),
        };

        let result = t.create_queue(request).await;

        t.record(label, result.map(|queue| queue.http_target));
    }
}

async fn create_queue_returns_http_target(t: &mut Target) {
    let account = t.service_account.clone();
    let targets = vec![
        (
            "every field",
            HttpTarget {
                uri_override: Some(UriOverride {
                    scheme: Some(uri_override::Scheme::Http.into()),
                    host: Some("example.net".to_owned()),
                    port: Some(8080),
                    path_override: Some(PathOverride {
                        path: "/path".to_owned(),
                    }),
                    query_override: Some(QueryOverride {
                        query_params: "a=1&b=2".to_owned(),
                    }),
                    uri_override_enforce_mode: uri_override::UriOverrideEnforceMode::IfNotExists
                        .into(),
                }),
                http_method: HttpMethod::Put.into(),
                header_overrides: vec![
                    header_override("X-Custom", "queue"),
                    header_override("Content-Type", "text/plain"),
                ],
                authorization_header: queue_oidc(&account, "https://example.com/audience"),
            },
        ),
        ("no fields", HttpTarget::default()),
        (
            "empty uri override",
            HttpTarget {
                uri_override: Some(UriOverride::default()),
                ..Default::default()
            },
        ),
        (
            "uri override clearing port, path and query",
            HttpTarget {
                uri_override: Some(UriOverride {
                    port: Some(0),
                    path_override: Some(PathOverride::default()),
                    query_override: Some(QueryOverride::default()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        (
            "always enforced",
            HttpTarget {
                uri_override: Some(UriOverride {
                    host: Some("example.net".to_owned()),
                    uri_override_enforce_mode: uri_override::UriOverrideEnforceMode::Always.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        (
            "unspecified scheme",
            HttpTarget {
                uri_override: Some(UriOverride {
                    scheme: Some(uri_override::Scheme::Unspecified.into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        (
            "oidc without audience",
            HttpTarget {
                authorization_header: queue_oidc(&account, ""),
                ..Default::default()
            },
        ),
        (
            "oauth with scope",
            HttpTarget {
                authorization_header: queue_oauth(
                    &account,
                    "https://www.googleapis.com/auth/userinfo.email",
                ),
                ..Default::default()
            },
        ),
        (
            "oauth without scope",
            HttpTarget {
                authorization_header: queue_oauth(&account, ""),
                ..Default::default()
            },
        ),
    ];

    record_http_targets(t, "target", targets).await;
}

async fn create_queue_rejects_invalid_http_target(t: &mut Target) {
    let account = t.service_account.clone();
    let uri = |uri_override: UriOverride| HttpTarget {
        uri_override: Some(uri_override),
        ..Default::default()
    };
    let host = |host: &str| {
        uri(UriOverride {
            host: Some(host.to_owned()),
            ..Default::default()
        })
    };
    let path = |path: &str| {
        uri(UriOverride {
            path_override: Some(PathOverride {
                path: path.to_owned(),
            }),
            ..Default::default()
        })
    };
    let token = |authorization_header| HttpTarget {
        authorization_header,
        ..Default::default()
    };
    let targets = vec![
        ("empty host", host("")),
        ("host with scheme", host("https://example.net")),
        ("host with port", host("example.net:8080")),
        ("host with path", host("example.net/path")),
        (
            "port -1",
            uri(UriOverride {
                port: Some(-1),
                ..Default::default()
            }),
        ),
        (
            "port 65536",
            uri(UriOverride {
                port: Some(65536),
                ..Default::default()
            }),
        ),
        ("path without slash", path("path")),
        ("path with space", path("/a b")),
        (
            "query with question mark",
            uri(UriOverride {
                query_override: Some(QueryOverride {
                    query_params: "?a=1".to_owned(),
                }),
                ..Default::default()
            }),
        ),
        (
            "unknown scheme",
            uri(UriOverride {
                scheme: Some(9),
                ..Default::default()
            }),
        ),
        (
            "unknown enforce mode",
            uri(UriOverride {
                host: Some("example.net".to_owned()),
                uri_override_enforce_mode: 9,
                ..Default::default()
            }),
        ),
        (
            "unknown http method",
            HttpTarget {
                http_method: 99,
                ..Default::default()
            },
        ),
        (
            "oidc without email",
            token(queue_oidc("", "https://example.com/audience")),
        ),
        (
            "oidc with malformed email",
            token(queue_oidc("not-an-email", "https://example.com/audience")),
        ),
        (
            "oidc with audience without scheme",
            token(queue_oidc(&account, "audience")),
        ),
        ("oauth without email", token(queue_oauth("", ""))),
        (
            "oauth with malformed email",
            token(queue_oauth("not-an-email", "")),
        ),
        (
            "oauth with scope without scheme",
            token(queue_oauth(&account, "scope")),
        ),
    ];

    record_http_targets(t, "invalid", targets).await;
}

async fn create_queue_sanitises_header_overrides(t: &mut Target) {
    let headers = |overrides: Vec<http_target::HeaderOverride>| HttpTarget {
        header_overrides: overrides,
        ..Default::default()
    };
    let targets = vec![
        (
            "override without header",
            headers(vec![http_target::HeaderOverride { header: None }]),
        ),
        ("empty key", headers(vec![header_override("", "value")])),
        (
            "empty value",
            headers(vec![header_override("X-Custom", "")]),
        ),
        (
            "key with colon",
            headers(vec![header_override("X:Custom", "value")]),
        ),
        (
            "value with line break",
            headers(vec![header_override("X-Custom", "a\r\nb")]),
        ),
        (
            "duplicate keys",
            headers(vec![
                header_override("X-Custom", "first"),
                header_override("x-custom", "second"),
            ]),
        ),
        (
            "Host",
            headers(vec![header_override("Host", "example.net")]),
        ),
        (
            "lowercase host",
            headers(vec![header_override("host", "example.net")]),
        ),
        (
            "User-Agent",
            headers(vec![header_override("User-Agent", "agent")]),
        ),
        (
            "Content-Length",
            headers(vec![header_override("Content-Length", "5")]),
        ),
        (
            "Content-Type",
            headers(vec![header_override("Content-Type", "text/plain")]),
        ),
        (
            "Authorization",
            headers(vec![header_override("Authorization", "Bearer token")]),
        ),
        (
            "Connection",
            headers(vec![header_override("Connection", "close")]),
        ),
        (
            "Transfer-Encoding",
            headers(vec![header_override("Transfer-Encoding", "chunked")]),
        ),
        (
            "X-Forwarded-For",
            headers(vec![header_override("X-Forwarded-For", "127.0.0.1")]),
        ),
        (
            "X-Google-Custom",
            headers(vec![header_override("X-Google-Custom", "value")]),
        ),
        (
            "X-AppEngine-Custom",
            headers(vec![header_override("X-AppEngine-Custom", "value")]),
        ),
        (
            "X-CloudTasks-TaskName",
            headers(vec![header_override("X-CloudTasks-TaskName", "spoofed")]),
        ),
        (
            "X-CloudTasks-Custom",
            headers(vec![header_override("X-CloudTasks-Custom", "value")]),
        ),
    ];

    record_http_targets(t, "headers", targets).await;

    // Real Cloud Tasks lists invalid headers in varying order
    let result = retry_until!(
        t.create_queue(CreateQueueRequest {
            parent: t.parent.clone(),
            queue: Some(Queue {
                name: t.queue_name("headers-two-reserved"),
                http_target: Some(headers(vec![
                    header_override("User-Agent", "agent"),
                    header_override("Host", "example.net"),
                ])),
                ..Default::default()
            }),
        }),
        |result: &Result<Queue, Status>| result
            .as_ref()
            .is_err_and(|status| status.message().ends_with("User-Agent, Host"))
    );
    t.record(
        "two reserved headers",
        result.map(|queue| queue.http_target),
    );
}

async fn update_queue_changes_http_target(t: &mut Target) {
    let account = t.service_account.clone();
    let created = Queue {
        http_target: Some(HttpTarget {
            uri_override: Some(UriOverride {
                host: Some("example.net".to_owned()),
                path_override: Some(PathOverride {
                    path: "/path".to_owned(),
                }),
                ..Default::default()
            }),
            http_method: HttpMethod::Put.into(),
            header_overrides: vec![header_override("X-Custom", "created")],
            authorization_header: queue_oidc(&account, "https://example.com/audience"),
        }),
        stackdriver_logging_config: Some(StackdriverLoggingConfig {
            sampling_ratio: 0.5,
        }),
        ..Default::default()
    };
    let update = Queue {
        http_target: Some(HttpTarget {
            uri_override: Some(UriOverride {
                scheme: Some(uri_override::Scheme::Https.into()),
                port: Some(8080),
                query_override: Some(QueryOverride {
                    query_params: "u=1".to_owned(),
                }),
                ..Default::default()
            }),
            http_method: HttpMethod::Get.into(),
            header_overrides: vec![header_override("X-Other", "updated")],
            authorization_header: queue_oauth(
                &account,
                "https://www.googleapis.com/auth/userinfo.email",
            ),
        }),
        stackdriver_logging_config: Some(StackdriverLoggingConfig {
            sampling_ratio: 0.25,
        }),
        ..Default::default()
    };
    for (path, update) in [
        ("http_target", update.clone()),
        ("http_target.uri_override", update.clone()),
        ("http_target.uri_override.scheme", update.clone()),
        ("http_target.uri_override.host", update.clone()),
        ("http_target.uri_override.port", update.clone()),
        ("http_target.uri_override.path_override", update.clone()),
        (
            "http_target.uri_override.path_override.path",
            update.clone(),
        ),
        (
            "http_target.uri_override.query_override.query_params",
            update.clone(),
        ),
        (
            "http_target.uri_override.uri_override_enforce_mode",
            update.clone(),
        ),
        ("http_target.http_method", update.clone()),
        ("http_target.header_overrides", update.clone()),
        ("http_target.oidc_token", update.clone()),
        ("http_target.oidc_token.audience", update.clone()),
        ("http_target.oauth_token", update.clone()),
        ("http_target.oauth_token.scope", update.clone()),
        ("stackdriver_logging_config", update.clone()),
        ("stackdriver_logging_config.sampling_ratio", update.clone()),
        ("http_target removed", Queue::default()),
    ] {
        let name = t
            .queue_with(
                &format!("update-{}", path.replace(['.', '_', ' '], "-")),
                created.clone(),
            )
            .await;
        let request = UpdateQueueRequest {
            queue: Some(Queue { name, ..update }),
            update_mask: mask(&[path.split(' ').next().unwrap()]),
        };

        let result = t.update_queue(request).await;

        t.record(
            path,
            result.map(|queue| (queue.http_target, queue.stackdriver_logging_config)),
        );
    }
}

async fn create_task_takes_queue_token(t: &mut Target) {
    let account = t.service_account.clone();
    let token = |authorization_header| HttpTarget {
        authorization_header,
        ..Default::default()
    };
    for (label, http_target, http_request) in [
        (
            "oidc replaces task oauth",
            token(queue_oidc(&account, "https://example.com/audience")),
            HttpRequest {
                authorization_header: oauth(&account, ""),
                ..with_headers(&[("Authorization", "Bearer token")])
            },
        ),
        (
            "oidc replaces invalid task token",
            token(queue_oidc(&account, "https://example.com/audience")),
            HttpRequest {
                authorization_header: oidc("", ""),
                ..http("https://example.com/work")
            },
        ),
        (
            "oidc without audience",
            token(queue_oidc(&account, "")),
            http("https://example.com/work?query"),
        ),
        (
            "oidc without audience under uri override",
            HttpTarget {
                uri_override: Some(UriOverride {
                    host: Some("example.net".to_owned()),
                    path_override: Some(PathOverride {
                        path: "/path".to_owned(),
                    }),
                    ..Default::default()
                }),
                authorization_header: queue_oidc(&account, ""),
                ..Default::default()
            },
            http("https://example.com/work"),
        ),
        (
            "oauth without scope",
            token(queue_oauth(&account, "")),
            http("https://example.googleapis.com/work"),
        ),
        (
            "oauth to other host",
            token(queue_oauth(&account, "")),
            http("https://example.com/work"),
        ),
        (
            "no queue token keeps task token",
            HttpTarget {
                http_method: HttpMethod::Put.into(),
                ..Default::default()
            },
            HttpRequest {
                authorization_header: oidc(&account, "https://example.com/audience"),
                ..http("https://example.com/work")
            },
        ),
        (
            "get override keeps task method and body",
            HttpTarget {
                http_method: HttpMethod::Get.into(),
                ..Default::default()
            },
            HttpRequest {
                body: b"hi".to_vec(),
                ..http("https://example.com/work")
            },
        ),
    ] {
        let queue = t
            .queue_with(
                &format!("task-{}", label.replace(' ', "-")),
                Queue {
                    http_target: Some(http_target),
                    ..Default::default()
                },
            )
            .await;
        let request = CreateTaskRequest {
            parent: queue,
            task: Some(t.task("", http_request)),
            response_view: task::View::Full.into(),
        };

        let result = t.create_task(request).await;

        t.record(label, result.map(|task| task.message_type));
    }
}

fn to_receiver(t: &Target, path: &str) -> UriOverride {
    let url = reqwest::Url::parse(&t.receiver.url).unwrap();
    UriOverride {
        scheme: Some(
            match url.scheme() {
                "http" => uri_override::Scheme::Http,
                _ => uri_override::Scheme::Https,
            }
            .into(),
        ),
        host: Some(url.host_str().unwrap().to_owned()),
        port: url.port().map(i64::from),
        path_override: Some(PathOverride {
            path: path.to_owned(),
        }),
        ..Default::default()
    }
}

/// What handler sees of request, with any token as its decoded claims
fn handled(t: &mut Target, request: &Received) -> impl Debug + use<> {
    let headers: BTreeMap<_, _> = request
        .headers
        .iter()
        .filter(|(name, _)| {
            ["content-type", "user-agent", "x-custom", "x-other"].contains(&name.as_str())
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
        .collect();
    let tokens: Vec<_> = request
        .headers
        .get_all("authorization")
        .iter()
        .map(|value| {
            let (scheme, token) = value.to_str().unwrap().split_once(' ').unwrap();
            (scheme.to_owned(), decode_jwt(t, token).1)
        })
        .collect();
    (
        request.method.clone(),
        request.path.clone(),
        request.query.clone(),
        headers,
        String::from_utf8_lossy(&request.body).into_owned(),
        tokens,
    )
}

async fn dispatch_applies_http_target(t: &mut Target) {
    let account = t.service_account.clone();
    let always = t
        .queue_with(
            "target-always",
            Queue {
                http_target: Some(HttpTarget {
                    uri_override: Some(UriOverride {
                        query_override: Some(QueryOverride {
                            query_params: "q=1".to_owned(),
                        }),
                        ..to_receiver(t, "/target-always/overridden/200")
                    }),
                    http_method: HttpMethod::Put.into(),
                    header_overrides: vec![
                        header_override("X-Custom", "queue"),
                        header_override("X-Other", "queue"),
                    ],
                    authorization_header: queue_oidc(&account, "https://example.com/audience"),
                }),
                ..Default::default()
            },
        )
        .await;
    let if_not_exists = t
        .queue_with(
            "target-if-not-exists",
            Queue {
                http_target: Some(HttpTarget {
                    uri_override: Some(UriOverride {
                        path_override: Some(PathOverride {
                            path: "/target-if-not-exists/overridden/200".to_owned(),
                        }),
                        query_override: Some(QueryOverride {
                            query_params: "q=1".to_owned(),
                        }),
                        uri_override_enforce_mode:
                            uri_override::UriOverrideEnforceMode::IfNotExists.into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    let get = t
        .queue_with(
            "target-get",
            Queue {
                http_target: Some(HttpTarget {
                    http_method: HttpMethod::Get.into(),
                    ..Default::default()
                }),
                // One-hour backoff keeps failed task, and its last attempt
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
    for (queue, id, http_request) in [
        (
            &always,
            "with-headers",
            HttpRequest {
                http_method: HttpMethod::Post.into(),
                headers: BTreeMap::from([
                    ("X-Custom".to_owned(), "task".to_owned()),
                    ("Content-Type".to_owned(), "text/plain".to_owned()),
                ]),
                body: b"hi".to_vec(),
                ..http("https://example.com/target-always/kept/200?b=2")
            },
        ),
        (
            &always,
            "without-headers",
            http("https://example.com/target-always/kept/200"),
        ),
        (
            &if_not_exists,
            "t",
            http(&t.receiver.url_for("target-if-not-exists/kept", "200")),
        ),
        (
            &get,
            "with-body",
            HttpRequest {
                body: b"hi".to_vec(),
                ..http(&t.receiver.url_for("target-get", "200"))
            },
        ),
        (
            &get,
            "without-body",
            HttpRequest {
                headers: BTreeMap::from([("Content-Type".to_owned(), "text/plain".to_owned())]),
                ..http(&t.receiver.url_for("target-get", "200"))
            },
        ),
    ] {
        let request = CreateTaskRequest {
            parent: queue.clone(),
            task: Some(Task {
                name: format!("{queue}/tasks/{id}"),
                message_type: Some(task::MessageType::HttpRequest(http_request)),
                ..Default::default()
            }),
            response_view: task::View::Unspecified.into(),
        };
        t.create_task(request).await.unwrap();
    }

    for (key, count) in [
        ("target-always", 2),
        ("target-if-not-exists", 1),
        ("target-get", 1),
    ] {
        let mut requests = t.receiver.wait_for(key, count).await;
        requests.sort_by_key(|request| request.headers["x-cloudtasks-taskname"].clone());
        for request in &requests {
            let label = format!("{key} {:?}", request.headers["x-cloudtasks-taskname"]);
            let request = handled(t, request);
            t.record(&label, Ok::<_, Status>(request));
        }
    }
    let with_body = t
        .poll_task(&format!("{get}/tasks/with-body"), |result| {
            result.as_ref().is_ok_and(|task| {
                task.last_attempt
                    .as_ref()
                    .is_some_and(|attempt| attempt.response_status.is_some())
            })
        })
        .await;
    t.record(
        "target-get with body",
        with_body.map(|task| task.last_attempt.unwrap().response_status),
    );
}

async fn dispatch_applies_http_target_to_existing_tasks(t: &mut Target) {
    let account = t.service_account.clone();
    let queue = t
        .queue_with(
            "target-existing",
            Queue {
                retry_config: Some(RetryConfig {
                    max_attempts: -1,
                    min_backoff: seconds(1),
                    max_backoff: seconds(5),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    t.pause_queue(PauseQueueRequest {
        name: queue.clone(),
    })
    .await
    .unwrap();
    t.create_task(CreateTaskRequest {
        parent: queue.clone(),
        task: Some(due_task(
            &format!("{queue}/tasks/t"),
            "https://unreachable.invalid/target-existing/kept/200".to_owned(),
        )),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();
    t.update_queue(UpdateQueueRequest {
        queue: Some(Queue {
            name: queue.clone(),
            http_target: Some(HttpTarget {
                uri_override: Some(to_receiver(t, "/target-existing/overridden/200")),
                authorization_header: queue_oidc(&account, "https://example.com/audience"),
                ..Default::default()
            }),
            ..Default::default()
        }),
        update_mask: mask(&["http_target"]),
    })
    .await
    .unwrap();

    t.resume_queue(ResumeQueueRequest {
        name: queue.clone(),
    })
    .await
    .unwrap();

    // Real Cloud Tasks takes minutes to apply update, so earlier attempts fail on old URL
    let request = retry_until!(
        async { t.receiver.received("target-existing").pop() },
        Option::is_some,
        300
    )
    .unwrap();
    let request = handled(t, &request);
    t.record("request", Ok::<_, Status>(request));
}

async fn list_queues_filters_by_target(t: &mut Target) {
    let account = t.service_account.clone();
    t.queue_with(
        "target-filter-full",
        Queue {
            http_target: Some(HttpTarget {
                uri_override: Some(UriOverride {
                    host: Some("example.net".to_owned()),
                    port: Some(8080),
                    path_override: Some(PathOverride {
                        path: "/path".to_owned(),
                    }),
                    ..Default::default()
                }),
                http_method: HttpMethod::Put.into(),
                header_overrides: vec![header_override("X-Custom", "queue")],
                authorization_header: queue_oidc(&account, "https://example.com/audience"),
            }),
            stackdriver_logging_config: Some(StackdriverLoggingConfig {
                sampling_ratio: 0.5,
            }),
            ..Default::default()
        },
    )
    .await;
    t.queue_with(
        "target-filter-empty",
        Queue {
            http_target: Some(HttpTarget::default()),
            ..Default::default()
        },
    )
    .await;
    t.queue("target-filter-none").await;
    for filter in [
        "http_target: *",
        "http_target.http_method = PUT",
        "http_target.http_method: put",
        "http_target.http_method = HTTP_METHOD_UNSPECIFIED",
        "http_target.http_method > POST",
        "http_target.uri_override: *",
        "http_target.uri_override.host: example",
        "http_target.uri_override.host: *",
        "http_target.uri_override.port = 8080",
        "http_target.uri_override.scheme = HTTPS",
        "http_target.uri_override.scheme = SCHEME_UNSPECIFIED",
        "http_target.uri_override.uri_override_enforce_mode = URI_OVERRIDE_ENFORCE_MODE_UNSPECIFIED",
        "http_target.uri_override.path_override: *",
        "http_target.uri_override.path_override.path: path",
        "http_target.header_overrides: *",
        "http_target.header_overrides.header.key: X-Custom",
        "http_target.oidc_token: *",
        "http_target.oidc_token.audience: example",
        "http_target.oauth_token: *",
        "stackdriver_logging_config: *",
        "stackdriver_logging_config.sampling_ratio > 0.1",
        "stackdriver_logging_config.sampling_ratio = 0",
        "app_engine_routing_override.instance: *",
    ] {
        let result = filtered_queue_names(t, filter, "-target-filter-").await;

        t.record(filter, result);
    }
}

async fn batch_create_tasks_creates_every_task(t: &mut Target) {
    let queue = t.queue("batch-create").await;
    let request = BatchCreateTasksRequest {
        parent: queue.clone(),
        requests: vec![
            CreateTaskRequest {
                parent: queue.clone(),
                task: Some(t.task(&format!("{queue}/tasks/a"), http("https://example.com/a"))),
                response_view: task::View::Unspecified.into(),
            },
            CreateTaskRequest {
                parent: queue.clone(),
                task: Some(t.task(&format!("{queue}/tasks/b"), http("https://example.com/b"))),
                response_view: task::View::Full.into(),
            },
        ],
        request_id: String::new(),
    };

    let result = t.batch_create_tasks(request).await;
    let got = t
        .get_task(GetTaskRequest {
            name: format!("{queue}/tasks/a"),
            response_view: task::View::Unspecified.into(),
        })
        .await;

    t.record("batch_create_tasks", result);
    t.record("get_task", got.map(|task| task.name));
}

async fn batch_create_tasks_reports_failed_requests(t: &mut Target) {
    let queue = t.queue("batch-failures").await;
    let existing = format!("{queue}/tasks/existing");
    let deleted = format!("{queue}/tasks/deleted");
    for name in [&existing, &deleted] {
        t.create_task(CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(name, http("https://example.com/work"))),
            response_view: task::View::Unspecified.into(),
        })
        .await
        .unwrap();
    }
    t.delete_task(DeleteTaskRequest {
        name: deleted.clone(),
    })
    .await
    .unwrap();
    let create = |task| CreateTaskRequest {
        parent: queue.clone(),
        task,
        response_view: task::View::Unspecified.into(),
    };
    let request = BatchCreateTasksRequest {
        parent: queue.clone(),
        requests: vec![
            create(Some(t.task(
                &format!("{queue}/tasks/new"),
                http("https://example.com/work"),
            ))),
            create(None),
            create(Some(Task {
                message_type: None,
                ..t.task("", http(""))
            })),
            create(Some(t.task("", http("ftp://example.com/work")))),
            create(Some(t.task(
                "",
                HttpRequest {
                    http_method: HttpMethod::Get.into(),
                    body: b"hi".to_vec(),
                    ..http("https://example.com/work")
                },
            ))),
            create(Some(Task {
                dispatch_deadline: seconds(14),
                ..t.task("", http("https://example.com/work"))
            })),
            create(Some(Task {
                schedule_time: Some(Timestamp {
                    seconds: 4_102_444_800,
                    nanos: 0,
                }),
                ..t.task("", http("https://example.com/work"))
            })),
            create(Some(t.task(
                "",
                HttpRequest {
                    authorization_header: oauth(&t.service_account, ""),
                    ..http("https://example.com/work")
                },
            ))),
            create(Some(t.task(&existing, http("https://example.com/work")))),
            create(Some(t.task(&deleted, http("https://example.com/work")))),
        ],
        request_id: String::new(),
    };

    let result = t.batch_create_tasks(request).await;

    t.record("batch_create_tasks", result);
}

async fn batch_create_tasks_fails_when_every_request_fails(t: &mut Target) {
    let queue = t.queue("batch-all-fail").await;
    let missing = t.queue_name("batch-missing");
    for (label, parent, url) in [
        ("unknown queue", missing, "https://example.com/work"),
        ("every request invalid", queue, "ftp://example.com/work"),
    ] {
        let request = BatchCreateTasksRequest {
            parent: parent.clone(),
            requests: vec![CreateTaskRequest {
                parent,
                task: Some(t.task("", http(url))),
                response_view: task::View::Unspecified.into(),
            }],
            request_id: String::new(),
        };

        let result = t.batch_create_tasks(request).await;

        t.record(label, result);
    }
}

async fn batch_create_tasks_rejects_invalid_requests(t: &mut Target) {
    let queue = t.queue("batch-invalid").await;
    let other = t.queue_name("batch-other");
    let create = |parent: &str, name: &str| CreateTaskRequest {
        parent: parent.to_owned(),
        task: Some(t.task(name, http("https://example.com/work"))),
        response_view: task::View::Unspecified.into(),
    };
    for (label, parent, requests) in [
        ("no requests", queue.clone(), vec![]),
        ("101 requests", queue.clone(), vec![create(&queue, ""); 101]),
        (
            "request for another queue",
            queue.clone(),
            vec![create(&other, "")],
        ),
        (
            "request without parent",
            queue.clone(),
            vec![create("", "")],
        ),
        (
            "duplicate task names",
            queue.clone(),
            vec![
                create(&queue, &format!("{queue}/tasks/t")),
                create(&queue, &format!("{queue}/tasks/t")),
            ],
        ),
        (
            "invalid task id",
            queue.clone(),
            vec![create(&queue, &format!("{queue}/tasks/not valid"))],
        ),
        (
            "task named in another queue",
            queue.clone(),
            vec![create(&queue, &format!("{other}/tasks/t"))],
        ),
        ("empty parent", String::new(), vec![create("", "")]),
        (
            "parent not in a project",
            "not-a-valid-name".to_owned(),
            vec![create("not-a-valid-name", "")],
        ),
    ] {
        let request = BatchCreateTasksRequest {
            parent,
            requests,
            request_id: String::new(),
        };

        let result = t.batch_create_tasks(request).await;

        t.record(label, result);
    }
}

async fn batch_create_tasks_names_operation_by_request_id(t: &mut Target) {
    let queue = t.queue("batch-request-id").await;
    let batch = |t: &Target, id: &str| BatchCreateTasksRequest {
        parent: queue.clone(),
        requests: vec![CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task(
                &format!("{queue}/tasks/{id}"),
                http("https://example.com/work"),
            )),
            response_view: task::View::Unspecified.into(),
        }],
        request_id: format!("{}-request", t.run),
    };

    let first = t.batch_create_tasks(batch(t, "a")).await;
    let repeated = t.batch_create_tasks(batch(t, "b")).await;
    let got = t
        .get_task(GetTaskRequest {
            name: format!("{queue}/tasks/b"),
            response_view: task::View::Unspecified.into(),
        })
        .await;

    t.record("first", first.map(|operation| operation.name));
    t.record("repeated", repeated.map(|operation| operation.name));
    t.record("get_task", got.map(|task| task.name));
}

async fn batch_delete_tasks_deletes_every_task(t: &mut Target) {
    let queue = t.queue("batch-delete").await;
    for id in ["a", "b"] {
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
    let request = BatchDeleteTasksRequest {
        parent: queue.clone(),
        names: vec![format!("{queue}/tasks/a"), format!("{queue}/tasks/b")],
        request_id: String::new(),
    };

    let result = t.batch_delete_tasks(request).await;
    let got = t
        .get_task(GetTaskRequest {
            name: format!("{queue}/tasks/a"),
            response_view: task::View::Unspecified.into(),
        })
        .await;

    t.record("batch_delete_tasks", result);
    t.record("get_task", got.map(|task| task.name));
}

async fn batch_delete_tasks_reports_failed_requests(t: &mut Target) {
    let queue = t.queue("batch-delete-failures").await;
    for id in ["existing", "repeated", "deleted"] {
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
    t.delete_task(DeleteTaskRequest {
        name: format!("{queue}/tasks/deleted"),
    })
    .await
    .unwrap();
    let request = BatchDeleteTasksRequest {
        parent: queue.clone(),
        names: vec![
            format!("{queue}/tasks/existing"),
            format!("{queue}/tasks/missing"),
            format!("{queue}/tasks/deleted"),
            format!("{}/tasks/t", t.queue_name("batch-delete-other")),
            format!("{queue}/tasks/not valid"),
            "not-a-valid-name".to_owned(),
            String::new(),
            format!("{queue}/tasks/repeated"),
            format!("{queue}/tasks/repeated"),
            format!("{queue}/tasks/repeated-missing"),
            format!("{queue}/tasks/repeated-missing"),
        ],
        request_id: String::new(),
    };

    let result = t.batch_delete_tasks(request).await;

    t.record("batch_delete_tasks", result);
}

async fn batch_delete_tasks_fails_when_every_request_fails(t: &mut Target) {
    let queue = t.queue("batch-delete-all-fail").await;
    let missing = t.queue_name("batch-delete-missing");
    for (label, parent) in [("unknown queue", missing), ("every task missing", queue)] {
        let request = BatchDeleteTasksRequest {
            parent: parent.clone(),
            names: vec![format!("{parent}/tasks/a"), format!("{parent}/tasks/b")],
            request_id: String::new(),
        };

        let result = t.batch_delete_tasks(request).await;

        t.record(label, result);
    }
}

async fn batch_delete_tasks_rejects_invalid_requests(t: &mut Target) {
    let queue = t.queue("batch-delete-invalid").await;
    for (label, parent, names) in [
        ("no names", queue.clone(), vec![]),
        (
            "1001 names",
            queue.clone(),
            vec![format!("{queue}/tasks/t"); 1001],
        ),
        ("empty parent", String::new(), vec!["t".to_owned()]),
        (
            "parent not in a project",
            "not-a-valid-name".to_owned(),
            vec!["t".to_owned()],
        ),
    ] {
        let request = BatchDeleteTasksRequest {
            parent,
            names,
            request_id: String::new(),
        };

        let result = t.batch_delete_tasks(request).await;

        t.record(label, result);
    }
}

async fn batch_delete_tasks_shares_request_ids_with_batch_create(t: &mut Target) {
    let queue = t.queue("batch-delete-request-id").await;
    let request_id = format!("{}-delete-request", t.run);
    let delete = BatchDeleteTasksRequest {
        parent: queue.clone(),
        names: vec![format!("{queue}/tasks/missing")],
        request_id: request_id.clone(),
    };
    let create = BatchCreateTasksRequest {
        parent: queue.clone(),
        requests: vec![CreateTaskRequest {
            parent: queue.clone(),
            task: Some(t.task("", http("https://example.com/work"))),
            response_view: task::View::Unspecified.into(),
        }],
        request_id,
    };

    let first = t.batch_delete_tasks(delete.clone()).await;
    let repeated = t.batch_delete_tasks(delete).await;
    let created = t.batch_create_tasks(create).await;

    t.record("first", first.map(|operation| operation.name));
    t.record("repeated", repeated.map(|operation| operation.name));
    t.record(
        "batch_create_tasks",
        created.map(|operation| operation.name),
    );
}

async fn get_operation_returns_batch_operations(t: &mut Target) {
    let queue = t.queue("get-operation").await;
    let created = t
        .batch_create_tasks(BatchCreateTasksRequest {
            parent: queue.clone(),
            requests: vec![CreateTaskRequest {
                parent: queue.clone(),
                task: Some(t.task(
                    &format!("{queue}/tasks/t"),
                    http("https://example.com/work"),
                )),
                response_view: task::View::Unspecified.into(),
            }],
            request_id: String::new(),
        })
        .await
        .unwrap();
    let deleted = t
        .batch_delete_tasks(BatchDeleteTasksRequest {
            parent: queue.clone(),
            names: vec![format!("{queue}/tasks/t")],
            request_id: String::new(),
        })
        .await
        .unwrap();

    let got_create = t
        .get_operation::<BatchCreateTasksMetadata, BatchCreateTasksResponse>(GetOperationRequest {
            name: created.name,
        })
        .await;
    let got_delete = t
        .get_operation::<BatchDeleteTasksMetadata, ()>(GetOperationRequest { name: deleted.name })
        .await;

    t.record("get_operation batch create", got_create);
    t.record("get_operation batch delete", got_delete);
}

async fn get_operation_rejects_invalid_names(t: &mut Target) {
    for (label, name) in [
        (
            "unknown operation",
            format!("{}/operations/{}-missing", t.parent, t.run),
        ),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        (
            "name without operation id",
            format!("{}/operations", t.parent),
        ),
    ] {
        let result = t
            .get_operation::<BatchCreateTasksMetadata, BatchCreateTasksResponse>(
                GetOperationRequest { name },
            )
            .await;

        t.record(label, result);
    }
}

async fn only_get_operation_is_served(t: &mut Target) {
    let queue = t.queue("other-operation-rpcs").await;
    let operation = t
        .batch_delete_tasks(BatchDeleteTasksRequest {
            parent: queue.clone(),
            names: vec![format!("{queue}/tasks/t")],
            request_id: String::new(),
        })
        .await
        .unwrap();

    let listed = t
        .list_operations(ListOperationsRequest {
            name: t.parent.clone(),
            ..Default::default()
        })
        .await;
    let waited = t
        .wait_operation::<BatchDeleteTasksMetadata, ()>(WaitOperationRequest {
            name: operation.name.clone(),
            timeout: None,
        })
        .await;
    let cancelled = t
        .cancel_operation(CancelOperationRequest {
            name: operation.name.clone(),
        })
        .await;
    let deleted = t
        .delete_operation(DeleteOperationRequest {
            name: operation.name.clone(),
        })
        .await;
    let got = t
        .get_operation::<BatchDeleteTasksMetadata, ()>(GetOperationRequest {
            name: operation.name.clone(),
        })
        .await;

    t.record(
        "list_operations has operation",
        listed.map(|names| names.contains(&operation.name)),
    );
    t.record("wait_operation", waited.map(|waited| waited.name));
    t.record("cancel_operation", cancelled);
    t.record("delete_operation", deleted);
    t.record("get_operation", got.map(|got| got.name));
}

fn viewer(t: &Target) -> Binding {
    Binding {
        role: "roles/cloudtasks.viewer".to_owned(),
        members: vec![format!("serviceAccount:{}", t.service_account)],
        condition: None,
    }
}

async fn get_iam_policy_returns_empty_policy(t: &mut Target) {
    let queue = t.queue("empty-policy").await;
    let request = GetIamPolicyRequest {
        resource: queue,
        options: None,
    };

    let result = t.get_iam_policy(request).await;

    t.record("get_iam_policy", result);
}

async fn get_iam_policy_rejects_invalid_names(t: &mut Target) {
    for (label, resource) in [
        ("unknown queue", t.queue_name("policy-missing")),
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("name without queue id", format!("{}/queues", t.parent)),
    ] {
        let request = GetIamPolicyRequest {
            resource,
            options: None,
        };

        let result = t.get_iam_policy(request).await;

        t.record(label, result);
    }
}

async fn set_iam_policy_stores_policy(t: &mut Target) {
    let queue = t.queue("set-policy").await;
    let account = format!("serviceAccount:{}", t.service_account);
    let request = SetIamPolicyRequest {
        resource: queue.clone(),
        policy: Some(Policy {
            // Cloud Tasks merges repeated roles and members and drops roles without members
            bindings: vec![
                Binding {
                    members: vec![account.clone(), account],
                    ..viewer(t)
                },
                viewer(t),
                Binding {
                    role: "roles/cloudtasks.enqueuer".to_owned(),
                    members: vec![],
                    condition: None,
                },
            ],
            ..Default::default()
        }),
        update_mask: None,
    };

    let set = t.set_iam_policy(request).await;
    let got = t
        .get_iam_policy(GetIamPolicyRequest {
            resource: queue,
            options: Some(GetPolicyOptions {
                requested_policy_version: 3,
            }),
        })
        .await;

    t.record("set_iam_policy", set);
    t.record("get_iam_policy", got);
}

async fn set_iam_policy_checks_etag(t: &mut Target) {
    let queue = t.queue("policy-etag").await;
    let set = |t: &Target, etag: Vec<u8>| SetIamPolicyRequest {
        resource: queue.clone(),
        policy: Some(Policy {
            etag,
            bindings: vec![viewer(t)],
            ..Default::default()
        }),
        update_mask: None,
    };
    let first = t.set_iam_policy(set(t, vec![])).await.unwrap();
    let first_etag = STANDARD.decode(&first.etag).unwrap();

    let current = t.set_iam_policy(set(t, first_etag.clone())).await;
    let stale = t.set_iam_policy(set(t, first_etag)).await;
    let empty_policy_etag = t.set_iam_policy(set(t, vec![0x00, 0x20, 0x01])).await;
    let no_etag = t.set_iam_policy(set(t, vec![])).await;

    // Cuts message before first etag that changes on every set
    let cut = |result: Result<Etagged, Status>, at: &str| {
        result.map_err(|status| {
            let (message, _) = status.message().split_once(at).unwrap();
            Status::with_details(status.code(), message, status.details().to_vec().into())
        })
    };
    t.record("current etag", current.map(|policy| policy.etag));
    t.record("stale etag", cut(stale, " The request's ETag"));
    t.record(
        "empty policy etag",
        cut(empty_policy_etag, " did not match"),
    );
    t.record("no etag", no_etag.map(|policy| policy.etag));
}

async fn set_iam_policy_rejects_invalid_requests(t: &mut Target) {
    let queue = t.queue("invalid-policy").await;
    let with_binding = |binding: Binding| {
        Some(Policy {
            bindings: vec![binding],
            ..Default::default()
        })
    };
    for (label, resource, policy) in [
        (
            "role without prefix",
            queue.clone(),
            with_binding(Binding {
                role: "viewer".to_owned(),
                ..viewer(t)
            }),
        ),
        (
            "member without type",
            queue.clone(),
            with_binding(Binding {
                members: vec!["nobody".to_owned()],
                ..viewer(t)
            }),
        ),
        (
            "member of unknown type",
            queue.clone(),
            with_binding(Binding {
                members: vec!["robot:x@example.com".to_owned()],
                ..viewer(t)
            }),
        ),
        (
            "condition",
            queue.clone(),
            Some(Policy {
                version: 3,
                bindings: vec![Binding {
                    condition: Some(Expr {
                        title: "until 2030".to_owned(),
                        expression: r#"request.time < timestamp("2030-01-01T00:00:00Z")"#
                            .to_owned(),
                        ..Default::default()
                    }),
                    ..viewer(t)
                }],
                ..Default::default()
            }),
        ),
        (
            "version 4",
            queue.clone(),
            Some(Policy {
                version: 4,
                ..Default::default()
            }),
        ),
        ("no policy", queue.clone(), None),
        (
            "unknown queue",
            t.queue_name("invalid-policy-missing"),
            Some(Policy::default()),
        ),
        ("empty name", String::new(), Some(Policy::default())),
        (
            "name not in a project",
            "not-a-valid-name".to_owned(),
            Some(Policy::default()),
        ),
        (
            "name without queue id",
            format!("{}/queues", t.parent),
            Some(Policy::default()),
        ),
    ] {
        let request = SetIamPolicyRequest {
            resource,
            policy,
            update_mask: None,
        };

        let result = t.set_iam_policy(request).await;

        t.record(label, result);
    }
}

async fn test_iam_permissions_returns_requested_permissions(t: &mut Target) {
    let queue = t.queue("test-permissions").await;
    let request = TestIamPermissionsRequest {
        resource: queue,
        permissions: [
            "cloudtasks.tasks.run",
            "cloudtasks.queues.get",
            "cloudtasks.tasks.run",
            "cloudtasks.queues.setIamPolicy",
        ]
        .map(str::to_owned)
        .to_vec(),
    };

    let result = t.test_iam_permissions(request).await;

    t.record("test_iam_permissions", result);
}

async fn test_iam_permissions_rejects_invalid_requests(t: &mut Target) {
    let queue = t.queue("invalid-permissions").await;
    for (label, resource, permissions) in [
        ("no permissions", queue.clone(), vec![]),
        ("one part", queue.clone(), vec!["foo"]),
        ("empty part", queue.clone(), vec!["cloudtasks.queues."]),
        ("wildcard", queue.clone(), vec!["cloudtasks.*"]),
        (
            "another service",
            queue.clone(),
            vec!["storage.objects.get"],
        ),
        (
            "location permission",
            queue.clone(),
            vec!["cloudtasks.queues.create"],
        ),
        (
            "first invalid of several",
            queue.clone(),
            vec!["cloudtasks.queues.get", "storage.objects.get", "foo"],
        ),
        (
            "unknown queue",
            t.queue_name("invalid-permissions-missing"),
            vec![],
        ),
        ("empty name", String::new(), vec!["cloudtasks.queues.get"]),
        (
            "name not in a project",
            "not-a-valid-name".to_owned(),
            vec!["cloudtasks.queues.get"],
        ),
        (
            "name without queue id",
            format!("{}/queues", t.parent),
            vec!["cloudtasks.queues.get"],
        ),
    ] {
        let request = TestIamPermissionsRequest {
            resource,
            permissions: permissions.into_iter().map(str::to_owned).collect(),
        };

        let result = t.test_iam_permissions(request).await;

        t.record(label, result);
    }
}

async fn get_cmek_config_returns_config(t: &mut Target) {
    let request = GetCmekConfigRequest {
        name: format!("{}/cmekConfig", t.parent),
    };

    let result = t.get_cmek_config(request).await;

    t.record("get_cmek_config", result);
}

async fn get_cmek_config_rejects_invalid_names(t: &mut Target) {
    for (label, name) in [
        ("empty name", String::new()),
        ("name not in a project", "not-a-valid-name".to_owned()),
        ("location name", t.parent.clone()),
        ("other config name", format!("{}/kmsConfig", t.parent)),
    ] {
        let result = t.get_cmek_config(GetCmekConfigRequest { name }).await;

        t.record(label, result);
    }
}

async fn update_cmek_config_rejects_invalid_requests(t: &mut Target) {
    // Run creates no queues here, as queue with header overrides blocks every key check
    let location = format!("{}/locations/us-central1", t.project);
    let name = format!("{location}/cmekConfig");
    let key_ring = format!("{location}/keyRings/missing");
    // Every key here is rejected, as real one would turn on CMEK for location
    let config = |name: &str, kms_key: &str| {
        Some(CmekConfig {
            name: name.to_owned(),
            kms_key: kms_key.to_owned(),
        })
    };
    for (label, cmek_config) in [
        ("malformed key", config(&name, "not-a-key")),
        ("key ring", config(&name, &key_ring)),
        (
            "key version",
            config(
                &name,
                &format!("{key_ring}/cryptoKeys/missing/cryptoKeyVersions/1"),
            ),
        ),
        (
            "key in another location",
            config(
                &name,
                &format!("{}/keyRings/missing/cryptoKeys/missing", t.parent),
            ),
        ),
        ("no config", None),
        ("empty name", config("", "not-a-key")),
        (
            "name not in a project",
            config("not-a-valid-name", "not-a-key"),
        ),
        ("location name", config(&location, "not-a-key")),
    ] {
        let request = UpdateCmekConfigRequest {
            cmek_config,
            update_mask: None,
        };

        let result = t.update_cmek_config(request).await;

        t.record(label, result);
    }
}

async fn update_cmek_config_refuses_queues_with_header_overrides(t: &mut Target) {
    t.queue_with(
        "cmek-header-overrides",
        Queue {
            http_target: Some(HttpTarget {
                header_overrides: vec![header_override("X-Custom", "value")],
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;
    let request = UpdateCmekConfigRequest {
        cmek_config: Some(CmekConfig {
            name: format!("{}/cmekConfig", t.parent),
            kms_key: "not-a-key".to_owned(),
        }),
        update_mask: None,
    };

    let result = t.update_cmek_config(request).await;

    t.record("update_cmek_config", result);
}

async fn rest_manages_queues(t: &mut Target) {
    let name = t.queue_name("rest-queue");
    let queue = json!({
        "name": name,
        "rateLimits": {"maxDispatchesPerSecond": 10},
        "retryConfig": {"maxAttempts": -1, "minBackoff": "1.5s"},
        "httpTarget": {
            "uriOverride": {"host": "example.com"},
            "httpMethod": "PUT",
            "oidcToken": {"serviceAccountEmail": t.service_account},
        },
        "stackdriverLoggingConfig": {"samplingRatio": 0.5},
    });
    let update = json!({"rate_limits": {"max_dispatches_per_second": 2}});

    let queues = format!("/v2/{}/queues", t.parent);
    t.rest("create", Method::POST, &queues, &queue.to_string())
        .await;
    t.rest("get", Method::GET, &format!("/v2/{name}"), "").await;
    let mask = "updateMask=rateLimits.maxDispatchesPerSecond";
    let path = format!("/v2/{name}?{mask}");
    t.rest("update", Method::PATCH, &path, &update.to_string())
        .await;
    t.rest("pause", Method::POST, &format!("/v2/{name}:pause"), "")
        .await;
    t.rest("resume", Method::POST, &format!("/v2/{name}:resume"), "{}")
        .await;
    let purged = t
        .rest("purge", Method::POST, &format!("/v2/{name}:purge"), "{}")
        .await;
    t.rest("delete", Method::DELETE, &format!("/v2/{name}"), "")
        .await;

    t.json_placeholder(&purged, "/purgeTime", "{purge_time}");
}

async fn rest_lists_queues(t: &mut Target) {
    t.queue("restlist").await;

    // Other cases' queues share location
    let path = format!("/v2/{}/queues?filter=name:restlist", t.parent);
    let listed = t.rest("list", Method::GET, &path, "").await;

    t.json_placeholder(&listed, "/nextPageToken", "{page_token}");
}

async fn rest_manages_tasks(t: &mut Target) {
    let queue = t.queue("rest-tasks").await;
    let name = format!("{queue}/tasks/t");
    // No headers beyond User-Agent that Cloud Tasks adds, as it orders map entries differently on each request
    let request = json!({
        "task": {
            "name": name,
            "scheduleTime": t.tomorrow.to_string(),
            "dispatchDeadline": "30s",
            "httpRequest": {
                "url": "https://example.com/work",
                "httpMethod": "PUT",
                "body": "aGk=",
            },
        },
        "responseView": "FULL",
    });

    let tasks = format!("/v2/{queue}/tasks");
    let created = t
        .rest("create", Method::POST, &tasks, &request.to_string())
        .await;
    let path = format!("/v2/{name}?responseView=FULL");
    t.rest("get", Method::GET, &path, "").await;
    let listed = t.rest("list", Method::GET, &tasks, "").await;
    t.rest("delete", Method::DELETE, &format!("/v2/{name}"), "")
        .await;

    t.json_placeholder(&created, "/createTime", "{create_time}");
    t.json_placeholder(&listed, "/nextPageToken", "{page_token}");
}

async fn rest_runs_tasks(t: &mut Target) {
    let queue = t.queue("rest-run").await;
    let name = format!("{queue}/tasks/t");
    t.create_task(CreateTaskRequest {
        parent: queue,
        task: Some(t.task(&name, http(&t.receiver.url_for("rest-run", "200")))),
        response_view: task::View::Unspecified.into(),
    })
    .await
    .unwrap();

    let run = t
        .rest("run", Method::POST, &format!("/v2/{name}:run"), "{}")
        .await;

    t.json_placeholder(&run, "/createTime", "{create_time}");
}

async fn rest_runs_batches(t: &mut Target) {
    let queue = t.queue("rest-batch").await;
    let name = format!("{queue}/tasks/t");
    let create = json!({
        "requests": [{
            "parent": queue,
            "task": {
                "name": name,
                "scheduleTime": t.tomorrow.to_string(),
                "httpRequest": {"url": "https://example.com/work"},
            },
        }],
        "requestId": format!("{}-rest-create", t.run),
    });
    let delete = json!({
        "names": [name],
        "requestId": format!("{}-rest-delete", t.run),
    });

    let path = format!("/v2/{queue}/tasks:batchCreate");
    let created = t
        .rest("batch create", Method::POST, &path, &create.to_string())
        .await;
    let path = format!("/v2/{queue}/tasks:batchDelete");
    let deleted = t
        .rest("batch delete", Method::POST, &path, &delete.to_string())
        .await;
    let path = format!("/v2/{}/operations/{}-rest-create", t.parent, t.run);
    t.rest("get operation", Method::GET, &path, "").await;

    for operation in [&created, &deleted] {
        t.json_placeholder(operation, "/metadata/startTime", "{start_time}");
        t.json_placeholder(operation, "/metadata/endTime", "{end_time}");
    }
    t.json_placeholder(&created, "/response/tasks/0/createTime", "{create_time}");
}

async fn rest_manages_iam_policies(t: &mut Target) {
    let queue = t.queue("rest-iam").await;
    let policy = json!({
        "policy": {
            "bindings": [{
                "role": "roles/cloudtasks.viewer",
                "members": [format!("serviceAccount:{}", t.service_account)],
            }],
        },
    });
    let permissions = json!({"permissions": ["cloudtasks.queues.get", "cloudtasks.tasks.create"]});

    t.rest(
        "get",
        Method::POST,
        &format!("/v2/{queue}:getIamPolicy"),
        "{}",
    )
    .await;
    let path = format!("/v2/{queue}:setIamPolicy");
    let set = t
        .rest("set", Method::POST, &path, &policy.to_string())
        .await;
    let path = format!("/v2/{queue}:testIamPermissions");
    t.rest("test", Method::POST, &path, &permissions.to_string())
        .await;

    t.json_placeholder(&set, "/etag", "{etag}");
}

async fn rest_manages_cmek_config(t: &mut Target) {
    // Run creates no queues here, and key is rejected, as real one would turn on CMEK for location
    let path = format!("/v2/{}/locations/us-central1/cmekConfig", t.project);

    t.rest("get", Method::GET, &path, "").await;
    t.rest("update", Method::PATCH, &path, r#"{"kmsKey": "not-a-key"}"#)
        .await;
}

async fn rest_reports_errors(t: &mut Target) {
    let queue = json!({
        "name": t.queue_name("rest-errors"),
        "httpTarget": {"uriOverride": {"host": "https://example.com"}},
    });

    let path = format!("/v2/{}/queues/not_valid", t.parent);
    t.rest("invalid queue id", Method::GET, &path, "").await;
    let path = format!("/v2/{}", t.queue_name("rest-missing"));
    t.rest("unknown queue", Method::GET, &path, "").await;
    let path = format!("/v2/{}/queues", t.parent);
    t.rest(
        "invalid http target",
        Method::POST,
        &path,
        &queue.to_string(),
    )
    .await;
}

async fn rest_rejects_invalid_requests(t: &mut Target) {
    let tasks = format!("/v2/{}/tasks", t.queue_name("rest-invalid"));

    t.rest("unknown field", Method::POST, &tasks, r#"{"bogus": 1}"#)
        .await;
    let body = r#"{"task": {"httpRequest": {"bogus": 1, "other": 2}}}"#;
    t.rest("unknown nested fields", Method::POST, &tasks, body)
        .await;
    let path = format!("{tasks}?bogus=1");
    t.rest("unknown query parameter", Method::GET, &path, "")
        .await;
    let body = r#"{"task": {"name": 5}}"#;
    t.rest("wrong type", Method::POST, &tasks, body).await;
    let path = format!("{tasks}?pageSize=abc");
    t.rest("wrong query parameter type", Method::GET, &path, "")
        .await;
    let path = format!("{tasks}?responseView=NOPE");
    t.rest("unknown enum", Method::GET, &path, "").await;
    t.rest("array body", Method::POST, &tasks, "[]").await;
    let malformed = t
        .rest("malformed json", Method::POST, &tasks, r#"{"task": "#)
        .await;

    // Cuts parser's account of error, which only Google's parser gives
    let body: Value = serde_json::from_str(&malformed).unwrap();
    let message = body["error"]["message"].to_string();
    let (_, account) = message
        .trim_matches('"')
        .split_once("Invalid JSON payload received. ")
        .unwrap();
    t.placeholder(account.to_owned(), "{account}");
}

async fn rest_serves_only_mapped_routes(t: &mut Target) {
    let operations = format!("/v2/{}/operations", t.parent);
    let operation = format!("{operations}/{}-missing", t.run);

    t.rest("list operations", Method::GET, &operations, "")
        .await;
    let path = format!("{operation}:wait");
    t.rest("wait operation", Method::POST, &path, "{}").await;
    let path = format!("{operation}:cancel");
    t.rest("cancel operation", Method::POST, &path, "{}").await;
    t.rest("delete operation", Method::DELETE, &operation, "")
        .await;
    t.rest("unknown path", Method::GET, "/v2/bogus", "").await;
    let path = format!("/v2/{}:pause", t.queue_name("rest-routes"));
    t.rest("wrong method", Method::DELETE, &path, "").await;
}

async fn rest_follows_system_parameters(t: &mut Target) {
    let queue = t.queue("rest-parameters").await;

    let path = format!("/v2/{queue}?$alt=json;enum-encoding=int");
    t.rest("enum numbers", Method::GET, &path, "").await;
    let path = format!("/v2/{queue}?alt=json&prettyPrint=false");
    t.rest("compact", Method::GET, &path, "").await;
}
