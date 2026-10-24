use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Bound;
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jiff::fmt::strtime::BrokenDownTime;
use prost::Message;
use prost_reflect::{DynamicMessage, Kind, ReflectMessage};
use prost_types::{Any, Timestamp};
use tonic::{Code, Request, Response, Status};

use crate::proto::google::cloud::tasks::v2 as pb;
use crate::proto::google::cloud::tasks::v2::cloud_tasks_server::CloudTasks;
use crate::proto::google::iam::v1 as iam;
use crate::proto::google::longrunning::operations_server::Operations;
use crate::proto::google::longrunning::{self, Operation, operation};
use crate::proto::google::rpc;
use crate::state::{
    Attempt, AuthorizationHeader, DEFAULT_SCOPE, Emulator, HttpMethod, HttpRequest, HttpTarget,
    Queue, QueueConfig, QueueState, RateLimits, RetryConfig, State, Task, UriOverride,
};
use crate::{filter, names};

const QUEUE_NOT_FOUND: &str = "Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.";
const QUEUE_RECENTLY_DELETED: &str =
    "The queue cannot be created because a queue with this name existed too recently.";
const TASK_RECENTLY_REMOVED: &str = "The task cannot be created because a task with this name existed too recently. For more information about task de-duplication see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create#body.request_body.FIELDS.task.";
const TASK_GONE: &str = "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.";
const NOT_FOUND: &str = "Requested entity was not found.";
const QUEUE_NAME_DOCS: &str = "https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues#Queue.FIELDS.name";
const TASK_NAME_DOCS: &str = "https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name";
/// Wording and code are guesses, as recording this needs Cloud KMS key
const CMEK_HEADER_OVERRIDES: &str =
    "Queues with HTTP Target Override headers cannot be created or updated while CMEK is enabled.";
/// How Google's front end refuses Operations RPCs that Cloud Tasks does not serve
const UNSERVED: &str = "Method doesn't allow unregistered callers (callers without established identity). Please use API Key or other form of API consumer identity to call this API.";
/// Dropped from task headers and rejected in queue header overrides, along with `RESERVED_PREFIXES`
const RESERVED_HEADERS: [&str; 14] = [
    "accept-encoding",
    "connection",
    "content-length",
    "from",
    "host",
    "keep-alive",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "transfer-encoding",
    "upgrade",
    "user-agent",
    "via",
    "x-forwarded-for",
];
const RESERVED_PREFIXES: [&str; 2] = ["x-google", "x-appengine"];
const MEMBER_PREFIXES: [&str; 7] = [
    "user:",
    "serviceAccount:",
    "group:",
    "domain:",
    "principal:",
    "principalSet:",
    "deleted:",
];
const QUEUE_PERMISSIONS: [&str; 14] = [
    "cloudtasks.queues.delete",
    "cloudtasks.queues.get",
    "cloudtasks.queues.getIamPolicy",
    "cloudtasks.queues.pause",
    "cloudtasks.queues.purge",
    "cloudtasks.queues.resume",
    "cloudtasks.queues.setIamPolicy",
    "cloudtasks.queues.update",
    "cloudtasks.tasks.create",
    "cloudtasks.tasks.delete",
    "cloudtasks.tasks.fullView",
    "cloudtasks.tasks.get",
    "cloudtasks.tasks.list",
    "cloudtasks.tasks.run",
];
/// Largest encoded task, 1 MiB less 5 bytes Cloud Tasks stores beyond proto
const MAX_TASK_SIZE: usize = 1024 * 1024 - 5;

/// Checks task and adds it to queue, failing as CreateTask does
fn insert_task(
    state: &mut State,
    parent: &str,
    name: String,
    task: pb::Task,
    view: pb::task::View,
) -> Result<pb::Task, Status> {
    let mut http_request = match task.message_type {
        Some(pb::task::MessageType::HttpRequest(http)) => http_request(http)?,
        Some(pb::task::MessageType::AppEngineHttpRequest(_)) => return Err(no_app_engine(parent)),
        None => return Err(Status::invalid_argument("Task.payload_type is not set.")),
    };
    let task_retry_config = task.retry_config.map(retry_config).transpose()?;
    let dispatch_deadline = task
        .dispatch_deadline
        .map(|deadline| {
            Duration::try_from(deadline)
                .ok()
                // Cloud Tasks keeps milliseconds
                .map(|deadline| Duration::from_millis(deadline.as_millis() as u64))
                .filter(|deadline| {
                    (Duration::from_secs(15)..=Duration::from_secs(30 * 60)).contains(deadline)
                })
                .ok_or_else(|| {
                    Status::invalid_argument("Task.dispatchDeadline must be between [15s, 30m].")
                })
        })
        .transpose()?
        .unwrap_or(Duration::from_secs(10 * 60));
    let now = SystemTime::now();
    let schedule_time = match task.schedule_time {
        None => now,
        // Past 9999, which Timestamp cannot hold
        Some(time) if time.seconds > 253_402_300_799 => {
            return Err(Status::invalid_argument(
                "Request contains an invalid argument.",
            ));
        }
        Some(time) => {
            let schedule_time = SystemTime::try_from(time)
                .ok()
                .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
                // Cloud Tasks keeps microseconds
                .map(|since_epoch| {
                    SystemTime::UNIX_EPOCH + Duration::from_micros(since_epoch.as_micros() as u64)
                })
                // Cloud Tasks moves any past time to now, even one before epoch
                .filter(|&schedule_time| schedule_time > now)
                .unwrap_or(now);
            if schedule_time > now + Duration::from_secs(720 * 60 * 60) {
                return Err(Status::invalid_argument(format!(
                    "The Task.scheduleTime, {}, is too far in the future. Schedule time must be no more than 720h in the future.",
                    pacific_time(time)
                )));
            }
            schedule_time
        }
    };

    let queue = state
        .queues
        .get_mut(parent)
        .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
    // Queue's token replaces task's before URL check, which never sees replaced one
    if let Some(header) = queue
        .config
        .http_target
        .as_ref()
        .and_then(|target| target.authorization_header.clone())
    {
        http_request.authorization_header = Some(header);
    }
    // Checks only host, allowing http URLs Cloud Tasks rejects tokens for, so local handlers get tokens
    if let Some(AuthorizationHeader::OAuthToken { .. }) = http_request.authorization_header
        && !reqwest::Url::parse(&http_request.url).is_ok_and(|url| {
            url.host_str()
                .is_some_and(|host| host.ends_with(".googleapis.com"))
        })
    {
        return Err(Status::invalid_argument(
            r#"Invalid url for HttpRequest.oauth_token. The hostname in the url must end with ".googleapis.com""#,
        ));
    }
    if queue.tasks.contains_key(&name) {
        return Err(Status::already_exists("Requested entity already exists"));
    }
    if queue.removed_tasks.contains(&name, now) {
        return Err(Status::already_exists(TASK_RECENTLY_REMOVED));
    }
    let task = Task {
        name,
        http_request,
        retry_config: task_retry_config,
        schedule_time,
        create_time: now,
        dispatch_deadline,
        dispatch_count: 0,
        response_count: 0,
        first_attempt: None,
        last_attempt: None,
        run_requested: false,
    };
    if task_to_proto(&task, pb::task::View::Full).encoded_len() > MAX_TASK_SIZE {
        return Err(Status::invalid_argument("Task size too large"));
    }
    let response = task_to_proto(&task, view);
    queue.tasks.insert(task.name.clone(), task);
    Ok(response)
}

/// Checks what create or update request asks for, before any mask merges it
fn check_requested(queue: &pb::Queue) -> Result<(), Status> {
    if queue.app_engine_routing_override.is_some() {
        return Err(no_app_engine(&queue.name));
    }
    // In full, as Cloud Tasks skips token check once mask merges target
    if let Some(target) = &queue.http_target
        && let Some(header) = http_target(target.clone())?.authorization_header
    {
        service_account(&header)?;
    }
    Ok(())
}

#[tonic::async_trait]
impl CloudTasks for Emulator {
    async fn list_queues(
        &self,
        request: Request<pb::ListQueuesRequest>,
    ) -> Result<Response<pb::ListQueuesResponse>, Status> {
        let request = request.into_inner();
        resource(
            &request.parent,
            names::LOCATION,
            "cloudtasks.queues.list",
            "google.cloud.tasks.v2.CloudTasks.ListQueues",
        )?;
        let filter = filter::parse(&request.filter)?;
        let state = self.state.lock().unwrap();
        let prefix = format!("{}/queues/", request.parent);
        // Filtering within page, which proto allows to return fewer queues than page size
        let (mut queues, next_page_token) = page(
            &state.queues,
            &prefix,
            "ListQueuesRequest",
            request.page_size,
            9800,
            &request.page_token,
            queue_to_proto,
        )?;
        queues.retain(|queue| filter.matches(queue));
        Ok(Response::new(pb::ListQueuesResponse {
            queues,
            next_page_token,
        }))
    }

    async fn get_queue(
        &self,
        request: Request<pb::GetQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::QUEUE,
            "cloudtasks.queues.get",
            "google.cloud.tasks.v2.CloudTasks.GetQueue",
        )?;
        let state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get(&request.name)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        Ok(Response::new(queue_to_proto(queue)))
    }

    async fn create_queue(
        &self,
        request: Request<pb::CreateQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        resource(
            &request.parent,
            names::LOCATION,
            "cloudtasks.queues.create",
            "google.cloud.tasks.v2.CloudTasks.CreateQueue",
        )?;
        // Cloud Tasks reads missing queue as one with empty name
        let queue = request.queue.unwrap_or_default();
        let ids = names::ids(&queue.name, names::QUEUE).ok_or_else(|| {
            detailed(
                Code::InvalidArgument,
                r#"Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>"."#,
                vec![help("Definition of queue name", QUEUE_NAME_DOCS)],
            )
        })?;
        check_ids(&ids, names::QUEUE)?;
        if !queue
            .name
            .starts_with(&format!("{}/queues/", request.parent))
        {
            return Err(Status::invalid_argument(format!(
                "{} must begin with {}.",
                queue.name, request.parent
            )));
        }
        check_requested(&queue)?;
        let name = queue.name.clone();
        let config = queue_config(queue)?;

        let now = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        if state.queues.contains_key(&name) {
            return Err(Status::already_exists("Queue already exists"));
        }
        if state.deleted_queues.contains(&name, now) {
            return Err(Status::failed_precondition(QUEUE_RECENTLY_DELETED));
        }
        if config
            .http_target
            .as_ref()
            .is_some_and(|target| !target.header_overrides.is_empty())
            && state.cmek_keys.contains_key(&request.parent)
        {
            return Err(Status::failed_precondition(CMEK_HEADER_OVERRIDES));
        }
        let queue = Queue::new(name, config, now);
        let response = queue_to_proto(&queue);
        state.queues.insert(queue.name.clone(), queue);
        Ok(Response::new(response))
    }

    async fn update_queue(
        &self,
        request: Request<pb::UpdateQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        // Cloud Tasks reads missing queue as one with empty name
        let update = request.queue.unwrap_or_default();
        resource(
            &update.name,
            names::QUEUE,
            "cloudtasks.queues.update",
            "google.cloud.tasks.v2.CloudTasks.UpdateQueue",
        )?;
        // Cloud Tasks checks request even outside mask
        check_requested(&update)?;

        let now = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        if !state.queues.contains_key(&update.name)
            && state.deleted_queues.contains(&update.name, now)
        {
            return Err(Status::failed_precondition(QUEUE_RECENTLY_DELETED));
        }
        // Unmasked fields of new queue take their defaults
        let current = state.queues.get(&update.name).map_or_else(
            || pb::Queue {
                name: update.name.clone(),
                ..Default::default()
            },
            queue_to_proto,
        );
        // Absent mask updates every field, while empty one updates none
        let merged = match request.update_mask {
            None => update,
            Some(mask) => masked(current, update, &mask.paths)?,
        };
        let name = merged.name.clone();
        let config = queue_config(merged)?;
        let (location, _) = name.rsplit_once("/queues/").expect("checked queue name");
        if config
            .http_target
            .as_ref()
            .is_some_and(|target| !target.header_overrides.is_empty())
            && state.cmek_keys.contains_key(location)
        {
            return Err(Status::failed_precondition(CMEK_HEADER_OVERRIDES));
        }
        let queue = state
            .queues
            .entry(name.clone())
            .or_insert_with(|| Queue::new(name, config.clone(), now));
        queue.config = config;
        let response = queue_to_proto(queue);
        drop(state);
        self.wake.notify_one();
        Ok(Response::new(response))
    }

    async fn delete_queue(
        &self,
        request: Request<pb::DeleteQueueRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::QUEUE,
            "cloudtasks.queues.delete",
            "google.cloud.tasks.v2.CloudTasks.DeleteQueue",
        )?;
        let mut state = self.state.lock().unwrap();
        state
            .queues
            .remove(&request.name)
            .ok_or_else(|| Status::not_found(NOT_FOUND))?;
        state.deleted_queues.insert(request.name, SystemTime::now());
        Ok(Response::new(()))
    }

    async fn purge_queue(
        &self,
        request: Request<pb::PurgeQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::QUEUE,
            "cloudtasks.queues.purge",
            "google.cloud.tasks.v2.CloudTasks.PurgeQueue",
        )?;
        let mut state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.name)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        queue.purge(SystemTime::now());
        Ok(Response::new(queue_to_proto(queue)))
    }

    async fn pause_queue(
        &self,
        request: Request<pb::PauseQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::QUEUE,
            "cloudtasks.queues.pause",
            "google.cloud.tasks.v2.CloudTasks.PauseQueue",
        )?;
        let mut state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.name)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        queue.state = QueueState::Paused;
        Ok(Response::new(queue_to_proto(queue)))
    }

    async fn resume_queue(
        &self,
        request: Request<pb::ResumeQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::QUEUE,
            "cloudtasks.queues.resume",
            "google.cloud.tasks.v2.CloudTasks.ResumeQueue",
        )?;
        let mut state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.name)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        queue.state = QueueState::Running;
        let response = queue_to_proto(queue);
        drop(state);
        self.wake.notify_one();
        Ok(Response::new(response))
    }

    async fn get_iam_policy(
        &self,
        request: Request<iam::GetIamPolicyRequest>,
    ) -> Result<Response<iam::Policy>, Status> {
        let request = request.into_inner();
        resource(
            &request.resource,
            names::QUEUE,
            "cloudtasks.queues.list",
            "google.cloud.tasks.v2.CloudTasks.GetIamPolicy",
        )?;
        let state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get(&request.resource)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        Ok(Response::new(queue.policy.clone()))
    }

    async fn set_iam_policy(
        &self,
        request: Request<iam::SetIamPolicyRequest>,
    ) -> Result<Response<iam::Policy>, Status> {
        let request = request.into_inner();
        resource(
            &request.resource,
            names::QUEUE,
            "cloudtasks.queues.list",
            "google.cloud.tasks.v2.CloudTasks.SetIamPolicy",
        )?;
        // Cloud Tasks reads missing policy as empty one
        let policy = request.policy.unwrap_or_default();
        if policy.version > 3 {
            return Err(Status::invalid_argument(format!(
                "Specified policy version ({}) is not supported. For more information, please refer to https://cloud.google.com/iam/help/allow-policies/versions.",
                policy.version
            )));
        }
        let mut bindings: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for binding in policy.bindings {
            if binding.condition.is_some() {
                return Err(Status::invalid_argument(
                    "Can't set conditional policy on this resource",
                ));
            }
            if ![
                &["roles"][..],
                &["projects", "roles"],
                &["organizations", "roles"],
            ]
            .iter()
            .any(|collections| names::ids(&binding.role, collections).is_some())
            {
                return Err(Status::invalid_argument(
                    r#"The role name must be in the form "roles/{role}", "organizations/{organization_id}/roles/{role}", or "projects/{project_id}/roles/{role}"."#,
                ));
            }
            if let Some(member) = binding.members.iter().find(|member| {
                !matches!(member.as_str(), "allUsers" | "allAuthenticatedUsers")
                    && !MEMBER_PREFIXES
                        .iter()
                        .any(|prefix| member.starts_with(prefix))
            }) {
                return Err(Status::invalid_argument(format!(
                    "The member {member} is of an unknown type. Please set a valid type prefix for the member."
                )));
            }
            if !binding.members.is_empty() {
                bindings
                    .entry(binding.role)
                    .or_default()
                    .extend(binding.members);
            }
        }

        let mut state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.resource)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        if !policy.etag.is_empty() && policy.etag != queue.policy.etag {
            return Err(Status::aborted(format!(
                "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag '{}' did not match the current policy's ETag '{}'.",
                c_escape(&policy.etag),
                c_escape(&queue.policy.etag)
            )));
        }
        queue.policy = iam::Policy {
            version: 1,
            etag: rand::random::<[u8; 8]>().to_vec(),
            bindings: bindings
                .into_iter()
                .map(|(role, members)| iam::Binding {
                    role,
                    members: members.into_iter().collect(),
                    condition: None,
                })
                .collect(),
            audit_configs: Vec::new(),
        };
        Ok(Response::new(queue.policy.clone()))
    }

    async fn test_iam_permissions(
        &self,
        request: Request<iam::TestIamPermissionsRequest>,
    ) -> Result<Response<iam::TestIamPermissionsResponse>, Status> {
        let request = request.into_inner();
        resource(
            &request.resource,
            names::QUEUE,
            "cloudtasks.queues.list",
            "google.cloud.tasks.v2.CloudTasks.TestIamPermissions",
        )?;
        if !self
            .state
            .lock()
            .unwrap()
            .queues
            .contains_key(&request.resource)
        {
            return Err(Status::not_found(QUEUE_NOT_FOUND));
        }
        if request.permissions.is_empty() {
            return Err(Status::invalid_argument(
                "At least one permission must be specified.",
            ));
        }
        for permission in &request.permissions {
            if permission.split('.').count() != 3 || permission.split('.').any(str::is_empty) {
                return Err(Status::invalid_argument(format!(
                    "The permission {permission} is invalid."
                )));
            }
            if !QUEUE_PERMISSIONS.contains(&permission.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "Permission {permission} is not valid for this resource."
                )));
            }
        }
        // Grants every permission, as emulator has no caller identity
        let mut permissions = request.permissions;
        permissions.sort();
        Ok(Response::new(iam::TestIamPermissionsResponse {
            permissions,
        }))
    }

    async fn list_tasks(
        &self,
        request: Request<pb::ListTasksRequest>,
    ) -> Result<Response<pb::ListTasksResponse>, Status> {
        let request = request.into_inner();
        resource(
            &request.parent,
            names::QUEUE,
            "cloudtasks.tasks.list",
            "google.cloud.tasks.v2.CloudTasks.ListTasks",
        )?;
        // Unlike create and get, list echoes unknown view as basic
        let view = match response_view(request.response_view) {
            pb::task::View::Full => pb::task::View::Full,
            _ => pb::task::View::Basic,
        };
        let state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get(&request.parent)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        let prefix = format!("{}/tasks/", request.parent);
        let (tasks, next_page_token) = page(
            &queue.tasks,
            &prefix,
            "ListTasksRequest",
            request.page_size,
            1000,
            &request.page_token,
            |task| task_to_proto(task, view),
        )?;
        Ok(Response::new(pb::ListTasksResponse {
            tasks,
            next_page_token,
        }))
    }

    async fn get_task(
        &self,
        request: Request<pb::GetTaskRequest>,
    ) -> Result<Response<pb::Task>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::TASK,
            "cloudtasks.tasks.get",
            "google.cloud.tasks.v2.CloudTasks.GetTask",
        )?;
        let view = response_view(request.response_view);
        let mut state = self.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(
                request
                    .name
                    .rsplit_once("/tasks/")
                    .expect("checked task name")
                    .0,
            )
            .ok_or_else(|| Status::not_found(NOT_FOUND))?;
        match queue.tasks.get(&request.name) {
            Some(task) => Ok(Response::new(task_to_proto(task, view))),
            None if queue
                .removed_tasks
                .contains(&request.name, SystemTime::now()) =>
            {
                Err(Status::not_found(TASK_GONE))
            }
            None => Err(Status::not_found(NOT_FOUND)),
        }
    }

    async fn create_task(
        &self,
        request: Request<pb::CreateTaskRequest>,
    ) -> Result<Response<pb::Task>, Status> {
        let request = request.into_inner();
        resource(
            &request.parent,
            names::QUEUE,
            "cloudtasks.tasks.create",
            "google.cloud.tasks.v2.CloudTasks.CreateTask",
        )?;
        // Cloud Tasks reads missing task as one without payload
        let task = request.task.unwrap_or_default();
        let name = task_name(&request.parent, &task.name)?;
        let mut state = self.state.lock().unwrap();
        let response = insert_task(
            &mut state,
            &request.parent,
            name,
            task,
            response_view(request.response_view),
        )?;
        drop(state);
        self.wake.notify_one();
        Ok(Response::new(response))
    }

    async fn batch_create_tasks(
        &self,
        request: Request<pb::BatchCreateTasksRequest>,
    ) -> Result<Response<Operation>, Status> {
        let request = request.into_inner();
        resource(
            &request.parent,
            names::QUEUE,
            "cloudtasks.tasks.create",
            "google.cloud.tasks.v2.CloudTasks.BatchCreateTasks",
        )?;
        if request.requests.is_empty() {
            return Err(Status::invalid_argument(
                "Request must contain at least one task.",
            ));
        }
        if request.requests.len() > 100 {
            return Err(Status::invalid_argument(format!(
                "A maximum of 100 tasks can be created in a single batch, but got {} tasks.",
                request.requests.len()
            )));
        }
        let mut creates = Vec::new();
        let mut task_names = HashSet::new();
        for create in request.requests {
            if create.parent != request.parent {
                return Err(Status::invalid_argument(format!(
                    "All tasks in a batch request must target the same queue. Found task for queue {} in batch request for queue {}.",
                    create.parent, request.parent
                )));
            }
            let task = create.task.unwrap_or_default();
            let name = task_name(&request.parent, &task.name)?;
            if !task_names.insert(name.clone()) {
                return Err(Status::invalid_argument(format!(
                    "Duplicate task name in the batch request: {name}"
                )));
            }
            creates.push((name, task, response_view(create.response_view)));
        }

        let start_time = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        let name = operation_name(&state, &request.parent, request.request_id)?;
        let mut tasks = Vec::new();
        let mut failed_requests = BTreeMap::new();
        for (index, (task_name, task, view)) in creates.into_iter().enumerate() {
            // Batch words missing payload differently from CreateTask
            let created = match task.message_type {
                None => Err(Status::invalid_argument("Payload type must be set.")),
                Some(_) => insert_task(&mut state, &request.parent, task_name, task, view),
            };
            match created {
                Ok(task) => tasks.push(task),
                Err(status) => {
                    failed_requests.insert(
                        index as i32,
                        rpc::Status {
                            code: status.code() as i32,
                            message: status.message().to_owned(),
                            details: Vec::new(),
                        },
                    );
                }
            }
        }
        let batch_state = match (tasks.is_empty(), failed_requests.is_empty()) {
            (_, true) => pb::batch_create_tasks_metadata::State::Succeeded,
            (true, false) => pb::batch_create_tasks_metadata::State::Failed,
            (false, false) => pb::batch_create_tasks_metadata::State::PartiallySucceeded,
        };
        let metadata = pb::BatchCreateTasksMetadata {
            start_time: Some(start_time.into()),
            end_time: Some(SystemTime::now().into()),
            state: batch_state.into(),
            failed_requests,
        };
        let operation = Operation {
            name: name.clone(),
            metadata: Some(Any::from_msg(&metadata).expect("Vec grows to fit")),
            done: true,
            // Batch that created nothing has no response
            result: (!tasks.is_empty()).then(|| {
                operation::Result::Response(
                    Any::from_msg(&pb::BatchCreateTasksResponse { tasks })
                        .expect("Vec grows to fit"),
                )
            }),
        };
        state.operations.insert(name, operation.clone());
        drop(state);
        self.wake.notify_one();
        Ok(Response::new(operation))
    }

    async fn delete_task(
        &self,
        request: Request<pb::DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::TASK,
            "cloudtasks.tasks.delete",
            "google.cloud.tasks.v2.CloudTasks.DeleteTask",
        )?;
        let mut state = self.state.lock().unwrap();
        state
            .queues
            .get_mut(
                request
                    .name
                    .rsplit_once("/tasks/")
                    .expect("checked task name")
                    .0,
            )
            .and_then(|queue| queue.remove_task(&request.name, SystemTime::now()))
            .ok_or_else(|| Status::not_found(NOT_FOUND))?;
        Ok(Response::new(()))
    }

    async fn batch_delete_tasks(
        &self,
        request: Request<pb::BatchDeleteTasksRequest>,
    ) -> Result<Response<Operation>, Status> {
        let request = request.into_inner();
        resource(
            &request.parent,
            names::QUEUE,
            "cloudtasks.tasks.delete",
            "google.cloud.tasks.v2.CloudTasks.BatchDeleteTasks",
        )?;
        if request.names.is_empty() {
            return Err(Status::invalid_argument(
                "Request contains an invalid argument.",
            ));
        }
        if request.names.len() > 1000 {
            return Err(Status::invalid_argument(format!(
                "A maximum of 1000 tasks can be handled in a batch, but {} were requested.",
                request.names.len()
            )));
        }

        let start_time = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        let name = operation_name(&state, &request.parent, request.request_id)?;
        let mut deleted = HashSet::new();
        let mut failed_requests = BTreeMap::new();
        for (index, task) in request.names.iter().enumerate() {
            // Name deleted earlier in batch counts as deleted again
            if deleted.contains(task) {
                continue;
            }
            let result = check_task_name(task)
                .map_err(|status| {
                    Status::invalid_argument(format!("Invalid task name: {}", status.message()))
                })
                .and_then(|()| check_task_queue(&request.parent, task))
                .and_then(|()| {
                    state
                        .queues
                        .get_mut(&request.parent)
                        .and_then(|queue| queue.remove_task(task, SystemTime::now()))
                        .ok_or_else(|| Status::not_found(NOT_FOUND))
                });
            match result {
                Ok(_) => {
                    deleted.insert(task);
                }
                Err(status) => {
                    failed_requests.insert(
                        index as i32,
                        rpc::Status {
                            code: status.code() as i32,
                            message: status.message().to_owned(),
                            details: Vec::new(),
                        },
                    );
                }
            }
        }
        let result = if failed_requests.len() == request.names.len() {
            operation::Result::Error(rpc::Status {
                code: rpc::Code::Aborted as i32,
                message: "None of the requests succeeded, refer to BatchDeleteTasksMetadata.failed_requests for individual error details".to_owned(),
                details: Vec::new(),
            })
        } else {
            operation::Result::Response(Any::from_msg(&()).expect("Vec grows to fit"))
        };
        // Cloud Tasks reports partial success even when every deletion fails
        let batch_state = if failed_requests.is_empty() {
            pb::batch_delete_tasks_metadata::State::Succeeded
        } else {
            pb::batch_delete_tasks_metadata::State::PartiallySucceeded
        };
        let metadata = pb::BatchDeleteTasksMetadata {
            start_time: Some(start_time.into()),
            end_time: Some(SystemTime::now().into()),
            state: batch_state.into(),
            failed_requests,
        };
        let operation = Operation {
            name: name.clone(),
            metadata: Some(Any::from_msg(&metadata).expect("Vec grows to fit")),
            done: true,
            result: Some(result),
        };
        state.operations.insert(name, operation.clone());
        Ok(Response::new(operation))
    }

    async fn run_task(
        &self,
        request: Request<pb::RunTaskRequest>,
    ) -> Result<Response<pb::Task>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::TASK,
            "cloudtasks.tasks.run",
            "google.cloud.tasks.v2.CloudTasks.RunTask",
        )?;
        let view = response_view(request.response_view);
        let mut state = self.state.lock().unwrap();
        let task = state
            .queues
            .get_mut(
                request
                    .name
                    .rsplit_once("/tasks/")
                    .expect("checked task name")
                    .0,
            )
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?
            .tasks
            .get_mut(&request.name)
            .ok_or_else(|| Status::not_found(NOT_FOUND))?;
        task.run_requested = true;
        // Cloud Tasks answers before it dispatches, so new attempt has only its schedule time
        let mut response = task_to_proto(task, view);
        response.dispatch_count += 1;
        response.last_attempt = Some(pb::Attempt {
            schedule_time: Some(timestamp(task.schedule_time)),
            ..Default::default()
        });
        drop(state);
        self.wake.notify_one();
        Ok(Response::new(response))
    }

    async fn update_cmek_config(
        &self,
        request: Request<pb::UpdateCmekConfigRequest>,
    ) -> Result<Response<pb::CmekConfig>, Status> {
        // Cloud Tasks reads missing config as one with empty name
        let config = request.into_inner().cmek_config.unwrap_or_default();
        let location = cmek_location(
            &config.name,
            "google.cloud.tasks.v2.CloudTasks.UpdateCmekConfig",
        )?;
        let mut state = self.state.lock().unwrap();
        if config.kms_key.is_empty() {
            state.cmek_keys.remove(location);
            return Ok(Response::new(config));
        }
        // Header overrides block CMEK, though docs name URI override host
        let prefix = format!("{location}/queues/");
        if state.queues.values().any(|queue| {
            queue.name.starts_with(&prefix)
                && queue
                    .config
                    .http_target
                    .as_ref()
                    .is_some_and(|target| !target.header_overrides.is_empty())
        }) {
            return Err(detailed(
                Code::FailedPrecondition,
                "CMEK cannot be enabled on any project that has one or more queues with HTTP Target Override headers or tasks ttl of more than 60 days.",
                vec![error_info(
                    "UNSATISFIED_PRECONDITION_FOR_CMEK",
                    "cloudtasks.googleapis.com",
                    &[],
                )],
            ));
        }
        let key_ring = &["projects", "locations", "keyRings"];
        let crypto_key = &["projects", "locations", "keyRings", "cryptoKeys"];
        let key_version = &[
            "projects",
            "locations",
            "keyRings",
            "cryptoKeys",
            "cryptoKeyVersions",
        ];
        // Checks only key's form and location, as emulator cannot see Cloud KMS
        match names::ids(&config.kms_key, crypto_key) {
            Some(ids) if ids[1] != names::last_segment(location) => {
                return Err(Status::not_found(NOT_FOUND));
            }
            Some(_) => {}
            None if names::ids(&config.kms_key, key_ring).is_some()
                || names::ids(&config.kms_key, key_version).is_some() =>
            {
                return Err(Status::invalid_argument(
                    "Request contains an invalid argument.",
                ));
            }
            None => {
                return Err(Status::invalid_argument(format!(
                    "Resource name [{}] does not match any known resource name pattern.",
                    config.kms_key
                )));
            }
        }
        state
            .cmek_keys
            .insert(location.to_owned(), config.kms_key.clone());
        Ok(Response::new(config))
    }

    async fn get_cmek_config(
        &self,
        request: Request<pb::GetCmekConfigRequest>,
    ) -> Result<Response<pb::CmekConfig>, Status> {
        let request = request.into_inner();
        let location = cmek_location(
            &request.name,
            "google.cloud.tasks.v2.CloudTasks.GetCmekConfig",
        )?;
        let state = self.state.lock().unwrap();
        let kms_key = state.cmek_keys.get(location).cloned().unwrap_or_default();
        Ok(Response::new(pb::CmekConfig {
            name: request.name,
            kms_key,
        }))
    }
}

#[tonic::async_trait]
impl Operations for Emulator {
    async fn get_operation(
        &self,
        request: Request<longrunning::GetOperationRequest>,
    ) -> Result<Response<Operation>, Status> {
        let request = request.into_inner();
        resource(
            &request.name,
            names::OPERATION,
            "cloudtasks.operations.get",
            "google.longrunning.Operations.GetOperation",
        )?;
        let state = self.state.lock().unwrap();
        let operation = state
            .operations
            .get(&request.name)
            .ok_or_else(|| Status::not_found(NOT_FOUND))?;
        Ok(Response::new(operation.clone()))
    }

    async fn list_operations(
        &self,
        _: Request<longrunning::ListOperationsRequest>,
    ) -> Result<Response<longrunning::ListOperationsResponse>, Status> {
        Err(Status::permission_denied(UNSERVED))
    }

    async fn delete_operation(
        &self,
        _: Request<longrunning::DeleteOperationRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::permission_denied(UNSERVED))
    }

    async fn cancel_operation(
        &self,
        _: Request<longrunning::CancelOperationRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::permission_denied(UNSERVED))
    }

    async fn wait_operation(
        &self,
        _: Request<longrunning::WaitOperationRequest>,
    ) -> Result<Response<Operation>, Status> {
        Err(Status::permission_denied(UNSERVED))
    }
}

/// Rejects malformed name as Google's front end does, then checks its IDs as Cloud Tasks does
fn resource(
    name: &str,
    collections: &[&str],
    permission: &str,
    method: &str,
) -> Result<(), Status> {
    check_project(name, method)?;
    let ids = names::ids(name, collections).ok_or_else(|| {
        Status::permission_denied(format!(
            r#"The principal (user or service account) lacks IAM permission "{permission}" for the resource "{name}" (or the resource may not exist)."#
        ))
    })?;
    check_ids(&ids, collections)
}

/// Rejects empty name or one outside project, as Google's front end does for `method`
fn check_project(name: &str, method: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(detailed(
            Code::InvalidArgument,
            "Invalid resource field value in the request.",
            vec![error_info(
                "RESOURCE_PROJECT_INVALID",
                "googleapis.com",
                &[("method", method), ("service", "cloudtasks.googleapis.com")],
            )],
        ));
    }
    if !name.starts_with("projects/") {
        let message = format!("Permission denied on resource project {name}.");
        let localized = rpc::LocalizedMessage {
            locale: "en-US".to_owned(),
            message: message.clone(),
        };
        return Err(detailed(
            Code::PermissionDenied,
            &message,
            vec![
                error_info(
                    "CONSUMER_INVALID",
                    "googleapis.com",
                    &[
                        ("consumer", &format!("projects/{name}")),
                        ("containerInfo", name),
                        ("service", "cloudtasks.googleapis.com"),
                    ],
                ),
                Any::from_msg(&localized).expect("Vec grows to fit"),
                help(
                    "Google developers console",
                    "https://console.developers.google.com",
                ),
            ],
        ));
    }
    Ok(())
}

/// Status with `details`, which clients read from `grpc-status-details-bin` trailer
fn detailed(code: Code, message: &str, details: Vec<Any>) -> Status {
    let status = rpc::Status {
        code: code as i32,
        message: message.to_owned(),
        details,
    };
    Status::with_details(code, message, status.encode_to_vec().into())
}

fn error_info(reason: &str, domain: &str, metadata: &[(&str, &str)]) -> Any {
    let info = rpc::ErrorInfo {
        reason: reason.to_owned(),
        domain: domain.to_owned(),
        metadata: metadata
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    };
    Any::from_msg(&info).expect("Vec grows to fit")
}

fn help(description: &str, url: &str) -> Any {
    let help = rpc::Help {
        links: vec![rpc::help::Link {
            description: description.to_owned(),
            url: url.to_owned(),
        }],
    };
    Any::from_msg(&help).expect("Vec grows to fit")
}

fn cmek_location<'a>(name: &'a str, method: &str) -> Result<&'a str, Status> {
    check_project(name, method)?;
    name.strip_suffix("/cmekConfig")
        .filter(|location| names::ids(location, names::LOCATION).is_some())
        .ok_or_else(|| {
            Status::invalid_argument(
                r#"CMEK Config name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/cmekConfig"."#,
            )
        })
}

fn task_name(parent: &str, name: &str) -> Result<String, Status> {
    if name.is_empty() {
        return Ok(format!("{parent}/tasks/{}", rand::random::<u64>()));
    }
    check_task_name(name)?;
    check_task_queue(parent, name)?;
    Ok(name.to_owned())
}

fn check_task_name(name: &str) -> Result<(), Status> {
    let ids = names::ids(name, names::TASK).ok_or_else(|| {
        detailed(
            Code::InvalidArgument,
            r#"Task name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>/tasks/<TASK_ID>"."#,
            vec![help("Definition of task name", TASK_NAME_DOCS)],
        )
    })?;
    check_ids(&ids, names::TASK)
}

fn check_task_queue(parent: &str, name: &str) -> Result<(), Status> {
    let (queue, _) = name.rsplit_once("/tasks/").expect("checked task name");
    if queue != parent {
        return Err(Status::invalid_argument(format!(
            "The queue name from request ('{parent}') must be the same as the queue name in the named task ('{queue}')."
        )));
    }
    Ok(())
}

fn operation_name(state: &State, parent: &str, request_id: String) -> Result<String, Status> {
    let id = if request_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_id
    };
    let (location, _) = parent.rsplit_once("/queues/").expect("checked queue name");
    let name = format!("{location}/operations/{id}");
    if state.operations.contains_key(&name) {
        return Err(Status::invalid_argument(format!(
            "Duplicate operation id in the batch request: {id}"
        )));
    }
    Ok(name)
}

fn check_ids(ids: &[&str], collections: &[&str]) -> Result<(), Status> {
    for (collection, id) in collections.iter().zip(ids) {
        let status = match *collection {
            "queues" if !names::is_id(id, 100, &['-']) => detailed(
                Code::InvalidArgument,
                &format!(
                    "Queue ID \"{id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Queue ID must be between 1 and 100 characters."
                ),
                vec![help("Definition of queue ID", QUEUE_NAME_DOCS)],
            ),
            "tasks" if !names::is_id(id, 500, &['-', '_']) => detailed(
                Code::InvalidArgument,
                &format!(
                    "Task ID \"{id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Task ID must between 1 and 500 characters."
                ),
                vec![help("Definition of task ID", TASK_NAME_DOCS)],
            ),
            "operations" if !names::is_id(id, 100, &['-', '_']) => {
                Status::invalid_argument(format!(
                    "Operation ID \"{id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Operation ID must be between 1 and 100 characters."
                ))
            }
            _ => continue,
        };
        return Err(status);
    }
    Ok(())
}

/// Copies fields named in `paths` from `update` onto `current`, leaving output-only ones unread
fn masked(current: pb::Queue, update: pb::Queue, paths: &[String]) -> Result<pb::Queue, Status> {
    let queue = crate::DESCRIPTORS
        .get_message_by_name("google.cloud.tasks.v2.Queue")
        .expect("descriptors include the queue");
    let mut merged = DynamicMessage::new(queue.clone());
    merged.transcode_from(&current).expect("queues transcode");
    let mut update_message = DynamicMessage::new(queue);
    update_message
        .transcode_from(&update)
        .expect("queues transcode");
    for path in paths {
        merge(&mut merged, &update_message, path)
            .ok_or_else(|| Status::invalid_argument(format!("Invalid mask: {path}")))?;
    }
    Ok(merged.transcode_to().expect("queues transcode"))
}

/// None when `path` names no field, and merges below message only when either side has it
fn merge(current: &mut DynamicMessage, update: &DynamicMessage, path: &str) -> Option<()> {
    let (name, rest) = path
        .split_once('.')
        .map_or((path, None), |(name, rest)| (name, Some(rest)));
    let field = current.descriptor().get_field_by_name(name)?;
    let Some(rest) = rest else {
        if update.has_field(&field) {
            current.set_field(&field, update.get_field(&field).into_owned());
        } else {
            current.clear_field(&field);
        }
        return Some(());
    };
    // Lists and well-known types are leaves, like scalars
    let Kind::Message(message) = field.kind() else {
        return None;
    };
    if field.is_list() || message.full_name().starts_with("google.protobuf.") {
        return None;
    }
    let below = update.get_field(&field);
    let below = below.as_message().expect("message fields hold messages");
    if current.has_field(&field) || update.has_field(&field) {
        let current = current
            .get_field_mut(&field)
            .as_message_mut()
            .expect("message fields hold messages");
        merge(current, below, rest)
    } else {
        // Checks path without setting message
        merge(&mut DynamicMessage::new(message), below, rest)
    }
}

/// Formats time as Cloud Tasks reports it, in Pacific time with shortest fraction
fn pacific_time(time: Timestamp) -> String {
    // jiff's timestamps end one day before 10000, by when offset has stopped changing
    let offset = jiff::tz::get!("America/Los_Angeles")
        .to_offset(jiff::Timestamp::new(time.seconds, time.nanos).unwrap_or(jiff::Timestamp::MAX));
    let local = jiff::civil::date(1970, 1, 1)
        .at(0, 0, 0, 0)
        .checked_add(jiff::SignedDuration::new(
            time.seconds + i64::from(offset.seconds()),
            time.nanos,
        ))
        .expect("checked schedule times end within 9999");
    let mut local = BrokenDownTime::from(local);
    local.set_offset(Some(offset));
    local
        .to_string("%Y-%m-%dT%H:%M:%S%.f%:z")
        .expect("format suits a datetime with an offset")
}

/// Pages through names under `prefix`, using last name returned as opaque page token
fn page<T, U>(
    items: &BTreeMap<String, T>,
    prefix: &str,
    request_type: &str,
    page_size: i32,
    max_page_size: usize,
    page_token: &str,
    to_proto: impl Fn(&T) -> U,
) -> Result<(Vec<U>, String), Status> {
    let page_size = match usize::try_from(page_size) {
        Ok(0) => max_page_size,
        Ok(page_size) => page_size.min(max_page_size),
        Err(_) => {
            return Err(Status::invalid_argument(format!(
                "{request_type}.pageSize cannot be negative."
            )));
        }
    };
    let after = match page_token {
        "" => None,
        token => Some(
            URL_SAFE_NO_PAD
                .decode(token)
                .ok()
                .and_then(|name| String::from_utf8(name).ok())
                .filter(|name| name.starts_with(prefix))
                .ok_or_else(|| Status::invalid_argument("Invalid page_token"))?,
        ),
    };
    let start = match &after {
        None => Bound::Included(prefix),
        Some(name) => Bound::Excluded(name.as_str()),
    };
    let items: Vec<(&String, &T)> = items
        .range::<str, _>((start, Bound::Unbounded))
        .take_while(|(name, _)| name.starts_with(prefix))
        .take(page_size)
        .collect();
    // Cloud Tasks returns token after every non-empty page, so only empty page ends list
    let next_page_token = items
        .last()
        .map_or_else(String::new, |(name, _)| URL_SAFE_NO_PAD.encode(name));
    Ok((
        items.into_iter().map(|(_, item)| to_proto(item)).collect(),
        next_page_token,
    ))
}

fn queue_config(queue: pb::Queue) -> Result<QueueConfig, Status> {
    let rate_limits = rate_limits(queue.rate_limits.unwrap_or_default())?;
    let retry_config = retry_config(queue.retry_config.unwrap_or_default())?;
    let http_target = queue.http_target.map(http_target).transpose()?;
    Ok(QueueConfig {
        rate_limits,
        retry_config,
        http_target,
        log_sampling_ratio: log_sampling_ratio(queue.stackdriver_logging_config)?,
    })
}

fn rate_limits(limits: pb::RateLimits) -> Result<RateLimits, Status> {
    let rate = limits.max_dispatches_per_second;
    let max_dispatches_per_second = if rate > 500.0 {
        return Err(Status::invalid_argument(
            "RateLimits.maxDispatchesPerSecond must be less than or equal to 500.",
        ));
    } else if rate == 0.0 {
        500.0
    } else if rate > 0.0 {
        rate
    } else {
        return Err(Status::invalid_argument(
            "RateLimits.maxDispatchesPerSecond cannot be negative.",
        ));
    };
    let max_concurrent_dispatches = match limits.max_concurrent_dispatches {
        0 => 1000,
        concurrency if concurrency > 5000 => {
            return Err(Status::invalid_argument(
                "RateLimits.maxConcurrentDispatches must be less than or equal to 5000.",
            ));
        }
        concurrency => u32::try_from(concurrency).map_err(|_| {
            Status::invalid_argument("RateLimits.maxConcurrentDispatches cannot be negative.")
        })?,
    };
    // Output only, so caller's value is ignored
    Ok(RateLimits {
        max_dispatches_per_second,
        max_burst_size: ((max_dispatches_per_second / 5.0) as u32).max(10),
        max_concurrent_dispatches,
    })
}

fn retry_config(config: pb::RetryConfig) -> Result<RetryConfig, Status> {
    let max_attempts = match config.max_attempts {
        0 => Some(100),
        -1 => None,
        attempts => Some(u32::try_from(attempts).map_err(|_| {
            Status::invalid_argument("RetryConfig.maxAttempts must be greater or equal to -1.")
        })?),
    };
    let max_doublings = match config.max_doublings {
        0 => 16,
        doublings => u32::try_from(doublings).map_err(|_| {
            Status::invalid_argument("RetryConfig.maxDoublings cannot be negative.")
        })?,
    };
    let max_retry_duration = match config.max_retry_duration {
        None => None,
        Some(max) if max.nanos != 0 => {
            return Err(Status::invalid_argument(
                "RetryConfig.max_retry_duration.nanos cannot be set.",
            ));
        }
        Some(max) => Some(Duration::from_secs(u64::try_from(max.seconds).map_err(
            |_| {
                Status::invalid_argument(
                    "RetryConfig.max_retry_duration.seconds cannot be negative.",
                )
            },
        )?))
        .filter(|max| !max.is_zero()),
    };
    // Zero counts as unset
    let min_backoff = config
        .min_backoff
        .map(Duration::try_from)
        .transpose()
        .map_err(|_| Status::invalid_argument("RetryConfig.minBackoff cannot be negative."))?
        .filter(|min| !min.is_zero())
        .unwrap_or(Duration::from_millis(100));
    let max_backoff = config
        .max_backoff
        .map(Duration::try_from)
        .transpose()
        .map_err(|_| Status::invalid_argument("RetryConfig.maxBackoff cannot be negative."))?
        .filter(|max| !max.is_zero())
        .unwrap_or(Duration::from_secs(3600));
    if min_backoff > max_backoff {
        return Err(Status::invalid_argument(
            "RetryConfig.minBackoff must be less than or equal to RetryConfig.maxBackoff.",
        ));
    }
    Ok(RetryConfig {
        max_attempts,
        max_retry_duration,
        min_backoff,
        max_backoff,
        max_doublings,
    })
}

fn http_request(request: pb::HttpRequest) -> Result<HttpRequest, Status> {
    if request.url.is_empty() {
        return Err(Status::invalid_argument("HttpRequest.url is required."));
    }
    if !request.url.starts_with("http://") && !request.url.starts_with("https://") {
        return Err(Status::invalid_argument(
            "HttpTarget.url must start with 'http://' or 'https://'.",
        ));
    }
    let authorization_header = request
        .authorization_header
        .map(|header| {
            let header = match header {
                pb::http_request::AuthorizationHeader::OauthToken(token) => oauth_token(token),
                pb::http_request::AuthorizationHeader::OidcToken(token) => oidc_token(token),
            };
            service_account(&header)?;
            Ok::<_, Status>(header)
        })
        .transpose()?;
    // Cloud Tasks stores unknown method as POST
    let http_method = pb::HttpMethod::try_from(request.http_method)
        .ok()
        .and_then(http_method)
        .unwrap_or(HttpMethod::Post);
    if !request.body.is_empty() && !http_method.allows_body() {
        return Err(Status::invalid_argument(
            "Body can only be set if the http_method is POST, PUT, or PATCH.",
        ));
    }
    let mut headers = stored_headers(request.headers);
    headers.insert("User-Agent".to_owned(), "Google-Cloud-Tasks".to_owned());
    Ok(HttpRequest {
        url: request.url,
        http_method,
        headers,
        body: request.body,
        authorization_header,
    })
}

/// For project in queue or task name, as Cloud Tasks answers when project has no app
fn no_app_engine(name: &str) -> Status {
    let project = names::project(name);
    Status::not_found(format!(
        "The project {project} does not contain an App Engine application. Please visit https://console.developers.google.com/appengine?project={project} to add an App Engine application."
    ))
}

fn http_method(method: pb::HttpMethod) -> Option<HttpMethod> {
    Some(match method {
        pb::HttpMethod::Unspecified => return None,
        pb::HttpMethod::Post => HttpMethod::Post,
        pb::HttpMethod::Get => HttpMethod::Get,
        pb::HttpMethod::Head => HttpMethod::Head,
        pb::HttpMethod::Put => HttpMethod::Put,
        pb::HttpMethod::Delete => HttpMethod::Delete,
        pb::HttpMethod::Patch => HttpMethod::Patch,
        pb::HttpMethod::Options => HttpMethod::Options,
    })
}

fn oauth_token(token: pb::OAuthToken) -> AuthorizationHeader {
    AuthorizationHeader::OAuthToken {
        service_account_email: token.service_account_email,
        scope: Some(token.scope).filter(|scope| !scope.is_empty()),
    }
}

fn oidc_token(token: pb::OidcToken) -> AuthorizationHeader {
    AuthorizationHeader::OidcToken {
        service_account_email: token.service_account_email,
        audience: Some(token.audience).filter(|audience| !audience.is_empty()),
    }
}

fn service_account(header: &AuthorizationHeader) -> Result<(), Status> {
    let (AuthorizationHeader::OAuthToken {
        service_account_email,
        ..
    }
    | AuthorizationHeader::OidcToken {
        service_account_email,
        ..
    }) = header;
    if service_account_email.is_empty() {
        return Err(Status::invalid_argument(
            "service_account_email must be set.",
        ));
    }
    if !service_account_email
        .split_once('@')
        .is_some_and(|(name, domain)| {
            !name.is_empty() && !domain.is_empty() && !domain.contains('@')
        })
    {
        return Err(Status::invalid_argument(
            "Request contains an invalid argument.",
        ));
    }
    Ok(())
}

fn http_target(target: pb::HttpTarget) -> Result<HttpTarget, Status> {
    let parse_error = |detail: &str| {
        detailed(
            Code::InvalidArgument,
            &format!("Error in parsing HttpTarget. {detail}"),
            vec![error_info(
                "INVALID_INPUT_ON_HTTP_TARGET_OVERRIDE",
                "cloudtasks.googleapis.com",
                &[("invalid_field", detail)],
            )],
        )
    };
    let uri_override = target
        .uri_override
        .map(|uri| {
            if uri.scheme.is_none()
                && uri.host.is_none()
                && uri.port.is_none()
                && uri.path_override.is_none()
                && uri.query_override.is_none()
            {
                return Err(parse_error("Invalid uri_override."));
            }
            let scheme = uri
                .scheme
                .map(pb::uri_override::Scheme::try_from)
                .transpose()
                .map_err(|_| parse_error(""))?;
            if let Some(host) = &uri.host
                // Only bare host, which parses back to itself
                && !url::Host::parse(host)
                    .is_ok_and(|parsed| parsed.to_string().eq_ignore_ascii_case(host))
            {
                return Err(parse_error("Invalid Host."));
            }
            let port = uri
                .port
                .map(u64::try_from)
                .transpose()
                .map_err(|_| parse_error("Port number should be a positive integer."))?;
            let enforce_mode =
                pb::uri_override::UriOverrideEnforceMode::try_from(uri.uri_override_enforce_mode)
                    .map_err(|_| parse_error(""))?;
            Ok(UriOverride {
                scheme,
                host: uri.host,
                port,
                path: uri.path_override.map(|path| path.path),
                query: uri.query_override.map(|query| query.query_params),
                enforce_mode,
            })
        })
        .transpose()?;
    let http_method = match pb::HttpMethod::try_from(target.http_method) {
        Ok(method) => http_method(method),
        Err(_) => return Err(parse_error("")),
    };
    let header_overrides: Vec<(String, String)> = target
        .header_overrides
        .into_iter()
        .filter_map(|header_override| header_override.header)
        .map(|header| (header.key, header.value))
        .collect();
    let invalid: Vec<&str> = header_overrides
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| {
            let lowercase = name.to_ascii_lowercase();
            name.is_empty()
                || !name.chars().all(|c| c.is_ascii_graphic() && c != ':')
                || RESERVED_HEADERS.contains(&lowercase.as_str())
                || RESERVED_PREFIXES
                    .iter()
                    .chain(&["x-cloudtasks-"])
                    .any(|prefix| lowercase.starts_with(prefix))
        })
        .collect();
    if !invalid.is_empty() {
        return Err(Status::invalid_argument(format!(
            "Error in parsing HttpTarget.headers. Header name cannot be empty or start with reserved prefixes. Invalid headers found: {}",
            invalid.join(", ")
        )));
    }
    Ok(HttpTarget {
        uri_override,
        http_method,
        header_overrides,
        authorization_header: target.authorization_header.map(|header| match header {
            pb::http_target::AuthorizationHeader::OauthToken(token) => oauth_token(token),
            pb::http_target::AuthorizationHeader::OidcToken(token) => oidc_token(token),
        }),
    })
}

/// Zero logs nothing, and is how Cloud Tasks reads missing config
fn log_sampling_ratio(config: Option<pb::StackdriverLoggingConfig>) -> Result<f64, Status> {
    let ratio = config.map_or(0.0, |config| config.sampling_ratio);
    if !(0.0..=1.0).contains(&ratio) {
        return Err(Status::invalid_argument(
            "sampling_ratio must be between 0.0 and 1.0.",
        ));
    }
    Ok(ratio)
}

/// Drops and rewrites headers as Cloud Tasks stores them
fn stored_headers(headers: BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .into_iter()
        .filter_map(|(name, value)| {
            let lowercase = name.to_ascii_lowercase();
            // Cloud Tasks ends value at NUL and replaces line breaks
            let value = value
                .split('\0')
                .next()
                .expect("split yields at least one part")
                .replace(['\r', '\n'], "_");
            let kept = !RESERVED_HEADERS.contains(&lowercase.as_str())
                && !RESERVED_PREFIXES
                    .iter()
                    .any(|prefix| lowercase.starts_with(prefix))
                && !name.is_empty()
                && name.chars().all(|c| c.is_ascii_graphic() && c != ':')
                && !value.is_empty()
                && value.chars().all(|c| c == '\t' || c == ' ' || c.is_ascii_graphic())
                // Keeps `name: value\r\n` within 8 KiB
                && name.len() + value.len() <= 8188;
            kept.then_some((name, value))
        })
        .collect()
}

/// Unknown view renders as basic but echoes as unspecified
fn response_view(view: i32) -> pb::task::View {
    match pb::task::View::try_from(view) {
        Ok(pb::task::View::Unspecified | pb::task::View::Basic) => pb::task::View::Basic,
        Ok(pb::task::View::Full) => pb::task::View::Full,
        Err(_) => pb::task::View::Unspecified,
    }
}

/// Truncated to microseconds, as Cloud Tasks stores times
fn timestamp(time: SystemTime) -> Timestamp {
    let mut timestamp = Timestamp::from(time);
    timestamp.nanos -= timestamp.nanos % 1000;
    timestamp
}

/// Prints bytes as Google's error messages do, in octal outside printable ASCII
fn c_escape(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&byte| match byte {
            b' '..=b'~' => (byte as char).to_string(),
            _ => format!("\\{byte:03o}"),
        })
        .collect()
}

fn queue_to_proto(queue: &Queue) -> pb::Queue {
    let config = &queue.config;
    pb::Queue {
        name: queue.name.clone(),
        rate_limits: Some(pb::RateLimits {
            max_dispatches_per_second: config.rate_limits.max_dispatches_per_second,
            max_burst_size: config.rate_limits.max_burst_size as i32,
            max_concurrent_dispatches: config.rate_limits.max_concurrent_dispatches as i32,
        }),
        retry_config: Some(retry_config_to_proto(&config.retry_config)),
        state: match queue.state {
            QueueState::Running => pb::queue::State::Running,
            QueueState::Paused => pb::queue::State::Paused,
        }
        .into(),
        purge_time: queue.purge_time.map(timestamp),
        http_target: config.http_target.as_ref().map(|target| pb::HttpTarget {
            uri_override: target.uri_override.as_ref().map(|uri| pb::UriOverride {
                scheme: uri.scheme.map(Into::into),
                host: uri.host.clone(),
                port: uri.port.map(|port| port as i64),
                path_override: uri.path.clone().map(|path| pb::PathOverride { path }),
                query_override: uri
                    .query
                    .clone()
                    .map(|query_params| pb::QueryOverride { query_params }),
                uri_override_enforce_mode: uri.enforce_mode.into(),
            }),
            http_method: target
                .http_method
                .map_or(pb::HttpMethod::Unspecified, http_method_to_proto)
                .into(),
            header_overrides: target
                .header_overrides
                .iter()
                .map(|(key, value)| pb::http_target::HeaderOverride {
                    header: Some(pb::http_target::Header {
                        key: key.clone(),
                        value: value.clone(),
                    }),
                })
                .collect(),
            authorization_header: target
                .authorization_header
                .clone()
                .map(|header| match header {
                    AuthorizationHeader::OAuthToken {
                        service_account_email,
                        scope,
                    } => pb::http_target::AuthorizationHeader::OauthToken(pb::OAuthToken {
                        service_account_email,
                        scope: scope.unwrap_or_default(),
                    }),
                    AuthorizationHeader::OidcToken {
                        service_account_email,
                        audience,
                    } => pb::http_target::AuthorizationHeader::OidcToken(pb::OidcToken {
                        service_account_email,
                        audience: audience.unwrap_or_default(),
                    }),
                }),
        }),
        app_engine_routing_override: None,
        stackdriver_logging_config: (config.log_sampling_ratio != 0.0).then_some(
            pb::StackdriverLoggingConfig {
                sampling_ratio: config.log_sampling_ratio,
            },
        ),
    }
}

fn http_method_to_proto(method: HttpMethod) -> pb::HttpMethod {
    match method {
        HttpMethod::Post => pb::HttpMethod::Post,
        HttpMethod::Get => pb::HttpMethod::Get,
        HttpMethod::Head => pb::HttpMethod::Head,
        HttpMethod::Put => pb::HttpMethod::Put,
        HttpMethod::Delete => pb::HttpMethod::Delete,
        HttpMethod::Patch => pb::HttpMethod::Patch,
        HttpMethod::Options => pb::HttpMethod::Options,
    }
}

fn retry_config_to_proto(retry_config: &RetryConfig) -> pb::RetryConfig {
    pb::RetryConfig {
        max_attempts: retry_config.max_attempts.map_or(-1, |max| max as i32),
        max_retry_duration: retry_config
            .max_retry_duration
            .map(|max| max.try_into().expect("durations fit")),
        min_backoff: Some(retry_config.min_backoff.try_into().expect("durations fit")),
        max_backoff: Some(retry_config.max_backoff.try_into().expect("durations fit")),
        max_doublings: retry_config.max_doublings as i32,
    }
}

fn task_to_proto(task: &Task, view: pb::task::View) -> pb::Task {
    let request = &task.http_request;
    pb::Task {
        name: task.name.clone(),
        message_type: Some(pb::task::MessageType::HttpRequest(pb::HttpRequest {
            url: request.url.clone(),
            http_method: http_method_to_proto(request.http_method).into(),
            headers: request.headers.clone(),
            body: if view == pb::task::View::Full {
                request.body.clone()
            } else {
                Vec::new()
            },
            authorization_header: request
                .authorization_header
                .clone()
                .map(|header| match header {
                    AuthorizationHeader::OAuthToken {
                        service_account_email,
                        scope,
                    } => pb::http_request::AuthorizationHeader::OauthToken(pb::OAuthToken {
                        service_account_email,
                        scope: scope.unwrap_or_else(|| DEFAULT_SCOPE.to_owned()),
                    }),
                    AuthorizationHeader::OidcToken {
                        service_account_email,
                        audience,
                    } => pb::http_request::AuthorizationHeader::OidcToken(pb::OidcToken {
                        service_account_email,
                        audience: audience.unwrap_or_else(|| request.url.clone()),
                    }),
                }),
        })),
        schedule_time: Some(timestamp(task.schedule_time)),
        create_time: Some(Timestamp {
            nanos: 0,
            ..Timestamp::from(task.create_time)
        }),
        dispatch_deadline: Some(task.dispatch_deadline.try_into().expect("durations fit")),
        dispatch_count: task.dispatch_count as i32,
        response_count: task.response_count as i32,
        first_attempt: task.first_attempt.map(|dispatch_time| pb::Attempt {
            dispatch_time: Some(timestamp(dispatch_time)),
            ..Default::default()
        }),
        last_attempt: task.last_attempt.as_ref().map(attempt_to_proto),
        view: view.into(),
        retry_config: task.retry_config.as_ref().map(retry_config_to_proto),
    }
}

fn attempt_to_proto(attempt: &Attempt) -> pb::Attempt {
    pb::Attempt {
        schedule_time: Some(timestamp(attempt.schedule_time)),
        dispatch_time: Some(timestamp(attempt.dispatch_time)),
        response_time: attempt
            .response
            .as_ref()
            .map(|response| timestamp(response.time)),
        response_status: attempt.response.as_ref().map(|response| {
            let status = response.outcome.status();
            let code = match status {
                200..=299 => rpc::Code::Ok,
                400 => rpc::Code::InvalidArgument,
                401 => rpc::Code::Unauthenticated,
                403 => rpc::Code::PermissionDenied,
                404 => rpc::Code::NotFound,
                409 => rpc::Code::Aborted,
                429 => rpc::Code::ResourceExhausted,
                499 => rpc::Code::Cancelled,
                402..=498 => rpc::Code::FailedPrecondition,
                501 => rpc::Code::Unimplemented,
                503 => rpc::Code::Unavailable,
                504 => rpc::Code::DeadlineExceeded,
                500..=599 => rpc::Code::Internal,
                _ => rpc::Code::Unknown,
            };
            rpc::Status {
                code: code as i32,
                message: format!(
                    "{}({}): HTTP status code {status}",
                    code.as_str_name(),
                    code as i32
                ),
                details: Vec::new(),
            }
        }),
    }
}
