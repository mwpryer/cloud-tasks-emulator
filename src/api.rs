use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jiff::fmt::strtime::BrokenDownTime;
use prost::Message;
use prost_types::{Any, Timestamp};
use tonic::{Code, Request, Response, Status};

use crate::filter::{self, Filter};
use crate::names;
use crate::proto::google::cloud::tasks::v2 as pb;
use crate::proto::google::cloud::tasks::v2::cloud_tasks_server::CloudTasks;
use crate::proto::google::iam::v1 as iam;
use crate::proto::google::longrunning::operations_server::Operations;
use crate::proto::google::longrunning::{self, Operation, operation};
use crate::proto::google::rpc;
use crate::state::{
    AppEngineHttpRequest, AppEngineRouting, Attempt, AuthorizationHeader, DEFAULT_DEADLINE,
    DEFAULT_SCOPE, Emulator, EnforceMode, HttpMethod, HttpRequest, HttpTarget, Payload, Policy,
    Queue, QueueState, RateLimits, RetryConfig, Scheme, State, Task, UriOverride,
};

const QUEUE_NOT_FOUND: &str = "Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.";
const QUEUE_RECENTLY_DELETED: &str =
    "The queue cannot be created because a queue with this name existed too recently.";
const TASK_RECENTLY_REMOVED: &str = "The task cannot be created because a task with this name existed too recently. For more information about task de-duplication see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create#body.request_body.FIELDS.task.";
const TASK_GONE: &str = "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.";
const NOT_FOUND: &str = "Requested entity was not found.";
const QUEUE_NAME_DOCS: &str = "https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues#Queue.FIELDS.name";
const TASK_NAME_DOCS: &str = "https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name";
/// Wording and code are guesses, as recording this needs a Cloud KMS key
const CMEK_HEADER_OVERRIDES: &str =
    "Queues with HTTP Target Override headers cannot be created or updated while CMEK is enabled.";
/// How Google's front end refuses the Operations RPCs that Cloud Tasks does not serve
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
/// Etag of a queue that never had a policy, `ACAB` in base64
const NO_POLICY_ETAG: [u8; 3] = [0x00, 0x20, 0x01];
const MEMBER_PREFIXES: [&str; 7] = [
    "user:",
    "serviceAccount:",
    "group:",
    "domain:",
    "principal:",
    "principalSet:",
    "deleted:",
];
/// Permissions TestIamPermissions accepts for a queue
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
/// Largest encoded task, 1 MiB less the 5 bytes Cloud Tasks stores beyond the proto
const MAX_TASK_SIZE: usize = 1024 * 1024 - 5;

pub(crate) struct CloudTasksService {
    emulator: Arc<Emulator>,
}

impl CloudTasksService {
    pub fn new(emulator: Arc<Emulator>) -> Self {
        Self { emulator }
    }

    /// Checks the task and adds it to the queue, failing as CreateTask does
    fn create(
        &self,
        state: &mut State,
        parent: &str,
        name: String,
        task: pb::Task,
        view: pb::task::View,
    ) -> Result<pb::Task, Status> {
        let mut payload = match task.message_type {
            Some(pb::task::MessageType::HttpRequest(http)) => Payload::Http(http_request(http)?),
            Some(pb::task::MessageType::AppEngineHttpRequest(app_engine)) => {
                if self.emulator.app_engine.is_none() {
                    return Err(no_app_engine(parent));
                }
                Payload::AppEngine(app_engine_http_request(app_engine)?)
            }
            None => return Err(Status::invalid_argument("Task.payload_type is not set.")),
        };
        let task_retry_config = task.retry_config.map(retry_config).transpose()?;
        let (max_deadline, range) = match payload {
            Payload::Http(_) => (Duration::from_secs(30 * 60), "[15s, 30m]"),
            Payload::AppEngine(_) => (Duration::from_secs(24 * 60 * 60 + 15), "[15s, 24h15s]"),
        };
        let dispatch_deadline = task
            .dispatch_deadline
            .map(|deadline| {
                Duration::try_from(deadline)
                    .ok()
                    // Cloud Tasks keeps milliseconds
                    .map(|deadline| Duration::from_millis(deadline.as_millis() as u64))
                    .filter(|deadline| (Duration::from_secs(15)..=max_deadline).contains(deadline))
                    .ok_or_else(|| {
                        Status::invalid_argument(format!(
                            "Task.dispatchDeadline must be between {range}."
                        ))
                    })
            })
            .transpose()?;
        let now = SystemTime::now();
        let schedule_time = match task.schedule_time {
            None => now,
            // Past 9999, which a Timestamp cannot hold
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
                        SystemTime::UNIX_EPOCH
                            + Duration::from_micros(since_epoch.as_micros() as u64)
                    })
                    // Cloud Tasks moves any past time to now, even one before the epoch
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
        if let Payload::Http(http_request) = &mut payload {
            // The queue's token replaces the task's before the URL check, which never sees the replaced one
            if let Some(header) = queue
                .http_target
                .as_ref()
                .and_then(|target| target.authorization_header.clone())
            {
                http_request.authorization_header = Some(header);
            }
            // Checks only the host, allowing the http URLs Cloud Tasks rejects tokens for, so local handlers get tokens
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
        }
        if queue.tasks.contains_key(&name) {
            return Err(Status::already_exists("Requested entity already exists"));
        }
        if queue.removed_tasks.contains(&name, now) {
            return Err(Status::already_exists(TASK_RECENTLY_REMOVED));
        }
        let task = Task::new(
            name,
            payload,
            task_retry_config,
            schedule_time,
            dispatch_deadline,
            now,
        );
        if task_to_proto(&task, pb::task::View::Full).encoded_len() > MAX_TASK_SIZE {
            return Err(Status::invalid_argument("Task size too large"));
        }
        let response = task_to_proto(&task, view);
        queue.tasks.insert(task.name.clone(), task);
        Ok(response)
    }
}

#[tonic::async_trait]
impl CloudTasks for CloudTasksService {
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
        let filter = Filter::parse(&request.filter).map_err(|error| match error {
            filter::Error::Syntax(token) => Status::invalid_argument(format!(
                "Failed to parse filter expression: syntax error at token '{token}'"
            )),
            filter::Error::FieldNotFound(field) => {
                Status::invalid_argument(format!("Field not found: '{field}'."))
            }
            filter::Error::IncorrectType(value) => {
                Status::invalid_argument(format!("The value '{value}' is of incorrect type."))
            }
            filter::Error::NestedType(field) => Status::invalid_argument(format!(
                "Cannot match a nested type '{field}'. A member field of scalar or enum type must be provided."
            )),
            filter::Error::Unsupported(feature) => Status::unimplemented(format!(
                "ListQueues filter {feature} is not supported yet"
            )),
        })?;
        let state = self.emulator.state.lock().unwrap();
        let prefix = format!("{}/queues/", request.parent);
        // Filtering within the page, which the proto allows to return fewer queues than the page size
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
        let state = self.emulator.state.lock().unwrap();
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
        // Cloud Tasks reads a missing queue as one with an empty name
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
        if queue.app_engine_routing_override.is_some() && self.emulator.app_engine.is_none() {
            return Err(no_app_engine(&queue.name));
        }
        let rate_limits = rate_limits(queue.rate_limits.unwrap_or_default())?;
        let retry_config = retry_config(queue.retry_config.unwrap_or_default())?;
        if let Some(target) = &queue.http_target {
            check_http_target(target)?;
        }
        let app_engine_routing_override = queue
            .app_engine_routing_override
            .map(app_engine_routing)
            .transpose()?;
        // A queue holds one target, and Cloud Tasks silently drops the HTTP one
        let http_target = queue
            .http_target
            .filter(|_| app_engine_routing_override.is_none())
            .map(http_target)
            .transpose()?;
        let log_sampling_ratio = log_sampling_ratio(queue.stackdriver_logging_config)?;

        let now = SystemTime::now();
        let mut state = self.emulator.state.lock().unwrap();
        if state.queues.contains_key(&queue.name) {
            return Err(Status::already_exists("Queue already exists"));
        }
        if state.deleted_queues.contains(&queue.name, now) {
            return Err(Status::failed_precondition(QUEUE_RECENTLY_DELETED));
        }
        if http_target
            .as_ref()
            .is_some_and(|target| !target.header_overrides.is_empty())
            && state.cmek_keys.contains_key(&request.parent)
        {
            return Err(Status::failed_precondition(CMEK_HEADER_OVERRIDES));
        }
        let queue = Queue::new(
            queue.name,
            rate_limits,
            retry_config,
            http_target,
            app_engine_routing_override,
            log_sampling_ratio,
            now,
        );
        let response = queue_to_proto(&queue);
        state.queues.insert(queue.name.clone(), queue);
        Ok(Response::new(response))
    }

    async fn update_queue(
        &self,
        request: Request<pb::UpdateQueueRequest>,
    ) -> Result<Response<pb::Queue>, Status> {
        let request = request.into_inner();
        // Cloud Tasks reads a missing queue as one with an empty name
        let update = request.queue.unwrap_or_default();
        resource(
            &update.name,
            names::QUEUE,
            "cloudtasks.queues.update",
            "google.cloud.tasks.v2.CloudTasks.UpdateQueue",
        )?;
        if update.app_engine_routing_override.is_some() && self.emulator.app_engine.is_none() {
            return Err(no_app_engine(&update.name));
        }
        // Cloud Tasks checks the requested target even outside the mask
        if let Some(target) = &update.http_target {
            check_http_target(target)?;
        }

        let now = SystemTime::now();
        let mut state = self.emulator.state.lock().unwrap();
        if !state.queues.contains_key(&update.name)
            && state.deleted_queues.contains(&update.name, now)
        {
            return Err(Status::failed_precondition(QUEUE_RECENTLY_DELETED));
        }
        // Unmasked fields of a new queue take their defaults
        let current = state.queues.get(&update.name).map_or_else(
            || pb::Queue {
                name: update.name.clone(),
                ..Default::default()
            },
            queue_to_proto,
        );
        // An absent mask updates every field, while an empty one updates none
        let merged = match request.update_mask {
            None => update,
            Some(mask) => masked(current, update, &mask.paths)?,
        };
        let rate_limits = rate_limits(merged.rate_limits.unwrap_or_default())?;
        let retry_config = retry_config(merged.retry_config.unwrap_or_default())?;
        let app_engine_routing_override = merged
            .app_engine_routing_override
            .map(app_engine_routing)
            .transpose()?;
        // A queue holds one target, and Cloud Tasks silently drops the HTTP one
        let http_target = merged
            .http_target
            .filter(|_| app_engine_routing_override.is_none())
            .map(http_target)
            .transpose()?;
        let log_sampling_ratio = log_sampling_ratio(merged.stackdriver_logging_config)?;
        let (location, _) = merged
            .name
            .rsplit_once("/queues/")
            .expect("checked queue name");
        if http_target
            .as_ref()
            .is_some_and(|target| !target.header_overrides.is_empty())
            && state.cmek_keys.contains_key(location)
        {
            return Err(Status::failed_precondition(CMEK_HEADER_OVERRIDES));
        }
        let queue = state.queues.entry(merged.name.clone()).or_insert_with(|| {
            Queue::new(
                merged.name,
                rate_limits,
                retry_config,
                http_target.clone(),
                app_engine_routing_override.clone(),
                log_sampling_ratio,
                now,
            )
        });
        queue.rate_limits = rate_limits;
        queue.retry_config = retry_config;
        queue.http_target = http_target;
        queue.app_engine_routing_override = app_engine_routing_override;
        queue.log_sampling_ratio = log_sampling_ratio;
        let response = queue_to_proto(queue);
        drop(state);
        self.emulator.wake.notify_one();
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
        let mut state = self.emulator.state.lock().unwrap();
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
        let mut state = self.emulator.state.lock().unwrap();
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
        let mut state = self.emulator.state.lock().unwrap();
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
        let mut state = self.emulator.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.name)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        queue.state = QueueState::Running;
        let response = queue_to_proto(queue);
        drop(state);
        self.emulator.wake.notify_one();
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
        let state = self.emulator.state.lock().unwrap();
        let queue = state
            .queues
            .get(&request.resource)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        Ok(Response::new(policy_to_proto(queue.policy.as_ref())))
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
        // Cloud Tasks reads a missing policy as an empty one
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

        let mut state = self.emulator.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.resource)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        let etag = queue
            .policy
            .as_ref()
            .map_or(&NO_POLICY_ETAG[..], |policy| &policy.etag);
        if !policy.etag.is_empty() && policy.etag != etag {
            return Err(Status::aborted(format!(
                "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag '{}' did not match the current policy's ETag '{}'.",
                c_escape(&policy.etag),
                c_escape(etag)
            )));
        }
        let policy = queue.policy.insert(Policy {
            etag: rand::random(),
            bindings,
        });
        Ok(Response::new(policy_to_proto(Some(policy))))
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
            .emulator
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
        // Grants every permission, as the emulator has no caller identity
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
        // Unlike create and get, list echoes an unknown view as basic
        let view = match response_view(request.response_view) {
            pb::task::View::Full => pb::task::View::Full,
            _ => pb::task::View::Basic,
        };
        let state = self.emulator.state.lock().unwrap();
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
        let mut state = self.emulator.state.lock().unwrap();
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
        // Cloud Tasks reads a missing task as one without a payload
        let task = request.task.unwrap_or_default();
        let name = task_name(&request.parent, &task.name)?;
        let mut state = self.emulator.state.lock().unwrap();
        let response = self.create(
            &mut state,
            &request.parent,
            name,
            task,
            response_view(request.response_view),
        )?;
        drop(state);
        self.emulator.wake.notify_one();
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
        let mut state = self.emulator.state.lock().unwrap();
        let name = operation_name(&state, &request.parent, request.request_id)?;
        let mut tasks = Vec::new();
        let mut failed_requests = BTreeMap::new();
        for (index, (task_name, task, view)) in creates.into_iter().enumerate() {
            // A batch words a missing payload differently from CreateTask
            let created = match task.message_type {
                None => Err(Status::invalid_argument("Payload type must be set.")),
                Some(_) => self.create(&mut state, &request.parent, task_name, task, view),
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
            metadata: Some(prost_types::Any {
                type_url: "type.googleapis.com/google.cloud.tasks.v2.BatchCreateTasksMetadata"
                    .to_owned(),
                value: metadata.encode_to_vec(),
            }),
            done: true,
            // A batch that created nothing has no response
            result: (!tasks.is_empty()).then(|| {
                operation::Result::Response(prost_types::Any {
                    type_url: "type.googleapis.com/google.cloud.tasks.v2.BatchCreateTasksResponse"
                        .to_owned(),
                    value: pb::BatchCreateTasksResponse { tasks }.encode_to_vec(),
                })
            }),
        };
        state.operations.insert(name, operation.clone());
        drop(state);
        self.emulator.wake.notify_one();
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
        let mut state = self.emulator.state.lock().unwrap();
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
        let mut state = self.emulator.state.lock().unwrap();
        let name = operation_name(&state, &request.parent, request.request_id)?;
        let mut deleted = HashSet::new();
        let mut failed_requests = BTreeMap::new();
        for (index, task) in request.names.iter().enumerate() {
            // A name deleted earlier in the batch counts as deleted again
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
                code: tonic::Code::Aborted as i32,
                message: "None of the requests succeeded, refer to BatchDeleteTasksMetadata.failed_requests for individual error details".to_owned(),
                details: Vec::new(),
            })
        } else {
            operation::Result::Response(prost_types::Any {
                type_url: "type.googleapis.com/google.protobuf.Empty".to_owned(),
                value: Vec::new(),
            })
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
            metadata: Some(prost_types::Any {
                type_url: "type.googleapis.com/google.cloud.tasks.v2.BatchDeleteTasksMetadata"
                    .to_owned(),
                value: metadata.encode_to_vec(),
            }),
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
        let mut state = self.emulator.state.lock().unwrap();
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
        // Cloud Tasks answers before it dispatches, so the new attempt has only its schedule time
        let mut response = task_to_proto(task, view);
        response.dispatch_count += 1;
        response.last_attempt = Some(pb::Attempt {
            schedule_time: Some(timestamp(task.schedule_time)),
            ..Default::default()
        });
        drop(state);
        self.emulator.wake.notify_one();
        Ok(Response::new(response))
    }

    async fn update_cmek_config(
        &self,
        request: Request<pb::UpdateCmekConfigRequest>,
    ) -> Result<Response<pb::CmekConfig>, Status> {
        // Cloud Tasks reads a missing config as one with an empty name
        let config = request.into_inner().cmek_config.unwrap_or_default();
        let location = cmek_location(
            &config.name,
            "google.cloud.tasks.v2.CloudTasks.UpdateCmekConfig",
        )?;
        let mut state = self.emulator.state.lock().unwrap();
        if config.kms_key.is_empty() {
            state.cmek_keys.remove(location);
            return Ok(Response::new(config));
        }
        // Unlike the docs, which name a URI override host instead
        let prefix = format!("{location}/queues/");
        if state.queues.values().any(|queue| {
            queue.name.starts_with(&prefix)
                && queue
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
        // Checks only the key's form and location, as the emulator cannot see Cloud KMS
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
        let state = self.emulator.state.lock().unwrap();
        let kms_key = state.cmek_keys.get(location).cloned().unwrap_or_default();
        Ok(Response::new(pb::CmekConfig {
            name: request.name,
            kms_key,
        }))
    }
}

pub(crate) struct OperationsService {
    emulator: Arc<Emulator>,
}

impl OperationsService {
    pub fn new(emulator: Arc<Emulator>) -> Self {
        Self { emulator }
    }
}

#[tonic::async_trait]
impl Operations for OperationsService {
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
        let state = self.emulator.state.lock().unwrap();
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

/// Rejects a malformed name the way Google's front end does, then checks its IDs as Cloud Tasks does
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

/// Rejects an empty name or one outside a project, the way Google's front end does for `method`
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
                Any {
                    type_url: "type.googleapis.com/google.rpc.LocalizedMessage".to_owned(),
                    value: localized.encode_to_vec(),
                },
                help(
                    "Google developers console",
                    "https://console.developers.google.com",
                ),
            ],
        ));
    }
    Ok(())
}

/// A status with `details`, which clients read from the `grpc-status-details-bin` trailer
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
    Any {
        type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_owned(),
        value: info.encode_to_vec(),
    }
}

fn help(description: &str, url: &str) -> Any {
    let help = rpc::Help {
        links: vec![rpc::help::Link {
            description: description.to_owned(),
            url: url.to_owned(),
        }],
    };
    Any {
        type_url: "type.googleapis.com/google.rpc.Help".to_owned(),
        value: help.encode_to_vec(),
    }
}

/// Checks a CMEK config name and returns its location's name
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

/// Checks a create request's task name, generating an ID for an unnamed task
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

/// Checks that a well-formed task name is in the request's queue
fn check_task_queue(parent: &str, name: &str) -> Result<(), Status> {
    let (queue, _) = name.rsplit_once("/tasks/").expect("checked task name");
    if queue != parent {
        return Err(Status::invalid_argument(format!(
            "The queue name from request ('{parent}') must be the same as the queue name in the named task ('{queue}')."
        )));
    }
    Ok(())
}

/// Names a batch's operation by its request ID, or a generated one, refusing a name already taken
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

/// Checks the queue, task and operation IDs among the IDs of a name in these collections
fn check_ids(ids: &[&str], collections: &[&str]) -> Result<(), Status> {
    for (collection, id) in collections.iter().zip(ids) {
        let status = match *collection {
            "queues" if !names::is_queue_id(id) => detailed(
                Code::InvalidArgument,
                &format!(
                    "Queue ID \"{id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Queue ID must be between 1 and 100 characters."
                ),
                vec![help("Definition of queue ID", QUEUE_NAME_DOCS)],
            ),
            "tasks" if !names::is_task_id(id) => detailed(
                Code::InvalidArgument,
                &format!(
                    "Task ID \"{id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Task ID must between 1 and 500 characters."
                ),
                vec![help("Definition of task ID", TASK_NAME_DOCS)],
            ),
            "operations" if !names::is_operation_id(id) => Status::invalid_argument(format!(
                "Operation ID \"{id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Operation ID must be between 1 and 100 characters."
            )),
            _ => continue,
        };
        return Err(status);
    }
    Ok(())
}

/// Copies the fields `paths` name from `update` onto `current`, ignoring output-only ones
fn masked(
    mut current: pb::Queue,
    update: pb::Queue,
    paths: &[String],
) -> Result<pb::Queue, Status> {
    for path in paths {
        merge_queue(&mut current, &update, path)
            .ok_or_else(|| Status::invalid_argument(format!("Invalid mask: {path}")))?;
    }
    Ok(current)
}

fn split_path(path: &str) -> (&str, Option<&str>) {
    path.split_once('.')
        .map_or((path, None), |(field, rest)| (field, Some(rest)))
}

/// None for a path below a scalar
fn merge_leaf<T: Clone>(current: &mut T, update: &T, rest: Option<&str>) -> Option<()> {
    if rest.is_some() {
        return None;
    }
    current.clone_from(update);
    Some(())
}

/// Replaces the whole message, or merges below it when either side has it
fn merge_message<T: Clone + Default>(
    current: &mut Option<T>,
    update: &Option<T>,
    rest: Option<&str>,
    merge: impl Fn(&mut T, &T, &str) -> Option<()>,
) -> Option<()> {
    let Some(rest) = rest else {
        current.clone_from(update);
        return Some(());
    };
    // Merges even when neither side has the message, so its path is still checked
    let mut merged = current.clone().unwrap_or_default();
    merge(&mut merged, &update.clone().unwrap_or_default(), rest)?;
    if current.is_some() || update.is_some() {
        *current = Some(merged);
    }
    Some(())
}

/// Merges one member of a oneof, which leaves the oneof unchanged when neither side holds that member
fn merge_member<O, T: Clone + Default>(
    current: &mut Option<O>,
    update: &Option<O>,
    rest: Option<&str>,
    member: impl Fn(&O) -> Option<&T>,
    wrap: impl Fn(T) -> O,
    merge: impl Fn(&mut T, &T, &str) -> Option<()>,
) -> Option<()> {
    let held = current.as_ref().and_then(&member).is_some();
    let mut merged = current.as_ref().and_then(&member).cloned();
    merge_message(
        &mut merged,
        &update.as_ref().and_then(&member).cloned(),
        rest,
        merge,
    )?;
    match merged {
        Some(merged) => *current = Some(wrap(merged)),
        None if held => *current = None,
        None => {}
    }
    Some(())
}

fn merge_queue(current: &mut pb::Queue, update: &pb::Queue, path: &str) -> Option<()> {
    let (field, rest) = split_path(path);
    match field {
        "rate_limits" => merge_message(
            &mut current.rate_limits,
            &update.rate_limits,
            rest,
            |current, update, path| {
                let (field, rest) = split_path(path);
                match field {
                    "max_dispatches_per_second" => merge_leaf(
                        &mut current.max_dispatches_per_second,
                        &update.max_dispatches_per_second,
                        rest,
                    ),
                    "max_burst_size" => {
                        merge_leaf(&mut current.max_burst_size, &update.max_burst_size, rest)
                    }
                    "max_concurrent_dispatches" => merge_leaf(
                        &mut current.max_concurrent_dispatches,
                        &update.max_concurrent_dispatches,
                        rest,
                    ),
                    _ => None,
                }
            },
        ),
        "retry_config" => merge_message(
            &mut current.retry_config,
            &update.retry_config,
            rest,
            |current, update, path| {
                let (field, rest) = split_path(path);
                match field {
                    "max_attempts" => {
                        merge_leaf(&mut current.max_attempts, &update.max_attempts, rest)
                    }
                    "max_retry_duration" => merge_leaf(
                        &mut current.max_retry_duration,
                        &update.max_retry_duration,
                        rest,
                    ),
                    "min_backoff" => {
                        merge_leaf(&mut current.min_backoff, &update.min_backoff, rest)
                    }
                    "max_backoff" => {
                        merge_leaf(&mut current.max_backoff, &update.max_backoff, rest)
                    }
                    "max_doublings" => {
                        merge_leaf(&mut current.max_doublings, &update.max_doublings, rest)
                    }
                    _ => None,
                }
            },
        ),
        "http_target" => merge_message(
            &mut current.http_target,
            &update.http_target,
            rest,
            merge_http_target,
        ),
        "stackdriver_logging_config" => merge_message(
            &mut current.stackdriver_logging_config,
            &update.stackdriver_logging_config,
            rest,
            |current, update, path| match split_path(path) {
                ("sampling_ratio", rest) => {
                    merge_leaf(&mut current.sampling_ratio, &update.sampling_ratio, rest)
                }
                _ => None,
            },
        ),
        "app_engine_routing_override" => merge_message(
            &mut current.app_engine_routing_override,
            &update.app_engine_routing_override,
            rest,
            |current, update, path| {
                let (field, rest) = split_path(path);
                match field {
                    "service" => merge_leaf(&mut current.service, &update.service, rest),
                    "version" => merge_leaf(&mut current.version, &update.version, rest),
                    "instance" => merge_leaf(&mut current.instance, &update.instance, rest),
                    "host" => merge_leaf(&mut current.host, &update.host, rest),
                    _ => None,
                }
            },
        ),
        // Output only
        "name" | "state" | "purge_time" if rest.is_none() => Some(()),
        _ => None,
    }
}

fn merge_http_target(
    current: &mut pb::HttpTarget,
    update: &pb::HttpTarget,
    path: &str,
) -> Option<()> {
    use pb::http_target::AuthorizationHeader as Header;
    let (field, rest) = split_path(path);
    match field {
        "uri_override" => merge_message(
            &mut current.uri_override,
            &update.uri_override,
            rest,
            |current, update, path| {
                let (field, rest) = split_path(path);
                match field {
                    "scheme" => merge_leaf(&mut current.scheme, &update.scheme, rest),
                    "host" => merge_leaf(&mut current.host, &update.host, rest),
                    "port" => merge_leaf(&mut current.port, &update.port, rest),
                    "path_override" => merge_message(
                        &mut current.path_override,
                        &update.path_override,
                        rest,
                        |current, update, path| match split_path(path) {
                            ("path", rest) => merge_leaf(&mut current.path, &update.path, rest),
                            _ => None,
                        },
                    ),
                    "query_override" => merge_message(
                        &mut current.query_override,
                        &update.query_override,
                        rest,
                        |current, update, path| match split_path(path) {
                            ("query_params", rest) => {
                                merge_leaf(&mut current.query_params, &update.query_params, rest)
                            }
                            _ => None,
                        },
                    ),
                    "uri_override_enforce_mode" => merge_leaf(
                        &mut current.uri_override_enforce_mode,
                        &update.uri_override_enforce_mode,
                        rest,
                    ),
                    _ => None,
                }
            },
        ),
        "http_method" => merge_leaf(&mut current.http_method, &update.http_method, rest),
        "header_overrides" => merge_leaf(
            &mut current.header_overrides,
            &update.header_overrides,
            rest,
        ),
        "oauth_token" => merge_member(
            &mut current.authorization_header,
            &update.authorization_header,
            rest,
            |header| match header {
                Header::OauthToken(token) => Some(token),
                Header::OidcToken(_) => None,
            },
            Header::OauthToken,
            |current, update, path| {
                let (field, rest) = split_path(path);
                match field {
                    "service_account_email" => merge_leaf(
                        &mut current.service_account_email,
                        &update.service_account_email,
                        rest,
                    ),
                    "scope" => merge_leaf(&mut current.scope, &update.scope, rest),
                    _ => None,
                }
            },
        ),
        "oidc_token" => merge_member(
            &mut current.authorization_header,
            &update.authorization_header,
            rest,
            |header| match header {
                Header::OidcToken(token) => Some(token),
                Header::OauthToken(_) => None,
            },
            Header::OidcToken,
            |current, update, path| {
                let (field, rest) = split_path(path);
                match field {
                    "service_account_email" => merge_leaf(
                        &mut current.service_account_email,
                        &update.service_account_email,
                        rest,
                    ),
                    "audience" => merge_leaf(&mut current.audience, &update.audience, rest),
                    _ => None,
                }
            },
        ),
        _ => None,
    }
}

/// Formats a time as Cloud Tasks reports it, in Pacific time with the shortest fraction
fn pacific_time(time: Timestamp) -> String {
    // jiff's timestamps end a day before 10000, by when the offset has stopped changing
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

/// Pages through names under `prefix`, using the last name returned as an opaque page token
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
    let names: Vec<&String> = items
        .range::<str, _>((start, Bound::Unbounded))
        .map(|(name, _)| name)
        .take_while(|name| name.starts_with(prefix))
        .take(page_size)
        .collect();
    // Cloud Tasks returns a token after every non-empty page, so only an empty page ends the list
    let next_page_token = names
        .last()
        .map_or_else(String::new, |name| URL_SAFE_NO_PAD.encode(name));
    Ok((
        names
            .into_iter()
            .map(|name| to_proto(&items[name]))
            .collect(),
        next_page_token,
    ))
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
    // Output only, so the caller's value is ignored
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
    // Cloud Tasks stores an unknown method as POST
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
    headers.retain(|name, _| !name.eq_ignore_ascii_case("user-agent"));
    headers.insert("User-Agent".to_owned(), "Google-Cloud-Tasks".to_owned());
    Ok(HttpRequest {
        url: request.url,
        http_method,
        headers,
        body: request.body,
        authorization_header,
    })
}

fn app_engine_http_request(
    request: pb::AppEngineHttpRequest,
) -> Result<AppEngineHttpRequest, Status> {
    let relative_uri = match request.relative_uri.as_str() {
        "" => "/".to_owned(),
        _ => request.relative_uri,
    };
    if !relative_uri.starts_with('/') || relative_uri.contains(' ') || relative_uri.len() > 2083 {
        return Err(Status::invalid_argument(
            "Invalid relative uri. The relative uri must begin with '/'. No spaces are allowed and the maximum length allowed is 2083 characters.",
        ));
    }
    // Cloud Tasks stores an unknown method as POST
    let http_method = pb::HttpMethod::try_from(request.http_method)
        .ok()
        .and_then(http_method)
        .unwrap_or(HttpMethod::Post);
    if !request.body.is_empty() && !http_method.allows_body() {
        return Err(Status::invalid_argument(
            "Body can only be set if the http_method is POST, PUT, or PATCH.",
        ));
    }
    const USER_AGENT: &str = "AppEngine-Google; (+http://code.google.com/appengine)";
    let mut headers = stored_headers(request.headers);
    match headers
        .iter_mut()
        .find(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
    {
        Some((_, agent)) => *agent = format!("{agent} {USER_AGENT}"),
        None => {
            headers.insert("User-Agent".to_owned(), USER_AGENT.to_owned());
        }
    }
    if !request.body.is_empty() {
        headers.insert("Content-Length".to_owned(), request.body.len().to_string());
        if !headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("content-type"))
        {
            headers.insert(
                "Content-Type".to_owned(),
                "application/octet-stream".to_owned(),
            );
        }
    }
    Ok(AppEngineHttpRequest {
        http_method,
        routing: app_engine_routing(request.app_engine_routing.unwrap_or_default())?,
        relative_uri,
        headers,
        body: request.body,
    })
}

/// Ignores the host, which is output only
fn app_engine_routing(routing: pb::AppEngineRouting) -> Result<AppEngineRouting, Status> {
    let set = |value: String| Some(value).filter(|value| !value.is_empty());
    let routing = AppEngineRouting {
        service: set(routing.service),
        version: set(routing.version),
        instance: set(routing.instance),
    };
    if routing.instance.is_some() && routing.service.is_none() && routing.version.is_none() {
        return Err(Status::invalid_argument(
            "If instance is set then service or version must also be set. For more information, see https://cloud.google.com/appengine/docs/standard/python/how-requests-are-routed#targeted_routing",
        ));
    }
    Ok(routing)
}

/// For the project in a queue or task name, as Cloud Tasks answers when the project has no app
fn no_app_engine(name: &str) -> Status {
    let project = names::project(name);
    Status::not_found(format!(
        "The project {project} does not contain an App Engine application. Please visit https://console.developers.google.com/appengine?project={project} to add an App Engine application."
    ))
}

/// The instance, version and service before the app's domain, which the emulator takes to be `<project>.appspot.com`
fn app_engine_host(routing: &AppEngineRouting, name: &str) -> String {
    [&routing.instance, &routing.version, &routing.service]
        .into_iter()
        .flatten()
        .cloned()
        .chain([format!("{}.appspot.com", names::project(name))])
        .collect::<Vec<_>>()
        .join(".")
}

/// None for unspecified
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

/// Checks a requested target in full, as Cloud Tasks skips the token check once a mask merges it
fn check_http_target(target: &pb::HttpTarget) -> Result<(), Status> {
    let target = http_target(target.clone())?;
    target
        .authorization_header
        .as_ref()
        .map_or(Ok(()), service_account)
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
                .map(|scheme| match pb::uri_override::Scheme::try_from(scheme) {
                    Ok(pb::uri_override::Scheme::Unspecified) => Ok(Scheme::Unspecified),
                    Ok(pb::uri_override::Scheme::Http) => Ok(Scheme::Http),
                    Ok(pb::uri_override::Scheme::Https) => Ok(Scheme::Https),
                    Err(_) => Err(parse_error("")),
                })
                .transpose()?;
            if let Some(host) = &uri.host
                // Only a bare host, which parses back to itself
                && !reqwest::Url::parse(&format!("http://{host}/")).is_ok_and(|url| {
                    url.host_str()
                        .is_some_and(|parsed| parsed.eq_ignore_ascii_case(host))
                        && url.port().is_none()
                        && url.path() == "/"
                })
            {
                return Err(parse_error("Invalid Host."));
            }
            let port = uri
                .port
                .map(u64::try_from)
                .transpose()
                .map_err(|_| parse_error("Port number should be a positive integer."))?;
            let enforce_mode = match pb::uri_override::UriOverrideEnforceMode::try_from(
                uri.uri_override_enforce_mode,
            ) {
                Ok(pb::uri_override::UriOverrideEnforceMode::Unspecified) => None,
                Ok(pb::uri_override::UriOverrideEnforceMode::IfNotExists) => {
                    Some(EnforceMode::IfNotExists)
                }
                Ok(pb::uri_override::UriOverrideEnforceMode::Always) => Some(EnforceMode::Always),
                Err(_) => return Err(parse_error("")),
            };
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

/// Zero logs nothing, and is how Cloud Tasks reads a missing config
fn log_sampling_ratio(config: Option<pb::StackdriverLoggingConfig>) -> Result<f64, Status> {
    let ratio = config.map_or(0.0, |config| config.sampling_ratio);
    if !(0.0..=1.0).contains(&ratio) {
        return Err(Status::invalid_argument(
            "sampling_ratio must be between 0.0 and 1.0.",
        ));
    }
    Ok(ratio)
}

/// Drops and rewrites headers as Cloud Tasks does when it stores a task, never rejecting one, but leaves User-Agent to the caller
fn stored_headers(headers: BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .into_iter()
        .filter_map(|(name, value)| {
            let lowercase = name.to_ascii_lowercase();
            // Cloud Tasks ends a value at NUL and replaces line breaks
            let value = value
                .split('\0')
                .next()
                .expect("split yields at least one part")
                .replace(['\r', '\n'], "_");
            let kept = (lowercase == "user-agent" || !RESERVED_HEADERS.contains(&lowercase.as_str()))
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

/// Basic for an unknown view too, which create and get then echo as unspecified
fn response_view(view: i32) -> pb::task::View {
    match pb::task::View::try_from(view) {
        Ok(pb::task::View::Unspecified | pb::task::View::Basic) => pb::task::View::Basic,
        Ok(pb::task::View::Full) => pb::task::View::Full,
        Err(_) => pb::task::View::Unspecified,
    }
}

fn proto_duration(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

/// Truncated to microseconds, as Cloud Tasks stores times
fn timestamp(time: SystemTime) -> Timestamp {
    let since_epoch = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("times are after the epoch");
    Timestamp {
        seconds: since_epoch.as_secs() as i64,
        nanos: (since_epoch.subsec_micros() * 1000) as i32,
    }
}

fn policy_to_proto(policy: Option<&Policy>) -> iam::Policy {
    match policy {
        None => iam::Policy {
            etag: NO_POLICY_ETAG.to_vec(),
            ..Default::default()
        },
        Some(policy) => iam::Policy {
            version: 1,
            etag: policy.etag.to_vec(),
            bindings: policy
                .bindings
                .iter()
                .map(|(role, members)| iam::Binding {
                    role: role.clone(),
                    members: members.iter().cloned().collect(),
                    condition: None,
                })
                .collect(),
            audit_configs: Vec::new(),
        },
    }
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
    pb::Queue {
        name: queue.name.clone(),
        rate_limits: Some(pb::RateLimits {
            max_dispatches_per_second: queue.rate_limits.max_dispatches_per_second,
            max_burst_size: queue.rate_limits.max_burst_size as i32,
            max_concurrent_dispatches: queue.rate_limits.max_concurrent_dispatches as i32,
        }),
        retry_config: Some(retry_config_to_proto(&queue.retry_config)),
        state: match queue.state {
            QueueState::Running => pb::queue::State::Running,
            QueueState::Paused => pb::queue::State::Paused,
        }
        .into(),
        purge_time: queue.purge_time.map(timestamp),
        http_target: queue.http_target.as_ref().map(|target| pb::HttpTarget {
            uri_override: target.uri_override.as_ref().map(|uri| pb::UriOverride {
                scheme: uri.scheme.map(|scheme| {
                    match scheme {
                        Scheme::Unspecified => pb::uri_override::Scheme::Unspecified,
                        Scheme::Http => pb::uri_override::Scheme::Http,
                        Scheme::Https => pb::uri_override::Scheme::Https,
                    }
                    .into()
                }),
                host: uri.host.clone(),
                port: uri.port.map(|port| port as i64),
                path_override: uri.path.clone().map(|path| pb::PathOverride { path }),
                query_override: uri
                    .query
                    .clone()
                    .map(|query_params| pb::QueryOverride { query_params }),
                uri_override_enforce_mode: match uri.enforce_mode {
                    None => pb::uri_override::UriOverrideEnforceMode::Unspecified,
                    Some(EnforceMode::IfNotExists) => {
                        pb::uri_override::UriOverrideEnforceMode::IfNotExists
                    }
                    Some(EnforceMode::Always) => pb::uri_override::UriOverrideEnforceMode::Always,
                }
                .into(),
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
        app_engine_routing_override: queue.app_engine_routing_override.as_ref().map(|routing| {
            pb::AppEngineRouting {
                service: routing.service.clone().unwrap_or_default(),
                version: routing.version.clone().unwrap_or_default(),
                instance: routing.instance.clone().unwrap_or_default(),
                host: app_engine_host(routing, &queue.name),
            }
        }),
        stackdriver_logging_config: (queue.log_sampling_ratio != 0.0).then_some(
            pb::StackdriverLoggingConfig {
                sampling_ratio: queue.log_sampling_ratio,
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
        max_retry_duration: retry_config.max_retry_duration.map(proto_duration),
        min_backoff: Some(proto_duration(retry_config.min_backoff)),
        max_backoff: Some(proto_duration(retry_config.max_backoff)),
        max_doublings: retry_config.max_doublings as i32,
    }
}

fn task_to_proto(task: &Task, view: pb::task::View) -> pb::Task {
    let create_time = task
        .create_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("times are after the epoch");
    let body = |body: &Vec<u8>| {
        if view == pb::task::View::Full {
            body.clone()
        } else {
            Vec::new()
        }
    };
    let (message_type, dispatch_deadline) = match &task.payload {
        Payload::Http(request) => (
            pb::task::MessageType::HttpRequest(pb::HttpRequest {
                url: request.url.clone(),
                http_method: http_method_to_proto(request.http_method).into(),
                headers: request.headers.clone(),
                body: body(&request.body),
                authorization_header: request.authorization_header.clone().map(
                    |header| match header {
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
                    },
                ),
            }),
            Some(task.dispatch_deadline.unwrap_or(DEFAULT_DEADLINE)),
        ),
        Payload::AppEngine(request) => {
            let routing = &request.routing;
            // Cloud Tasks keeps only the host, and reads its labels back as version and service or as all three
            let labels: Vec<&str> = [&routing.instance, &routing.version, &routing.service]
                .into_iter()
                .flatten()
                .flat_map(|value| value.split('.'))
                .collect();
            let (instance, version, service) = match labels[..] {
                [version, service] => ("", version, service),
                [instance, version, service] => (instance, version, service),
                _ => ("", "", ""),
            };
            (
                pb::task::MessageType::AppEngineHttpRequest(pb::AppEngineHttpRequest {
                    http_method: http_method_to_proto(request.http_method).into(),
                    app_engine_routing: Some(pb::AppEngineRouting {
                        service: service.to_owned(),
                        version: version.to_owned(),
                        instance: instance.to_owned(),
                        host: app_engine_host(routing, &task.name),
                    }),
                    relative_uri: request.relative_uri.clone(),
                    headers: request.headers.clone(),
                    body: body(&request.body),
                }),
                task.dispatch_deadline,
            )
        }
    };
    pb::Task {
        name: task.name.clone(),
        message_type: Some(message_type),
        schedule_time: Some(timestamp(task.schedule_time)),
        create_time: Some(Timestamp {
            seconds: create_time.as_secs() as i64,
            nanos: 0,
        }),
        dispatch_deadline: dispatch_deadline.map(proto_duration),
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
            let (code, name) = match status {
                200..=299 => (0, "OK"),
                400 => (3, "INVALID_ARGUMENT"),
                401 => (16, "UNAUTHENTICATED"),
                403 => (7, "PERMISSION_DENIED"),
                404 => (5, "NOT_FOUND"),
                409 => (10, "ABORTED"),
                429 => (8, "RESOURCE_EXHAUSTED"),
                499 => (1, "CANCELLED"),
                402..=498 => (9, "FAILED_PRECONDITION"),
                501 => (12, "UNIMPLEMENTED"),
                503 => (14, "UNAVAILABLE"),
                504 => (4, "DEADLINE_EXCEEDED"),
                500..=599 => (13, "INTERNAL"),
                _ => (2, "UNKNOWN"),
            };
            let message = format!("{name}({code}): HTTP status code {status}");
            rpc::Status {
                code,
                message,
                details: Vec::new(),
            }
        }),
    }
}
