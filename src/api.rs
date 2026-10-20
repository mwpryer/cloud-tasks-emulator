use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use jiff::fmt::strtime::BrokenDownTime;
use prost::Message;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use crate::filter::{self, Filter};
use crate::names;
use crate::proto::google::cloud::tasks::v2 as pb;
use crate::proto::google::cloud::tasks::v2::cloud_tasks_server::CloudTasks;
use crate::proto::google::iam::v1 as iam;
use crate::proto::google::longrunning::Operation;
use crate::proto::google::rpc;
use crate::state::{
    Attempt, AuthorizationHeader, Emulator, HttpMethod, HttpRequest, Queue, QueueState, RateLimits,
    RetryConfig, Task,
};

const QUEUE_NOT_FOUND: &str = "Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.";
const QUEUE_RECENTLY_DELETED: &str =
    "The queue cannot be created because a queue with this name existed too recently.";
const TASK_RECENTLY_REMOVED: &str = "The task cannot be created because a task with this name existed too recently. For more information about task de-duplication see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create#body.request_body.FIELDS.task.";
const TASK_GONE: &str = "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.";
const NOT_FOUND: &str = "Requested entity was not found.";
/// Largest encoded task, 1 MiB less the 5 bytes Cloud Tasks stores beyond the proto
const MAX_TASK_SIZE: usize = 1024 * 1024 - 5;

pub(crate) struct CloudTasksService {
    emulator: Arc<Emulator>,
}

impl CloudTasksService {
    pub fn new(emulator: Arc<Emulator>) -> Self {
        Self { emulator }
    }
}

#[tonic::async_trait]
impl CloudTasks for CloudTasksService {
    async fn list_queues(
        &self,
        request: Request<pb::ListQueuesRequest>,
    ) -> Result<Response<pb::ListQueuesResponse>, Status> {
        let request = request.into_inner();
        resource(&request.parent, names::LOCATION, "cloudtasks.queues.list")?;
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
        resource(&request.name, names::QUEUE, "cloudtasks.queues.get")?;
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
        resource(&request.parent, names::LOCATION, "cloudtasks.queues.create")?;
        // Cloud Tasks reads a missing queue as one with an empty name
        let queue = request.queue.unwrap_or_default();
        let ids = names::ids(&queue.name, names::QUEUE).ok_or_else(|| {
            Status::invalid_argument(
                r#"Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>"."#,
            )
        })?;
        check_ids(&ids)?;
        if !queue
            .name
            .starts_with(&format!("{}/queues/", request.parent))
        {
            return Err(Status::invalid_argument(format!(
                "{} must begin with {}.",
                queue.name, request.parent
            )));
        }
        if queue.app_engine_routing_override.is_some()
            || queue.http_target.is_some()
            || queue.stackdriver_logging_config.is_some()
        {
            return Err(Status::unimplemented(
                "Queue app_engine_routing_override, http_target and stackdriver_logging_config are not supported yet",
            ));
        }
        let rate_limits = rate_limits(queue.rate_limits.unwrap_or_default())?;
        let retry_config = retry_config(queue.retry_config.unwrap_or_default())?;

        let now = SystemTime::now();
        let mut state = self.emulator.state.lock().unwrap();
        if state.queues.contains_key(&queue.name) {
            return Err(Status::already_exists("Queue already exists"));
        }
        if state.deleted_queues.contains(&queue.name, now) {
            return Err(Status::failed_precondition(QUEUE_RECENTLY_DELETED));
        }
        let queue = Queue::new(queue.name, rate_limits, retry_config, now);
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
        resource(&update.name, names::QUEUE, "cloudtasks.queues.update")?;
        if update.app_engine_routing_override.is_some()
            || update.http_target.is_some()
            || update.stackdriver_logging_config.is_some()
        {
            return Err(Status::unimplemented(
                "Queue app_engine_routing_override, http_target and stackdriver_logging_config are not supported yet",
            ));
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
        let queue = state
            .queues
            .entry(merged.name.clone())
            .or_insert_with(|| Queue::new(merged.name, rate_limits, retry_config, now));
        queue.rate_limits = rate_limits;
        queue.retry_config = retry_config;
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
        resource(&request.name, names::QUEUE, "cloudtasks.queues.delete")?;
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
        resource(&request.name, names::QUEUE, "cloudtasks.queues.purge")?;
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
        resource(&request.name, names::QUEUE, "cloudtasks.queues.pause")?;
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
        resource(&request.name, names::QUEUE, "cloudtasks.queues.resume")?;
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
        _: Request<iam::GetIamPolicyRequest>,
    ) -> Result<Response<iam::Policy>, Status> {
        Err(Status::unimplemented("GetIamPolicy is not supported yet"))
    }

    async fn set_iam_policy(
        &self,
        _: Request<iam::SetIamPolicyRequest>,
    ) -> Result<Response<iam::Policy>, Status> {
        Err(Status::unimplemented("SetIamPolicy is not supported yet"))
    }

    async fn test_iam_permissions(
        &self,
        _: Request<iam::TestIamPermissionsRequest>,
    ) -> Result<Response<iam::TestIamPermissionsResponse>, Status> {
        Err(Status::unimplemented(
            "TestIamPermissions is not supported yet",
        ))
    }

    async fn list_tasks(
        &self,
        request: Request<pb::ListTasksRequest>,
    ) -> Result<Response<pb::ListTasksResponse>, Status> {
        let request = request.into_inner();
        resource(&request.parent, names::QUEUE, "cloudtasks.tasks.list")?;
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
        resource(&request.name, names::TASK, "cloudtasks.tasks.get")?;
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
        resource(&request.parent, names::QUEUE, "cloudtasks.tasks.create")?;
        let view = response_view(request.response_view);
        // Cloud Tasks reads a missing task as one without a payload
        let task = request.task.unwrap_or_default();
        let name = if task.name.is_empty() {
            format!("{}/tasks/{}", request.parent, rand::random::<u64>())
        } else {
            let ids = names::ids(&task.name, names::TASK).ok_or_else(|| {
                Status::invalid_argument(
                    r#"Task name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>/tasks/<TASK_ID>"."#,
                )
            })?;
            check_ids(&ids)?;
            let (queue, _) = task.name.rsplit_once("/tasks/").expect("checked task name");
            if queue != request.parent {
                return Err(Status::invalid_argument(format!(
                    "The queue name from request ('{}') must be the same as the queue name in the named task ('{queue}').",
                    request.parent
                )));
            }
            task.name
        };
        let http_request = match task.message_type {
            Some(pb::task::MessageType::HttpRequest(http_request)) => {
                self::http_request(http_request)?
            }
            Some(pb::task::MessageType::AppEngineHttpRequest(_)) => {
                return Err(Status::unimplemented(
                    "App Engine tasks are not supported yet",
                ));
            }
            None => return Err(Status::invalid_argument("Task.payload_type is not set.")),
        };
        let task_retry_config = task.retry_config.map(retry_config).transpose()?;
        let dispatch_deadline = match task.dispatch_deadline {
            None => Duration::from_secs(600),
            Some(deadline) => Duration::try_from(deadline)
                .ok()
                // Cloud Tasks keeps milliseconds
                .map(|deadline| Duration::from_millis(deadline.as_millis() as u64))
                .filter(|deadline| {
                    (Duration::from_secs(15)..=Duration::from_secs(30 * 60)).contains(deadline)
                })
                .ok_or_else(|| {
                    Status::invalid_argument("Task.dispatchDeadline must be between [15s, 30m].")
                })?,
        };
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

        let mut state = self.emulator.state.lock().unwrap();
        let queue = state
            .queues
            .get_mut(&request.parent)
            .ok_or_else(|| Status::not_found(QUEUE_NOT_FOUND))?;
        if queue.tasks.contains_key(&name) {
            return Err(Status::already_exists("Requested entity already exists"));
        }
        if queue.removed_tasks.contains(&name, now) {
            return Err(Status::already_exists(TASK_RECENTLY_REMOVED));
        }
        let task = Task::new(
            name,
            http_request,
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
        drop(state);
        self.emulator.wake.notify_one();
        Ok(Response::new(response))
    }

    async fn batch_create_tasks(
        &self,
        _: Request<pb::BatchCreateTasksRequest>,
    ) -> Result<Response<Operation>, Status> {
        Err(Status::unimplemented(
            "BatchCreateTasks is not supported yet",
        ))
    }

    async fn delete_task(
        &self,
        request: Request<pb::DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        resource(&request.name, names::TASK, "cloudtasks.tasks.delete")?;
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
        _: Request<pb::BatchDeleteTasksRequest>,
    ) -> Result<Response<Operation>, Status> {
        Err(Status::unimplemented(
            "BatchDeleteTasks is not supported yet",
        ))
    }

    async fn run_task(
        &self,
        request: Request<pb::RunTaskRequest>,
    ) -> Result<Response<pb::Task>, Status> {
        let request = request.into_inner();
        resource(&request.name, names::TASK, "cloudtasks.tasks.run")?;
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
        _: Request<pb::UpdateCmekConfigRequest>,
    ) -> Result<Response<pb::CmekConfig>, Status> {
        Err(Status::unimplemented(
            "UpdateCmekConfig is not supported yet",
        ))
    }

    async fn get_cmek_config(
        &self,
        _: Request<pb::GetCmekConfigRequest>,
    ) -> Result<Response<pb::CmekConfig>, Status> {
        Err(Status::unimplemented("GetCmekConfig is not supported yet"))
    }
}

/// Rejects a malformed name the way Google's front end does, then checks its IDs as Cloud Tasks does
fn resource(name: &str, collections: &[&str], permission: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument(
            "Invalid resource field value in the request.",
        ));
    }
    if !name.starts_with("projects/") {
        return Err(Status::permission_denied(format!(
            "Permission denied on resource project {name}."
        )));
    }
    let ids = names::ids(name, collections).ok_or_else(|| {
        Status::permission_denied(format!(
            r#"The principal (user or service account) lacks IAM permission "{permission}" for the resource "{name}" (or the resource may not exist)."#
        ))
    })?;
    check_ids(&ids)
}

/// Checks the queue and task IDs among a name's project, location, queue and task IDs
fn check_ids(ids: &[&str]) -> Result<(), Status> {
    if let Some(queue_id) = ids.get(2)
        && !names::is_queue_id(queue_id)
    {
        return Err(Status::invalid_argument(format!(
            "Queue ID \"{queue_id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Queue ID must be between 1 and 100 characters."
        )));
    }
    if let Some(task_id) = ids.get(3)
        && !names::is_task_id(task_id)
    {
        return Err(Status::invalid_argument(format!(
            "Task ID \"{task_id}\" can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Task ID must between 1 and 500 characters."
        )));
    }
    Ok(())
}

/// Copies the fields `paths` name from `update` onto `current`, ignoring output-only ones
fn masked(
    mut current: pb::Queue,
    update: pb::Queue,
    paths: &[String],
) -> Result<pb::Queue, Status> {
    let rate_limits = update.rate_limits.unwrap_or_default();
    let retry_config = update.retry_config.unwrap_or_default();
    for path in paths {
        match path.as_str() {
            "rate_limits" => current.rate_limits = update.rate_limits,
            "rate_limits.max_dispatches_per_second" => {
                current
                    .rate_limits
                    .get_or_insert_default()
                    .max_dispatches_per_second = rate_limits.max_dispatches_per_second;
            }
            "rate_limits.max_concurrent_dispatches" => {
                current
                    .rate_limits
                    .get_or_insert_default()
                    .max_concurrent_dispatches = rate_limits.max_concurrent_dispatches;
            }
            "retry_config" => current.retry_config = update.retry_config,
            "retry_config.max_attempts" => {
                current.retry_config.get_or_insert_default().max_attempts =
                    retry_config.max_attempts;
            }
            "retry_config.max_retry_duration" => {
                current
                    .retry_config
                    .get_or_insert_default()
                    .max_retry_duration = retry_config.max_retry_duration;
            }
            "retry_config.min_backoff" => {
                current.retry_config.get_or_insert_default().min_backoff = retry_config.min_backoff;
            }
            "retry_config.max_backoff" => {
                current.retry_config.get_or_insert_default().max_backoff = retry_config.max_backoff;
            }
            "retry_config.max_doublings" => {
                current.retry_config.get_or_insert_default().max_doublings =
                    retry_config.max_doublings;
            }
            // Output only, or fields of later layers that the request cannot set
            "name"
            | "state"
            | "purge_time"
            | "rate_limits.max_burst_size"
            | "app_engine_routing_override"
            | "http_target"
            | "stackdriver_logging_config" => {}
            _ => return Err(Status::invalid_argument(format!("Invalid mask: {path}"))),
        }
    }
    Ok(current)
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

/// Pages through names under `prefix`, using the last name returned as the page token
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
    let start = match page_token {
        "" => Bound::Included(prefix),
        token if token.starts_with(prefix) => Bound::Excluded(token),
        _ => return Err(Status::invalid_argument("Invalid page_token")),
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
        .map_or_else(String::new, |name| (*name).clone());
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
        .map(|header| authorization_header(header, &request.url))
        .transpose()?;
    let http_method = match pb::HttpMethod::try_from(request.http_method) {
        // Cloud Tasks stores an unknown method as POST
        Ok(pb::HttpMethod::Unspecified | pb::HttpMethod::Post) | Err(_) => HttpMethod::Post,
        Ok(pb::HttpMethod::Get) => HttpMethod::Get,
        Ok(pb::HttpMethod::Head) => HttpMethod::Head,
        Ok(pb::HttpMethod::Put) => HttpMethod::Put,
        Ok(pb::HttpMethod::Delete) => HttpMethod::Delete,
        Ok(pb::HttpMethod::Patch) => HttpMethod::Patch,
        Ok(pb::HttpMethod::Options) => HttpMethod::Options,
    };
    if !request.body.is_empty()
        && !matches!(
            http_method,
            HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch
        )
    {
        return Err(Status::invalid_argument(
            "Body can only be set if the http_method is POST, PUT, or PATCH.",
        ));
    }
    Ok(HttpRequest {
        url: request.url,
        http_method,
        headers: stored_headers(request.headers),
        body: request.body,
        authorization_header,
    })
}

/// Fills the defaults Cloud Tasks stores, but allows the http URLs it rejects so local handlers get tokens
fn authorization_header(
    header: pb::http_request::AuthorizationHeader,
    url: &str,
) -> Result<AuthorizationHeader, Status> {
    let service_account_email = match &header {
        pb::http_request::AuthorizationHeader::OauthToken(token) => &token.service_account_email,
        pb::http_request::AuthorizationHeader::OidcToken(token) => &token.service_account_email,
    };
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
    Ok(match header {
        pb::http_request::AuthorizationHeader::OauthToken(token) => {
            if !reqwest::Url::parse(url).is_ok_and(|url| {
                url.host_str()
                    .is_some_and(|host| host.ends_with(".googleapis.com"))
            }) {
                return Err(Status::invalid_argument(
                    r#"Invalid url for HttpRequest.oauth_token. The hostname in the url must end with ".googleapis.com""#,
                ));
            }
            AuthorizationHeader::OAuthToken {
                service_account_email: token.service_account_email,
                scope: if token.scope.is_empty() {
                    "https://www.googleapis.com/auth/cloud-platform".to_owned()
                } else {
                    token.scope
                },
            }
        }
        pb::http_request::AuthorizationHeader::OidcToken(token) => AuthorizationHeader::OidcToken {
            service_account_email: token.service_account_email,
            audience: if token.audience.is_empty() {
                url.to_owned()
            } else {
                token.audience
            },
        },
    })
}

/// Drops and rewrites headers as Cloud Tasks does when it stores a task, never rejecting one
fn stored_headers(headers: BTreeMap<String, String>) -> BTreeMap<String, String> {
    const DROPPED: [&str; 14] = [
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
    let mut stored: BTreeMap<String, String> = headers
        .into_iter()
        .filter_map(|(name, value)| {
            let lowercase = name.to_ascii_lowercase();
            // Cloud Tasks ends a value at NUL and replaces line breaks
            let value = value
                .split('\0')
                .next()
                .expect("split yields at least one part")
                .replace(['\r', '\n'], "_");
            let kept = !DROPPED.contains(&lowercase.as_str())
                && !lowercase.starts_with("x-google")
                && !lowercase.starts_with("x-appengine")
                && !name.is_empty()
                && name.chars().all(|c| c.is_ascii_graphic() && c != ':')
                && !value.is_empty()
                && value.chars().all(|c| c == '\t' || c == ' ' || c.is_ascii_graphic())
                // Keeps `name: value\r\n` within 8 KiB
                && name.len() + value.len() <= 8188;
            kept.then_some((name, value))
        })
        .collect();
    stored.insert("User-Agent".to_owned(), "Google-Cloud-Tasks".to_owned());
    stored
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
        ..Default::default()
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
    let request = &task.http_request;
    let http_method = match request.http_method {
        HttpMethod::Post => pb::HttpMethod::Post,
        HttpMethod::Get => pb::HttpMethod::Get,
        HttpMethod::Head => pb::HttpMethod::Head,
        HttpMethod::Put => pb::HttpMethod::Put,
        HttpMethod::Delete => pb::HttpMethod::Delete,
        HttpMethod::Patch => pb::HttpMethod::Patch,
        HttpMethod::Options => pb::HttpMethod::Options,
    };
    let create_time = task
        .create_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("times are after the epoch");
    pb::Task {
        name: task.name.clone(),
        message_type: Some(pb::task::MessageType::HttpRequest(pb::HttpRequest {
            url: request.url.clone(),
            http_method: http_method.into(),
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
                        scope,
                    }),
                    AuthorizationHeader::OidcToken {
                        service_account_email,
                        audience,
                    } => pb::http_request::AuthorizationHeader::OidcToken(pb::OidcToken {
                        service_account_email,
                        audience,
                    }),
                }),
        })),
        schedule_time: Some(timestamp(task.schedule_time)),
        create_time: Some(Timestamp {
            seconds: create_time.as_secs() as i64,
            nanos: 0,
        }),
        dispatch_deadline: Some(proto_duration(task.dispatch_deadline)),
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
