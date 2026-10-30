use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use reqwest::Url;
use tokio::sync::Notify;

use crate::names;
use crate::proto::google::cloud::tasks::v2 as pb;
use crate::proto::google::iam::v1 as iam;
use crate::proto::google::longrunning::Operation;

#[derive(Default)]
pub(crate) struct Emulator {
    pub state: Mutex<State>,
    /// Wakes scheduler after any change that can make task due sooner
    pub wake: Notify,
}

pub(crate) struct State {
    pub queues: BTreeMap<String, Queue>,
    pub deleted_queues: Tombstones,
    /// Finished batch operations, kept as returned since nothing changes them
    pub operations: BTreeMap<String, Operation>,
    /// KMS key of each location with CMEK on, which emulator stores without encrypting
    pub cmek_keys: BTreeMap<String, String>,
    next_attempt_id: u64,
    /// Number of resets so far
    generation: u64,
}

pub(crate) struct Queue {
    pub name: String,
    pub config: QueueConfig,
    pub state: QueueState,
    pub purge_time: Option<SystemTime>,
    tasks: BTreeMap<String, Task>,
    /// Tasks not in flight, in order they dispatch
    waiting: BTreeSet<(SystemTime, String)>,
    /// Set by RunTask until scheduler starts attempt
    run_requested: BTreeSet<String>,
    /// Finished and deleted task names
    pub removed_tasks: Tombstones,
    /// Stored only, as emulator has no caller identity
    pub policy: iam::Policy,
    tokens: f64,
    refilled_at: SystemTime,
    in_flight: u32,
}

/// Settings that CreateQueue sets and UpdateQueue replaces
#[derive(Clone)]
pub(crate) struct QueueConfig {
    pub rate_limits: RateLimits,
    pub retry_config: RetryConfig,
    /// Applies to every task as it dispatches, including tasks created before it
    pub http_target: Option<HttpTarget>,
    /// Stored only, as emulator writes no operation logs
    pub log_sampling_ratio: f64,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum QueueState {
    Running,
    Paused,
}

/// Names removed recently enough that they cannot be reused yet
pub(crate) struct Tombstones {
    window: Duration,
    /// Oldest first, so expired names come off front
    removed: VecDeque<(SystemTime, String)>,
    names: HashSet<String>,
}

#[derive(Clone, Copy)]
pub(crate) struct RateLimits {
    pub max_dispatches_per_second: f64,
    pub max_burst_size: u32,
    pub max_concurrent_dispatches: u32,
}

#[derive(Clone, Copy)]
pub(crate) struct RetryConfig {
    /// None is unlimited
    pub max_attempts: Option<u32>,
    /// None is unlimited
    pub max_retry_duration: Option<Duration>,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub max_doublings: u32,
}

#[derive(Clone)]
pub(crate) struct HttpTarget {
    pub uri_override: Option<UriOverride>,
    pub http_method: Option<HttpMethod>,
    pub header_overrides: Vec<(String, String)>,
    /// Also copied into each task as it is created
    pub authorization_header: Option<AuthorizationHeader>,
}

/// Each part replaces that part of task URL, and empty path or query removes it
#[derive(Clone)]
pub(crate) struct UriOverride {
    /// Unspecified is HTTPS, as proto documents
    pub scheme: Option<pb::uri_override::Scheme>,
    pub host: Option<String>,
    /// Zero removes port
    pub port: Option<u64>,
    pub path: Option<String>,
    pub query: Option<String>,
    /// Unspecified applies override always
    pub enforce_mode: pb::uri_override::UriOverrideEnforceMode,
}

pub(crate) struct Task {
    pub name: String,
    pub http_request: HttpRequest,
    /// Overrides queue's
    pub retry_config: Option<RetryConfig>,
    pub schedule_time: SystemTime,
    pub create_time: SystemTime,
    pub dispatch_deadline: Duration,
    pub dispatch_count: u32,
    /// Responses other than 5xx, which Cloud Tasks also sends as X-CloudTasks-TaskExecutionCount
    pub response_count: u32,
    /// Dispatch time only, as Cloud Tasks keeps nothing else of first attempt
    pub first_attempt: Option<SystemTime>,
    pub last_attempt: Option<Attempt>,
}

pub(crate) struct HttpRequest {
    pub url: String,
    pub http_method: HttpMethod,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub authorization_header: Option<AuthorizationHeader>,
}

/// Token generated for each attempt, which replaces any stored Authorization header
#[derive(Clone)]
pub(crate) enum AuthorizationHeader {
    OAuthToken {
        service_account_email: String,
        /// None is `DEFAULT_SCOPE`
        scope: Option<String>,
    },
    OidcToken {
        service_account_email: String,
        /// None is task URL
        audience: Option<String>,
    },
}

pub(crate) const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[derive(Clone, Copy)]
pub(crate) enum HttpMethod {
    Post,
    Get,
    Head,
    Put,
    Delete,
    Patch,
    Options,
}

pub(crate) struct Attempt {
    /// Tells finished delivery whether RunTask has since started newer attempt
    pub id: u64,
    pub schedule_time: SystemTime,
    pub dispatch_time: SystemTime,
    /// None while in flight
    pub response: Option<AttemptResponse>,
}

pub(crate) struct AttemptResponse {
    pub time: SystemTime,
    pub outcome: Outcome,
}

#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    Status(u16),
    DeadlineExceeded,
    Unreachable,
}

impl Outcome {
    pub fn status(self) -> u16 {
        match self {
            Outcome::Status(status) => status,
            Outcome::DeadlineExceeded => 504,
            Outcome::Unreachable => 0,
        }
    }
}

pub(crate) struct Delivery {
    pub queue: String,
    pub task: String,
    pub attempt_id: u64,
    pub generation: u64,
    /// None when Cloud Tasks fails attempt without sending anything
    pub request: Option<Request>,
}

pub(crate) struct Request {
    pub http_method: HttpMethod,
    pub url: Url,
    pub headers: Vec<(String, String)>,
    pub token: Option<Token>,
    pub body: Vec<u8>,
    pub deadline: Duration,
}

pub(crate) enum Token {
    OAuth,
    Oidc {
        service_account_email: String,
        audience: String,
    },
}

impl Default for State {
    fn default() -> Self {
        Self {
            queues: BTreeMap::new(),
            deleted_queues: Tombstones::new(Duration::from_secs(3 * 24 * 60 * 60)),
            operations: BTreeMap::new(),
            cmek_keys: BTreeMap::new(),
            next_attempt_id: 0,
            generation: 0,
        }
    }
}

impl State {
    /// Takes contents of `fresh`, as restart would, so attempts in flight finish unrecorded
    pub fn reset(&mut self, fresh: State) {
        *self = State {
            generation: self.generation + 1,
            ..fresh
        };
    }

    /// Starts every due attempt that queue limits allow, and returns how long until one could next start
    pub fn start_due(&mut self, now: SystemTime) -> (Vec<Delivery>, Option<Duration>) {
        let mut deliveries = Vec::new();
        let mut wait: Option<Duration> = None;
        for queue in self.queues.values_mut() {
            let rate_limits = queue.config.rate_limits;
            let rate = rate_limits.max_dispatches_per_second;
            let elapsed = now.duration_since(queue.refilled_at).unwrap_or_default();
            queue.tokens = (queue.tokens + elapsed.as_secs_f64() * rate)
                .min(rate_limits.max_burst_size as f64);
            queue.refilled_at = now;

            // RunTask dispatches whatever queue's state, limits and task's schedule
            for name in std::mem::take(&mut queue.run_requested) {
                let task = queue
                    .tasks
                    .get_mut(&name)
                    .expect("removing task drops its run request");
                queue.waiting.remove(&(task.schedule_time, name));
                self.next_attempt_id += 1;
                queue.in_flight += 1;
                deliveries.push(task.start_attempt(
                    &queue.name,
                    queue.config.http_target.as_ref(),
                    self.next_attempt_id,
                    self.generation,
                    now,
                ));
            }
            // Resuming wakes scheduler, so paused queue needs no timer
            if queue.state == QueueState::Paused {
                continue;
            }

            while let Some((schedule_time, _)) = queue.waiting.first() {
                if let Ok(until_due) = schedule_time.duration_since(now)
                    && !until_due.is_zero()
                {
                    wait = Some(wait.map_or(until_due, |wait| wait.min(until_due)));
                    break;
                }
                // Finished attempt wakes scheduler, so full queue needs no timer
                if queue.in_flight >= rate_limits.max_concurrent_dispatches {
                    break;
                }
                if queue.tokens < 1.0 {
                    // Rate near zero puts next token past what Duration holds, so needs no timer
                    if let Ok(until_token) =
                        Duration::try_from_secs_f64((1.0 - queue.tokens) / rate)
                    {
                        wait = Some(wait.map_or(until_token, |wait| wait.min(until_token)));
                    }
                    break;
                }
                queue.tokens -= 1.0;
                let (_, name) = queue.waiting.pop_first().expect("first was checked");
                let task = queue
                    .tasks
                    .get_mut(&name)
                    .expect("removing task drops it from waiting");
                self.next_attempt_id += 1;
                queue.in_flight += 1;
                deliveries.push(task.start_attempt(
                    &queue.name,
                    queue.config.http_target.as_ref(),
                    self.next_attempt_id,
                    self.generation,
                    now,
                ));
            }
        }
        (deliveries, wait)
    }

    pub fn finish_attempt(&mut self, delivery: &Delivery, outcome: Outcome, now: SystemTime) {
        if delivery.generation != self.generation {
            return;
        }
        // Queue or task may have been deleted while attempt was in flight
        let Some(queue) = self.queues.get_mut(&delivery.queue) else {
            return;
        };
        queue.in_flight = queue
            .in_flight
            .checked_sub(1)
            .expect("deleted queue names stay blocked longer than attempts run");
        let Some(task) = queue.tasks.get_mut(&delivery.task) else {
            return;
        };
        let Some(attempt) = task
            .last_attempt
            .as_mut()
            .filter(|attempt| attempt.id == delivery.attempt_id)
        else {
            return;
        };

        attempt.response = Some(AttemptResponse { time: now, outcome });
        if let Outcome::Status(status) = outcome
            && !(500..600).contains(&status)
        {
            task.response_count += 1;
        }

        let first_attempt = task
            .first_attempt
            .expect("in-flight task has a first attempt");
        let retrying_for = now.duration_since(first_attempt).unwrap_or_default();
        let retry_config = task.retry_config.unwrap_or(queue.config.retry_config);
        if matches!(outcome, Outcome::Status(200..=299))
            || retry_config.exhausted(task.dispatch_count, retrying_for)
        {
            queue.remove_task(&delivery.task, now);
            return;
        }
        // Caps at end of 9999, last time Timestamp holds
        let last = SystemTime::UNIX_EPOCH + Duration::from_secs(253_402_300_799);
        task.schedule_time = now
            .checked_add(retry_config.backoff(task.dispatch_count))
            .unwrap_or(last)
            .min(last);
        queue
            .waiting
            .insert((task.schedule_time, delivery.task.clone()));
    }
}

impl Queue {
    pub fn new(name: String, config: QueueConfig, now: SystemTime) -> Self {
        Self {
            name,
            tokens: config.rate_limits.max_burst_size as f64,
            config,
            state: QueueState::Running,
            purge_time: None,
            tasks: BTreeMap::new(),
            waiting: BTreeSet::new(),
            run_requested: BTreeSet::new(),
            removed_tasks: Tombstones::new(Duration::from_secs(24 * 60 * 60)),
            policy: iam::Policy {
                // Etag of queue that never had policy, `ACAB` in base64
                etag: vec![0x00, 0x20, 0x01],
                ..Default::default()
            },
            refilled_at: now,
            in_flight: 0,
        }
    }

    pub fn tasks(&self) -> &BTreeMap<String, Task> {
        &self.tasks
    }

    pub fn insert_task(&mut self, task: Task) {
        self.waiting.insert((task.schedule_time, task.name.clone()));
        self.tasks.insert(task.name.clone(), task);
    }

    /// Asks scheduler to start attempt now
    pub fn run_task(&mut self, name: &str) -> Option<&Task> {
        let task = self.tasks.get(name)?;
        self.run_requested.insert(task.name.clone());
        Some(task)
    }

    pub fn remove_task(&mut self, name: &str, now: SystemTime) -> Option<Task> {
        let task = self.tasks.remove(name)?;
        self.run_requested.remove(name);
        self.waiting
            .remove(&(task.schedule_time, task.name.clone()));
        self.removed_tasks.insert(task.name.clone(), now);
        Some(task)
    }

    pub fn purge(&mut self, now: SystemTime) {
        self.purge_time = Some(now);
        self.waiting.clear();
        self.run_requested.clear();
        for name in std::mem::take(&mut self.tasks).into_keys() {
            self.removed_tasks.insert(name, now);
        }
    }
}

impl UriOverride {
    /// None when override leaves no valid URL
    fn apply(&self, url: &str) -> Option<Url> {
        let mut url = Url::parse(url).ok()?;
        // Task always has URL, so this mode never applies
        if self.enforce_mode == pb::uri_override::UriOverrideEnforceMode::IfNotExists {
            return Some(url);
        }
        if let Some(scheme) = self.scheme {
            url.set_scheme(match scheme {
                pb::uri_override::Scheme::Http => "http",
                pb::uri_override::Scheme::Unspecified | pb::uri_override::Scheme::Https => "https",
            })
            .expect("http and https convert to each other");
        }
        if let Some(host) = &self.host {
            url.set_host(Some(host))
                .expect("queue hosts are checked as they are set");
        }
        match self.port {
            None => {}
            Some(0) => url.set_port(None).expect("http URLs have ports"),
            Some(port) => url
                .set_port(Some(u16::try_from(port).ok()?))
                .expect("http URLs have ports"),
        }
        if let Some(path) = &self.path {
            url.set_path(path);
        }
        if let Some(query) = &self.query {
            url.set_query(
                Some(query)
                    .filter(|query| !query.is_empty())
                    .map(String::as_str),
            );
        }
        Some(url)
    }
}

impl HttpMethod {
    pub fn allows_body(self) -> bool {
        matches!(self, HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch)
    }
}

impl Tombstones {
    fn new(window: Duration) -> Self {
        Self {
            window,
            removed: VecDeque::new(),
            names: HashSet::new(),
        }
    }

    pub fn insert(&mut self, name: String, now: SystemTime) {
        self.prune(now);
        assert!(self.names.insert(name.clone()), "{name} is already blocked");
        self.removed.push_back((now, name));
    }

    pub fn contains(&mut self, name: &str, now: SystemTime) -> bool {
        self.prune(now);
        self.names.contains(name)
    }

    fn prune(&mut self, now: SystemTime) {
        while let Some((removed_at, _)) = self.removed.front()
            && *removed_at + self.window <= now
        {
            let (_, name) = self.removed.pop_front().expect("front was checked");
            self.names.remove(&name);
        }
    }
}

impl RetryConfig {
    /// Doubles `max_doublings` times, then grows linearly by last doubled interval
    fn backoff(&self, attempts: u32) -> Duration {
        let doublings = (attempts - 1).min(self.max_doublings);
        let doubled = self
            .min_backoff
            .saturating_mul(2u32.saturating_pow(doublings));
        doubled
            .saturating_mul(attempts - doublings)
            .min(self.max_backoff)
    }

    /// Both limits must be reached, and unlimited one counts as reached only when other is set
    fn exhausted(&self, attempts: u32, retrying_for: Duration) -> bool {
        if self.max_attempts.is_none() && self.max_retry_duration.is_none() {
            return false;
        }
        self.max_attempts.is_none_or(|max| attempts >= max)
            && self
                .max_retry_duration
                .is_none_or(|max| retrying_for >= max)
    }
}

impl Task {
    fn start_attempt(
        &mut self,
        queue: &str,
        http_target: Option<&HttpTarget>,
        attempt_id: u64,
        generation: u64,
        now: SystemTime,
    ) -> Delivery {
        let previous = self
            .last_attempt
            .as_ref()
            .and_then(|attempt| attempt.response.as_ref());
        let eta = self
            .schedule_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let mut injected = vec![
            (
                "X-CloudTasks-QueueName".to_owned(),
                names::last_segment(queue).to_owned(),
            ),
            (
                "X-CloudTasks-TaskName".to_owned(),
                names::last_segment(&self.name).to_owned(),
            ),
            (
                "X-CloudTasks-TaskRetryCount".to_owned(),
                self.dispatch_count.to_string(),
            ),
            (
                "X-CloudTasks-TaskExecutionCount".to_owned(),
                self.response_count.to_string(),
            ),
            (
                "X-CloudTasks-TaskETA".to_owned(),
                // Shortest decimal for float, so 1.120000 is 1.12
                eta.as_secs_f64().to_string(),
            ),
        ];
        if let Some(previous) = previous {
            injected.push((
                "X-CloudTasks-TaskPreviousResponse".to_owned(),
                previous.outcome.status().to_string(),
            ));
            injected.push(("X-CloudTasks-TaskRetryReason".to_owned(), String::new()));
        }
        let stored = &self.http_request;
        let authorization_header = http_target
            .and_then(|target| target.authorization_header.as_ref())
            .or(stored.authorization_header.as_ref());
        // Overrides, then injected headers, replace stored ones of same name
        let overrides = http_target.map_or(&[][..], |target| &target.header_overrides);
        let mut headers: Vec<(String, String)> = stored
            .headers
            .iter()
            .filter(|(name, _)| {
                !overrides
                    .iter()
                    .any(|(overridden, _)| overridden.eq_ignore_ascii_case(name))
            })
            .chain(overrides.iter().map(|(name, value)| (name, value)))
            .filter(|(name, _)| {
                !injected
                    .iter()
                    .any(|(injected, _)| injected.eq_ignore_ascii_case(name))
                    && !(authorization_header.is_some()
                        && name.eq_ignore_ascii_case("authorization"))
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        headers.extend(injected);
        let http_method = http_target
            .and_then(|target| target.http_method)
            .unwrap_or(stored.http_method);
        let url = match http_target.and_then(|target| target.uri_override.as_ref()) {
            Some(uri_override) => uri_override.apply(&stored.url),
            None => Url::parse(&stored.url).ok(),
        };
        let token = authorization_header.map(|header| match header {
            AuthorizationHeader::OAuthToken { .. } => Token::OAuth,
            AuthorizationHeader::OidcToken {
                service_account_email,
                audience,
            } => Token::Oidc {
                service_account_email: service_account_email.clone(),
                audience: audience.clone().unwrap_or_else(|| stored.url.clone()),
            },
        });
        // Cloud Tasks fails method override that disallows body, though proto says it drops it
        let request = url
            .filter(|_| stored.body.is_empty() || http_method.allows_body())
            .map(|url| Request {
                http_method,
                url,
                headers,
                token,
                body: stored.body.clone(),
                deadline: self.dispatch_deadline,
            });

        self.dispatch_count += 1;
        self.first_attempt.get_or_insert(now);
        self.last_attempt = Some(Attempt {
            id: attempt_id,
            schedule_time: self.schedule_time,
            dispatch_time: now,
            response: None,
        });
        Delivery {
            queue: queue.to_owned(),
            task: self.name.clone(),
            attempt_id,
            generation,
            request,
        }
    }
}
