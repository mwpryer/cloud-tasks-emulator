use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use reqwest::Url;
use tokio::sync::Notify;

use crate::names;

pub(crate) struct Emulator {
    pub state: Mutex<State>,
    /// Wakes the scheduler after any change that can make a task due sooner
    pub wake: Notify,
    /// Origin that every App Engine task goes to, or None when no project has an App Engine app
    pub app_engine: Option<String>,
}

pub(crate) struct State {
    pub queues: BTreeMap<String, Queue>,
    /// Deleted queue names, blocked for the 3 days the proto documents
    pub deleted_queues: Tombstones,
    next_attempt_id: u64,
}

pub(crate) struct Queue {
    pub name: String,
    pub rate_limits: RateLimits,
    pub retry_config: RetryConfig,
    /// Applies to every HTTP task as it dispatches, including tasks created before it
    pub http_target: Option<HttpTarget>,
    pub app_engine_routing_override: Option<AppEngineRouting>,
    /// Stored only, as the emulator writes no operation logs
    pub log_sampling_ratio: f64,
    pub state: QueueState,
    pub purge_time: Option<SystemTime>,
    pub tasks: BTreeMap<String, Task>,
    /// Finished and deleted task names, blocked for the 24 hours the proto documents
    pub removed_tasks: Tombstones,
    tokens: f64,
    refilled_at: SystemTime,
    in_flight: HashSet<u64>,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum QueueState {
    Running,
    Paused,
}

/// Names removed recently enough that they cannot be reused yet
pub(crate) struct Tombstones {
    window: Duration,
    /// Oldest first, so expired names come off the front
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

/// Each part replaces that part of the task URL, and an empty port, path or query removes it
#[derive(Clone)]
pub(crate) struct UriOverride {
    pub scheme: Option<Scheme>,
    pub host: Option<String>,
    /// Zero removes the port
    pub port: Option<u64>,
    pub path: Option<String>,
    pub query: Option<String>,
    /// None applies the override always
    pub enforce_mode: Option<EnforceMode>,
}

#[derive(Clone, Copy)]
pub(crate) enum Scheme {
    /// HTTPS, as the proto documents
    Unspecified,
    Http,
    Https,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum EnforceMode {
    IfNotExists,
    Always,
}

pub(crate) struct Task {
    pub name: String,
    pub payload: Payload,
    /// Overrides the queue's
    pub retry_config: Option<RetryConfig>,
    pub schedule_time: SystemTime,
    pub create_time: SystemTime,
    /// None is `DEFAULT_DEADLINE`
    pub dispatch_deadline: Option<Duration>,
    pub dispatch_count: u32,
    /// Responses other than 5xx, which Cloud Tasks also sends as X-CloudTasks-TaskExecutionCount
    pub response_count: u32,
    /// Dispatch time only, as Cloud Tasks keeps nothing else of the first attempt
    pub first_attempt: Option<SystemTime>,
    pub last_attempt: Option<Attempt>,
    /// Set by RunTask until the scheduler starts the attempt
    pub run_requested: bool,
    in_flight: Option<u64>,
}

pub(crate) enum Payload {
    Http(HttpRequest),
    AppEngine(AppEngineHttpRequest),
}

pub(crate) struct HttpRequest {
    pub url: String,
    pub http_method: HttpMethod,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub authorization_header: Option<AuthorizationHeader>,
}

pub(crate) struct AppEngineHttpRequest {
    pub http_method: HttpMethod,
    pub routing: AppEngineRouting,
    pub relative_uri: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// Decides only the reported host, as every App Engine task goes to one origin
#[derive(Clone)]
pub(crate) struct AppEngineRouting {
    pub service: Option<String>,
    pub version: Option<String>,
    pub instance: Option<String>,
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
        /// None is the task URL
        audience: Option<String>,
    },
}

pub(crate) const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
/// For HTTP tasks, and for App Engine apps with automatic scaling
pub(crate) const DEFAULT_DEADLINE: Duration = Duration::from_secs(10 * 60);

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
    pub schedule_time: SystemTime,
    pub dispatch_time: SystemTime,
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
    /// The status Cloud Tasks reports, which it sets to 504 after a timeout and 0 without a connection
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
    /// None when Cloud Tasks fails the attempt without sending anything
    pub request: Option<Request>,
}

pub(crate) struct Request {
    pub http_method: HttpMethod,
    pub url: Url,
    pub headers: Vec<(String, String)>,
    pub token: Option<Token>,
    pub body: Vec<u8>,
    pub deadline: Duration,
    pub follows_redirects: bool,
}

pub(crate) enum Token {
    OAuth,
    Oidc {
        service_account_email: String,
        audience: String,
    },
}

impl Emulator {
    pub fn new(app_engine: Option<String>) -> Self {
        Self {
            state: Mutex::default(),
            wake: Notify::new(),
            app_engine,
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            queues: BTreeMap::new(),
            deleted_queues: Tombstones::new(Duration::from_secs(3 * 24 * 60 * 60)),
            next_attempt_id: 0,
        }
    }
}

impl State {
    /// Starts every due attempt the queue limits allow and returns how long until one could next start
    pub fn start_due(
        &mut self,
        app_engine: Option<&str>,
        now: SystemTime,
    ) -> (Vec<Delivery>, Option<Duration>) {
        let mut deliveries = Vec::new();
        let mut wait: Option<Duration> = None;
        for queue in self.queues.values_mut() {
            let rate = queue.rate_limits.max_dispatches_per_second;
            let elapsed = now.duration_since(queue.refilled_at).unwrap_or_default();
            queue.tokens = (queue.tokens + elapsed.as_secs_f64() * rate)
                .min(queue.rate_limits.max_burst_size as f64);
            queue.refilled_at = now;

            // RunTask dispatches whatever the queue's state, limits and the task's schedule
            for task in queue.tasks.values_mut().filter(|task| task.run_requested) {
                task.run_requested = false;
                self.next_attempt_id += 1;
                queue.in_flight.insert(self.next_attempt_id);
                deliveries.push(task.start_attempt(
                    &queue.name,
                    queue.http_target.as_ref(),
                    app_engine,
                    self.next_attempt_id,
                    now,
                ));
            }
            // Resuming wakes the scheduler, so a paused queue needs no timer
            if queue.state == QueueState::Paused {
                continue;
            }

            let mut waiting: Vec<&mut Task> = queue
                .tasks
                .values_mut()
                .filter(|task| task.in_flight.is_none())
                .collect();
            waiting.sort_by_key(|task| task.schedule_time);
            for task in waiting {
                if let Ok(until_due) = task.schedule_time.duration_since(now)
                    && !until_due.is_zero()
                {
                    wait = Some(wait.map_or(until_due, |wait| wait.min(until_due)));
                    break;
                }
                // A finishing attempt wakes the scheduler, so a full queue needs no timer
                if queue.in_flight.len() >= queue.rate_limits.max_concurrent_dispatches as usize {
                    break;
                }
                if queue.tokens < 1.0 {
                    let until_token = Duration::from_secs_f64((1.0 - queue.tokens) / rate);
                    wait = Some(wait.map_or(until_token, |wait| wait.min(until_token)));
                    break;
                }
                queue.tokens -= 1.0;
                self.next_attempt_id += 1;
                queue.in_flight.insert(self.next_attempt_id);
                deliveries.push(task.start_attempt(
                    &queue.name,
                    queue.http_target.as_ref(),
                    app_engine,
                    self.next_attempt_id,
                    now,
                ));
            }
        }
        (deliveries, wait)
    }

    pub fn finish_attempt(&mut self, delivery: &Delivery, outcome: Outcome, now: SystemTime) {
        // The queue or task may have been deleted while the attempt was in flight
        let Some(queue) = self.queues.get_mut(&delivery.queue) else {
            return;
        };
        if !queue.in_flight.remove(&delivery.attempt_id) {
            return;
        }
        let Some(task) = queue
            .tasks
            .get_mut(&delivery.task)
            .filter(|task| task.in_flight == Some(delivery.attempt_id))
        else {
            return;
        };

        task.in_flight = None;
        if let Outcome::Status(status) = outcome
            && !(500..600).contains(&status)
        {
            task.response_count += 1;
        }
        let attempt = task
            .last_attempt
            .as_mut()
            .expect("in-flight task has an attempt");
        attempt.response = Some(AttemptResponse { time: now, outcome });

        let first_attempt = task
            .first_attempt
            .expect("in-flight task has a first attempt");
        let retrying_for = now.duration_since(first_attempt).unwrap_or_default();
        let retry_config = task.retry_config.unwrap_or(queue.retry_config);
        if matches!(outcome, Outcome::Status(200..=299))
            || retry_config.exhausted(task.dispatch_count, retrying_for)
        {
            queue.remove_task(&delivery.task, now);
            return;
        }
        task.schedule_time = now + retry_config.backoff(task.dispatch_count);
    }
}

impl Queue {
    pub fn new(
        name: String,
        rate_limits: RateLimits,
        retry_config: RetryConfig,
        http_target: Option<HttpTarget>,
        app_engine_routing_override: Option<AppEngineRouting>,
        log_sampling_ratio: f64,
        now: SystemTime,
    ) -> Self {
        Self {
            name,
            rate_limits,
            retry_config,
            http_target,
            app_engine_routing_override,
            log_sampling_ratio,
            state: QueueState::Running,
            purge_time: None,
            tasks: BTreeMap::new(),
            removed_tasks: Tombstones::new(Duration::from_secs(24 * 60 * 60)),
            tokens: rate_limits.max_burst_size as f64,
            refilled_at: now,
            in_flight: HashSet::new(),
        }
    }

    /// Removes the task and blocks its name from reuse
    pub fn remove_task(&mut self, name: &str, now: SystemTime) -> Option<Task> {
        let task = self.tasks.remove(name)?;
        self.removed_tasks.insert(task.name.clone(), now);
        Some(task)
    }

    pub fn purge(&mut self, now: SystemTime) {
        self.purge_time = Some(now);
        for name in std::mem::take(&mut self.tasks).into_keys() {
            self.removed_tasks.insert(name, now);
        }
    }
}

impl UriOverride {
    /// None when the override leaves no valid URL
    fn apply(&self, url: &str) -> Option<Url> {
        let mut url = Url::parse(url).ok()?;
        // A task always has a URL, so this mode never applies
        if self.enforce_mode == Some(EnforceMode::IfNotExists) {
            return Some(url);
        }
        if let Some(scheme) = self.scheme {
            url.set_scheme(match scheme {
                Scheme::Http => "http",
                Scheme::Unspecified | Scheme::Https => "https",
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
    /// Doubles `max_doublings` times, then grows linearly by the last doubled interval
    fn backoff(&self, attempts: u32) -> Duration {
        let doublings = (attempts - 1).min(self.max_doublings);
        let doubled = self
            .min_backoff
            .saturating_mul(2u32.saturating_pow(doublings));
        doubled
            .saturating_mul(attempts - doublings)
            .min(self.max_backoff)
    }

    /// Both limits must be reached, and an unlimited one counts as reached only when the other is set
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
    pub fn new(
        name: String,
        payload: Payload,
        retry_config: Option<RetryConfig>,
        schedule_time: SystemTime,
        dispatch_deadline: Option<Duration>,
        now: SystemTime,
    ) -> Self {
        Self {
            name,
            payload,
            retry_config,
            schedule_time,
            create_time: now,
            dispatch_deadline,
            dispatch_count: 0,
            response_count: 0,
            first_attempt: None,
            last_attempt: None,
            run_requested: false,
            in_flight: None,
        }
    }

    fn start_attempt(
        &mut self,
        queue: &str,
        http_target: Option<&HttpTarget>,
        app_engine: Option<&str>,
        attempt_id: u64,
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
        // Named X-CloudTasks-* for HTTP tasks and X-AppEngine-* for App Engine tasks
        let injected = |prefix: &str| {
            let mut injected = vec![
                (
                    format!("{prefix}QueueName"),
                    names::last_segment(queue).to_owned(),
                ),
                (
                    format!("{prefix}TaskName"),
                    names::last_segment(&self.name).to_owned(),
                ),
                (
                    format!("{prefix}TaskRetryCount"),
                    self.dispatch_count.to_string(),
                ),
                (
                    format!("{prefix}TaskExecutionCount"),
                    self.response_count.to_string(),
                ),
                (
                    format!("{prefix}TaskETA"),
                    // Shortest decimal for the float, so 1.120000 is 1.12
                    eta.as_secs_f64().to_string(),
                ),
            ];
            if let Some(previous) = previous {
                injected.push((
                    format!("{prefix}TaskPreviousResponse"),
                    previous.outcome.status().to_string(),
                ));
                injected.push((format!("{prefix}TaskRetryReason"), String::new()));
            }
            injected
        };
        let request = match &self.payload {
            Payload::Http(stored) => {
                let injected = injected("X-CloudTasks-");
                let authorization_header = http_target
                    .and_then(|target| target.authorization_header.as_ref())
                    .or(stored.authorization_header.as_ref());
                // Overrides, then injected headers, replace stored ones of the same name
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
                // Cloud Tasks fails a method override that disallows the body, though the proto says it drops it
                url.filter(|_| stored.body.is_empty() || http_method.allows_body())
                    .map(|url| Request {
                        http_method,
                        url,
                        headers,
                        token,
                        body: stored.body.clone(),
                        deadline: self.dispatch_deadline.unwrap_or(DEFAULT_DEADLINE),
                        follows_redirects: true,
                    })
            }
            Payload::AppEngine(stored) => {
                let injected = injected("X-AppEngine-");
                let mut headers: Vec<(String, String)> = stored
                    .headers
                    .iter()
                    .filter(|(name, _)| {
                        !injected
                            .iter()
                            .any(|(injected, _)| injected.eq_ignore_ascii_case(name))
                    })
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect();
                headers.extend(injected);
                let origin =
                    app_engine.expect("App Engine tasks exist only with an App Engine URL");
                // Joined as text, so a relative URI that starts with // stays a path
                Url::parse(&format!("{origin}{}", stored.relative_uri))
                    .ok()
                    .map(|url| Request {
                        http_method: stored.http_method,
                        url,
                        headers,
                        token: None,
                        body: stored.body.clone(),
                        deadline: self.dispatch_deadline.unwrap_or(DEFAULT_DEADLINE),
                        follows_redirects: false,
                    })
            }
        };

        self.dispatch_count += 1;
        self.first_attempt.get_or_insert(now);
        self.last_attempt = Some(Attempt {
            schedule_time: self.schedule_time,
            dispatch_time: now,
            response: None,
        });
        self.in_flight = Some(attempt_id);
        Delivery {
            queue: queue.to_owned(),
            task: self.name.clone(),
            attempt_id,
            request,
        }
    }
}
