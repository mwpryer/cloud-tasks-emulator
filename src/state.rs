use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use tokio::sync::Notify;

use crate::names;

#[derive(Default)]
pub(crate) struct Emulator {
    pub state: Mutex<State>,
    /// Wakes the scheduler after any change that can make a task due sooner
    pub wake: Notify,
}

#[derive(Default)]
pub(crate) struct State {
    pub queues: BTreeMap<String, Queue>,
    next_attempt_id: u64,
}

pub(crate) struct Queue {
    pub name: String,
    pub rate_limits: RateLimits,
    pub retry_config: RetryConfig,
    pub tasks: BTreeMap<String, Task>,
    tokens: f64,
    refilled_at: SystemTime,
    in_flight: HashSet<u64>,
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

pub(crate) struct Task {
    pub name: String,
    pub http_request: HttpRequest,
    pub schedule_time: SystemTime,
    pub create_time: SystemTime,
    pub dispatch_deadline: Duration,
    pub dispatch_count: u32,
    /// Responses other than 5xx, which Cloud Tasks also sends as X-CloudTasks-TaskExecutionCount
    pub response_count: u32,
    /// Dispatch time only, as Cloud Tasks keeps nothing else of the first attempt
    pub first_attempt: Option<SystemTime>,
    pub last_attempt: Option<Attempt>,
    in_flight: Option<u64>,
}

pub(crate) struct HttpRequest {
    pub url: String,
    pub http_method: HttpMethod,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

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
    pub http_method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub deadline: Duration,
}

impl State {
    /// Starts every due attempt the queue limits allow and returns how long until one could next start
    pub fn start_due(&mut self, now: SystemTime) -> (Vec<Delivery>, Option<Duration>) {
        let mut deliveries = Vec::new();
        let mut wait: Option<Duration> = None;
        for queue in self.queues.values_mut() {
            let rate = queue.rate_limits.max_dispatches_per_second;
            let elapsed = now.duration_since(queue.refilled_at).unwrap_or_default();
            queue.tokens = (queue.tokens + elapsed.as_secs_f64() * rate)
                .min(queue.rate_limits.max_burst_size as f64);
            queue.refilled_at = now;

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
                deliveries.push(task.start_attempt(&queue.name, self.next_attempt_id, now));
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
        if matches!(outcome, Outcome::Status(200..=299))
            || queue
                .retry_config
                .exhausted(task.dispatch_count, retrying_for)
        {
            queue.tasks.remove(&delivery.task);
            return;
        }
        task.schedule_time = now + queue.retry_config.backoff(task.dispatch_count);
    }
}

impl Queue {
    pub fn new(
        name: String,
        rate_limits: RateLimits,
        retry_config: RetryConfig,
        now: SystemTime,
    ) -> Self {
        Self {
            name,
            rate_limits,
            retry_config,
            tasks: BTreeMap::new(),
            tokens: rate_limits.max_burst_size as f64,
            refilled_at: now,
            in_flight: HashSet::new(),
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
        http_request: HttpRequest,
        schedule_time: SystemTime,
        dispatch_deadline: Duration,
        now: SystemTime,
    ) -> Self {
        Self {
            name,
            http_request,
            schedule_time,
            create_time: now,
            dispatch_deadline,
            dispatch_count: 0,
            response_count: 0,
            first_attempt: None,
            last_attempt: None,
            in_flight: None,
        }
    }

    fn start_attempt(&mut self, queue: &str, attempt_id: u64, now: SystemTime) -> Delivery {
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
                // Shortest decimal for the float, so 1.120000 is 1.12
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
        // Injected headers replace stored ones of the same name
        let mut headers: Vec<(String, String)> = self
            .http_request
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
            http_method: self.http_request.http_method,
            url: self.http_request.url.clone(),
            headers,
            body: self.http_request.body.clone(),
            deadline: self.dispatch_deadline,
        }
    }
}
