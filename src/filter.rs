//! ListQueues filters, in syntax of https://google.aip.dev/160

use std::cmp::Ordering;
use std::iter::Peekable;
use std::str::Chars;

use tonic::Status;

use crate::proto::google::cloud::tasks::v2 as pb;

pub(crate) enum Expression {
    All(Vec<Expression>),
    Any(Vec<Expression>),
    Not(Box<Expression>),
    Present(String),
    Contains(String, String),
    Compare(String, Comparator, Scalar),
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Comparator {
    Has,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(PartialEq, PartialOrd)]
pub(crate) enum Scalar {
    Number(f64),
    Nanos(i128),
}

enum Value<'a> {
    Text(&'a str),
    /// Field of each element of repeated message
    Texts(Vec<&'a str>),
    /// With parser for its value names
    Enum(i32, fn(&str) -> Option<i32>),
    Number(f64),
    Duration(Option<prost_types::Duration>),
    Time(Option<prost_types::Timestamp>),
    Message(bool),
}

#[derive(PartialEq)]
enum Token {
    Text(String),
    Quoted(String),
    Comparator(Comparator),
    Open,
    Close,
    End,
}

pub(crate) fn parse(filter: &str) -> Result<Expression, Status> {
    let mut parser = Parser {
        tokens: tokens(filter)?,
        next: 0,
    };
    if parser.peek() == &Token::End {
        return Ok(Expression::All(Vec::new()));
    }
    let expression = parser.expression()?;
    parser.expect(Token::End)?;
    Ok(expression)
}

impl Expression {
    pub fn matches(&self, queue: &pb::Queue) -> bool {
        match self {
            Expression::All(expressions) => expressions.iter().all(|e| e.matches(queue)),
            Expression::Any(expressions) => expressions.iter().any(|e| e.matches(queue)),
            Expression::Not(expression) => !expression.matches(queue),
            Expression::Present(path) => match value(queue, path).expect("resolved at parse") {
                Value::Text(text) => !text.is_empty(),
                Value::Texts(texts) => texts.iter().any(|text| !text.is_empty()),
                Value::Enum(number, _) => number != 0,
                Value::Number(number) => number != 0.0,
                Value::Duration(duration) => duration.is_some(),
                Value::Time(time) => time.is_some(),
                Value::Message(present) => present,
            },
            Expression::Contains(path, needle) => {
                match value(queue, path).expect("resolved at parse") {
                    Value::Text(text) => text.to_lowercase().contains(needle),
                    Value::Texts(texts) => texts
                        .iter()
                        .any(|text| text.to_lowercase().contains(needle)),
                    _ => unreachable!("only text fields take a substring"),
                }
            }
            Expression::Compare(path, comparator, operand) => {
                // Unset durations and times compare as zero, as in Cloud Tasks
                let field = match value(queue, path).expect("resolved at parse") {
                    Value::Enum(number, _) => Scalar::Number(number.into()),
                    Value::Number(number) => Scalar::Number(number),
                    Value::Duration(duration) => Scalar::Nanos(duration.map_or(0, |duration| {
                        i128::from(duration.seconds) * 1_000_000_000 + i128::from(duration.nanos)
                    })),
                    Value::Time(time) => Scalar::Nanos(time.map_or(0, |time| {
                        i128::from(time.seconds) * 1_000_000_000 + i128::from(time.nanos)
                    })),
                    Value::Text(_) | Value::Texts(_) | Value::Message(_) => {
                        unreachable!("text and message fields take no comparison")
                    }
                };
                let ordering = field.partial_cmp(operand);
                match comparator {
                    Comparator::Has | Comparator::Equal => ordering == Some(Ordering::Equal),
                    Comparator::NotEqual => ordering != Some(Ordering::Equal),
                    Comparator::Less => ordering == Some(Ordering::Less),
                    Comparator::LessOrEqual => ordering.is_some_and(Ordering::is_le),
                    Comparator::Greater => ordering == Some(Ordering::Greater),
                    Comparator::GreaterOrEqual => ordering.is_some_and(Ordering::is_ge),
                }
            }
        }
    }
}

/// Looks up path in lower case without underscores, so snake and camel case both match
fn value<'a>(queue: &'a pb::Queue, path: &str) -> Option<Value<'a>> {
    use pb::http_target::AuthorizationHeader;
    let rate_limits = queue.rate_limits.unwrap_or_default();
    let retry_config = queue.retry_config.unwrap_or_default();
    let routing = queue.app_engine_routing_override.as_ref();
    let http_target = queue.http_target.as_ref();
    let uri = http_target.and_then(|target| target.uri_override.as_ref());
    let path_override = uri.and_then(|uri| uri.path_override.as_ref());
    let query_override = uri.and_then(|uri| uri.query_override.as_ref());
    let headers: Vec<&pb::http_target::Header> = http_target.map_or_else(Vec::new, |target| {
        target
            .header_overrides
            .iter()
            .filter_map(|header_override| header_override.header.as_ref())
            .collect()
    });
    let (oauth, oidc) = match http_target.and_then(|target| target.authorization_header.as_ref()) {
        Some(AuthorizationHeader::OauthToken(token)) => (Some(token), None),
        Some(AuthorizationHeader::OidcToken(token)) => (None, Some(token)),
        None => (None, None),
    };
    Some(match path {
        "name" => Value::Text(&queue.name),
        "state" => Value::Enum(queue.state, |name| {
            pb::queue::State::from_str_name(name).map(|state| state as i32)
        }),
        "ratelimits" => Value::Message(queue.rate_limits.is_some()),
        "ratelimits.maxdispatchespersecond" => Value::Number(rate_limits.max_dispatches_per_second),
        "ratelimits.maxburstsize" => Value::Number(rate_limits.max_burst_size.into()),
        "ratelimits.maxconcurrentdispatches" => {
            Value::Number(rate_limits.max_concurrent_dispatches.into())
        }
        "retryconfig" => Value::Message(queue.retry_config.is_some()),
        "retryconfig.maxattempts" => Value::Number(retry_config.max_attempts.into()),
        "retryconfig.maxretryduration" => Value::Duration(retry_config.max_retry_duration),
        "retryconfig.minbackoff" => Value::Duration(retry_config.min_backoff),
        "retryconfig.maxbackoff" => Value::Duration(retry_config.max_backoff),
        "retryconfig.maxdoublings" => Value::Number(retry_config.max_doublings.into()),
        "purgetime" => Value::Time(queue.purge_time),
        "appengineroutingoverride" => Value::Message(routing.is_some()),
        "appengineroutingoverride.service" => Value::Text(routing.map_or("", |r| &r.service)),
        "appengineroutingoverride.version" => Value::Text(routing.map_or("", |r| &r.version)),
        "appengineroutingoverride.instance" => Value::Text(routing.map_or("", |r| &r.instance)),
        "appengineroutingoverride.host" => Value::Text(routing.map_or("", |r| &r.host)),
        "httptarget" => Value::Message(http_target.is_some()),
        "httptarget.urioverride" => Value::Message(uri.is_some()),
        "httptarget.urioverride.scheme" => {
            Value::Enum(uri.and_then(|uri| uri.scheme).unwrap_or(0), |name| {
                pb::uri_override::Scheme::from_str_name(name).map(|scheme| scheme as i32)
            })
        }
        "httptarget.urioverride.host" => {
            Value::Text(uri.and_then(|uri| uri.host.as_deref()).unwrap_or(""))
        }
        "httptarget.urioverride.port" => {
            Value::Number(uri.and_then(|uri| uri.port).unwrap_or(0) as f64)
        }
        "httptarget.urioverride.pathoverride" => Value::Message(path_override.is_some()),
        "httptarget.urioverride.pathoverride.path" => {
            Value::Text(path_override.map_or("", |path| &path.path))
        }
        "httptarget.urioverride.queryoverride" => Value::Message(query_override.is_some()),
        "httptarget.urioverride.queryoverride.queryparams" => {
            Value::Text(query_override.map_or("", |query| &query.query_params))
        }
        "httptarget.urioverride.urioverrideenforcemode" => {
            Value::Enum(uri.map_or(0, |uri| uri.uri_override_enforce_mode), |name| {
                pb::uri_override::UriOverrideEnforceMode::from_str_name(name)
                    .map(|mode| mode as i32)
            })
        }
        "httptarget.httpmethod" => {
            Value::Enum(http_target.map_or(0, |target| target.http_method), |name| {
                pb::HttpMethod::from_str_name(name).map(|method| method as i32)
            })
        }
        "httptarget.headeroverrides" => {
            Value::Message(http_target.is_some_and(|target| !target.header_overrides.is_empty()))
        }
        "httptarget.headeroverrides.header" => Value::Message(!headers.is_empty()),
        "httptarget.headeroverrides.header.key" => {
            Value::Texts(headers.iter().map(|header| header.key.as_str()).collect())
        }
        "httptarget.headeroverrides.header.value" => {
            Value::Texts(headers.iter().map(|header| header.value.as_str()).collect())
        }
        "httptarget.oauthtoken" => Value::Message(oauth.is_some()),
        "httptarget.oauthtoken.serviceaccountemail" => {
            Value::Text(oauth.map_or("", |token| &token.service_account_email))
        }
        "httptarget.oauthtoken.scope" => Value::Text(oauth.map_or("", |token| &token.scope)),
        "httptarget.oidctoken" => Value::Message(oidc.is_some()),
        "httptarget.oidctoken.serviceaccountemail" => {
            Value::Text(oidc.map_or("", |token| &token.service_account_email))
        }
        "httptarget.oidctoken.audience" => Value::Text(oidc.map_or("", |token| &token.audience)),
        "stackdriverloggingconfig" => Value::Message(queue.stackdriver_logging_config.is_some()),
        "stackdriverloggingconfig.samplingratio" => Value::Number(
            queue
                .stackdriver_logging_config
                .map_or(0.0, |config| config.sampling_ratio),
        ),
        _ => return None,
    })
}

fn restriction(field: &str, comparator: Comparator, operand: &str) -> Result<Expression, Status> {
    let path = field.to_lowercase().replace('_', "");
    // Types operand by field's value in empty queue
    let empty = pb::Queue::default();
    let kind = value(&empty, &path)
        .ok_or_else(|| Status::invalid_argument(format!("Field not found: '{field}'.")))?;
    if comparator == Comparator::Has && operand == "*" {
        return Ok(Expression::Present(path));
    }
    let incorrect_type = || {
        Status::invalid_argument(format!(
            "The value '{}' is of incorrect type.",
            operand.to_lowercase()
        ))
    };
    let scalar = match kind {
        Value::Message(_) => {
            return Err(Status::invalid_argument(format!(
                "Cannot match a nested type '{field}'. A member field of scalar or enum type must be provided."
            )));
        }
        Value::Text(_) | Value::Texts(_) if comparator == Comparator::Has => {
            return Ok(Expression::Contains(path, operand.to_lowercase()));
        }
        Value::Text(_) | Value::Texts(_) => {
            return Err(Status::unimplemented(
                "ListQueues filter text comparisons other than ':' is not supported yet",
            ));
        }
        Value::Enum(_, parse) => parse(&operand.to_uppercase())
            .map(|number| Scalar::Number(number.into()))
            .ok_or_else(incorrect_type)?,
        Value::Number(_) => Scalar::Number(operand.parse().map_err(|_| incorrect_type())?),
        Value::Duration(_) => {
            let seconds: f64 = operand
                .strip_suffix('s')
                .and_then(|seconds| seconds.parse().ok())
                .ok_or_else(incorrect_type)?;
            Scalar::Nanos((seconds * 1e9).round() as i128)
        }
        Value::Time(_) => {
            let time: jiff::Timestamp = operand.parse().map_err(|_| incorrect_type())?;
            Scalar::Nanos(time.as_nanosecond())
        }
    };
    Ok(Expression::Compare(path, comparator, scalar))
}

/// For token where parsing failed, `<EOF>` at end
fn syntax_error(token: &str) -> Status {
    Status::invalid_argument(format!(
        "Failed to parse filter expression: syntax error at token '{token}'"
    ))
}

fn tokens(filter: &str) -> Result<Vec<Token>, Status> {
    let mut tokens = Vec::new();
    let mut chars = filter.chars().peekable();
    while let Some(c) = chars.next() {
        let or_equal = |chars: &mut Peekable<Chars>| chars.next_if_eq(&'=').is_some();
        let token = match c {
            c if c.is_whitespace() => continue,
            '(' => Token::Open,
            ')' => Token::Close,
            ':' => Token::Comparator(Comparator::Has),
            '=' => Token::Comparator(Comparator::Equal),
            '<' if or_equal(&mut chars) => Token::Comparator(Comparator::LessOrEqual),
            '<' => Token::Comparator(Comparator::Less),
            '>' if or_equal(&mut chars) => Token::Comparator(Comparator::GreaterOrEqual),
            '>' => Token::Comparator(Comparator::Greater),
            '!' if or_equal(&mut chars) => Token::Comparator(Comparator::NotEqual),
            '"' => {
                let mut text = String::new();
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => text.extend(chars.next()),
                        Some(c) => text.push(c),
                        None => return Err(syntax_error("<EOF>")),
                    }
                }
                Token::Quoted(text)
            }
            c if is_symbol(c) => return Err(syntax_error(&c.to_string())),
            c => {
                let mut text = c.to_string();
                while let Some(c) = chars.next_if(|&c| !c.is_whitespace() && !is_symbol(c)) {
                    text.push(c);
                }
                Token::Text(text)
            }
        };
        tokens.push(token);
    }
    tokens.push(Token::End);
    Ok(tokens)
}

fn is_symbol(c: char) -> bool {
    "()\":=<>!~".contains(c)
}

struct Parser {
    tokens: Vec<Token>,
    next: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.next]
    }

    fn take(&mut self) -> Token {
        let token = std::mem::replace(&mut self.tokens[self.next], Token::End);
        self.next = (self.next + 1).min(self.tokens.len() - 1);
        token
    }

    fn keyword(&mut self, keyword: &str) -> bool {
        let found = matches!(self.peek(), Token::Text(text) if text == keyword);
        if found {
            self.take();
        }
        found
    }

    fn expect(&mut self, expected: Token) -> Result<(), Status> {
        if *self.peek() == expected {
            self.take();
            Ok(())
        } else {
            Err(self.unexpected())
        }
    }

    fn unexpected(&self) -> Status {
        syntax_error(&match self.peek() {
            Token::Text(text) => text.clone(),
            Token::Quoted(text) => format!("\"{text}\""),
            Token::Comparator(comparator) => match comparator {
                Comparator::Has => ":",
                Comparator::Equal => "=",
                Comparator::NotEqual => "!=",
                Comparator::Less => "<",
                Comparator::LessOrEqual => "<=",
                Comparator::Greater => ">",
                Comparator::GreaterOrEqual => ">=",
            }
            .to_owned(),
            Token::Open => "(".to_owned(),
            Token::Close => ")".to_owned(),
            Token::End => "<EOF>".to_owned(),
        })
    }

    /// Sequences joined by AND, which binds looser than OR
    fn expression(&mut self) -> Result<Expression, Status> {
        let mut sequences = vec![self.sequence()?];
        while self.keyword("AND") {
            sequences.push(self.sequence()?);
        }
        Ok(Expression::All(sequences))
    }

    /// Factors side by side, which AIP-160 reads as AND
    fn sequence(&mut self) -> Result<Expression, Status> {
        let mut factors = vec![self.factor()?];
        while match self.peek() {
            Token::Text(text) => text != "AND" && text != "OR",
            Token::Quoted(_) | Token::Open => true,
            Token::Comparator(_) | Token::Close | Token::End => false,
        } {
            factors.push(self.factor()?);
        }
        Ok(Expression::All(factors))
    }

    fn factor(&mut self) -> Result<Expression, Status> {
        let mut terms = vec![self.term()?];
        while self.keyword("OR") {
            terms.push(self.term()?);
        }
        Ok(Expression::Any(terms))
    }

    fn term(&mut self) -> Result<Expression, Status> {
        if self.keyword("NOT") {
            return Ok(Expression::Not(Box::new(self.simple()?)));
        }
        if let Token::Text(text) = &mut self.tokens[self.next]
            && text.len() > 1
            && text.starts_with('-')
        {
            text.remove(0);
            return Ok(Expression::Not(Box::new(self.simple()?)));
        }
        self.simple()
    }

    fn simple(&mut self) -> Result<Expression, Status> {
        if self.peek() == &Token::Open {
            self.take();
            let expression = self.expression()?;
            self.expect(Token::Close)?;
            return Ok(expression);
        }
        let field = match self.peek() {
            Token::Text(text) if !["AND", "OR", "NOT"].contains(&text.as_str()) => text.clone(),
            Token::Quoted(text) => text.clone(),
            _ => return Err(self.unexpected()),
        };
        self.take();
        let Token::Comparator(comparator) = *self.peek() else {
            return Err(Status::unimplemented(
                "ListQueues filter search across every field is not supported yet",
            ));
        };
        self.take();
        let operand = match self.peek() {
            Token::Text(text) | Token::Quoted(text) => text.clone(),
            _ => return Err(self.unexpected()),
        };
        self.take();
        restriction(&field, comparator, &operand)
    }
}
