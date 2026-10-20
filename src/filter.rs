//! ListQueues filters, in the syntax of https://google.aip.dev/160

use std::cmp::Ordering;
use std::iter::Peekable;
use std::str::Chars;

use crate::proto::google::cloud::tasks::v2 as pb;

pub(crate) struct Filter(Expression);

pub(crate) enum Error {
    /// The token where parsing failed, `<EOF>` at the end
    Syntax(String),
    FieldNotFound(String),
    IncorrectType(String),
    NestedType(String),
    Unsupported(&'static str),
}

enum Expression {
    All(Vec<Expression>),
    Any(Vec<Expression>),
    Not(Box<Expression>),
    Present(String),
    Contains(String, String),
    Compare(String, Comparator, Scalar),
}

#[derive(Clone, Copy, PartialEq)]
enum Comparator {
    Has,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(PartialEq, PartialOrd)]
enum Scalar {
    Number(f64),
    Nanos(i128),
}

/// A queue field's value, by the kind of operand it takes
enum Value<'a> {
    Text(&'a str),
    Enum(i32),
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

impl Filter {
    pub fn parse(filter: &str) -> Result<Self, Error> {
        let mut parser = Parser {
            tokens: tokens(filter)?,
            next: 0,
        };
        if parser.peek() == &Token::End {
            return Ok(Self(Expression::All(Vec::new())));
        }
        let expression = parser.expression()?;
        parser.expect(Token::End)?;
        Ok(Self(expression))
    }

    pub fn matches(&self, queue: &pb::Queue) -> bool {
        self.0.matches(queue)
    }
}

impl Expression {
    fn matches(&self, queue: &pb::Queue) -> bool {
        match self {
            Expression::All(expressions) => expressions.iter().all(|e| e.matches(queue)),
            Expression::Any(expressions) => expressions.iter().any(|e| e.matches(queue)),
            Expression::Not(expression) => !expression.matches(queue),
            Expression::Present(path) => match value(queue, path).expect("resolved at parse") {
                Value::Text(text) => !text.is_empty(),
                Value::Enum(number) => number != 0,
                Value::Number(number) => number != 0.0,
                Value::Duration(duration) => duration.is_some(),
                Value::Time(time) => time.is_some(),
                Value::Message(present) => present,
            },
            Expression::Contains(path, needle) => {
                match value(queue, path).expect("resolved at parse") {
                    Value::Text(text) => text.to_lowercase().contains(needle),
                    _ => unreachable!("only text fields take a substring"),
                }
            }
            Expression::Compare(path, comparator, operand) => {
                // Unset durations and times compare as zero, as in Cloud Tasks
                let field = match value(queue, path).expect("resolved at parse") {
                    Value::Enum(number) => Scalar::Number(number.into()),
                    Value::Number(number) => Scalar::Number(number),
                    Value::Duration(duration) => Scalar::Nanos(duration.map_or(0, |duration| {
                        i128::from(duration.seconds) * 1_000_000_000 + i128::from(duration.nanos)
                    })),
                    Value::Time(time) => Scalar::Nanos(time.map_or(0, |time| {
                        i128::from(time.seconds) * 1_000_000_000 + i128::from(time.nanos)
                    })),
                    Value::Text(_) | Value::Message(_) => {
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

/// Looks up a path in lower case without underscores, so snake and camel case both match
fn value<'a>(queue: &'a pb::Queue, path: &str) -> Option<Value<'a>> {
    let rate_limits = queue.rate_limits.unwrap_or_default();
    let retry_config = queue.retry_config.unwrap_or_default();
    Some(match path {
        "name" => Value::Text(&queue.name),
        "state" => Value::Enum(queue.state),
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
        "appengineroutingoverride" => Value::Message(queue.app_engine_routing_override.is_some()),
        "httptarget" => Value::Message(queue.http_target.is_some()),
        "stackdriverloggingconfig" => Value::Message(queue.stackdriver_logging_config.is_some()),
        _ => return None,
    })
}

fn restriction(field: &str, comparator: Comparator, operand: &str) -> Result<Expression, Error> {
    let path = field.to_lowercase().replace('_', "");
    if [
        "appengineroutingoverride.",
        "httptarget.",
        "stackdriverloggingconfig.",
    ]
    .iter()
    .any(|message| path.starts_with(message))
    {
        return Err(Error::Unsupported("fields of later layers' messages"));
    }
    // Types the operand by the field's value in an empty queue
    let empty = pb::Queue::default();
    let kind = value(&empty, &path).ok_or_else(|| Error::FieldNotFound(field.to_owned()))?;
    if comparator == Comparator::Has && operand == "*" {
        return Ok(Expression::Present(path));
    }
    let incorrect_type = || Error::IncorrectType(operand.to_lowercase());
    let scalar = match kind {
        Value::Message(_) => return Err(Error::NestedType(field.to_owned())),
        Value::Text(_) if comparator == Comparator::Has => {
            return Ok(Expression::Contains(path, operand.to_lowercase()));
        }
        Value::Text(_) => return Err(Error::Unsupported("text comparisons other than ':'")),
        Value::Enum(_) => pb::queue::State::from_str_name(&operand.to_uppercase())
            .map(|state| Scalar::Number(f64::from(state as i32)))
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

fn tokens(filter: &str) -> Result<Vec<Token>, Error> {
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
                        None => return Err(Error::Syntax("<EOF>".to_owned())),
                    }
                }
                Token::Quoted(text)
            }
            c if is_symbol(c) => return Err(Error::Syntax(c.to_string())),
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

    fn expect(&mut self, expected: Token) -> Result<(), Error> {
        if *self.peek() == expected {
            self.take();
            Ok(())
        } else {
            Err(self.syntax_error())
        }
    }

    fn syntax_error(&self) -> Error {
        Error::Syntax(match self.peek() {
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
    fn expression(&mut self) -> Result<Expression, Error> {
        let mut sequences = vec![self.sequence()?];
        while self.keyword("AND") {
            sequences.push(self.sequence()?);
        }
        Ok(Expression::All(sequences))
    }

    /// Factors side by side, which AIP-160 reads as AND
    fn sequence(&mut self) -> Result<Expression, Error> {
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

    fn factor(&mut self) -> Result<Expression, Error> {
        let mut terms = vec![self.term()?];
        while self.keyword("OR") {
            terms.push(self.term()?);
        }
        Ok(Expression::Any(terms))
    }

    fn term(&mut self) -> Result<Expression, Error> {
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

    fn simple(&mut self) -> Result<Expression, Error> {
        if self.peek() == &Token::Open {
            self.take();
            let expression = self.expression()?;
            self.expect(Token::Close)?;
            return Ok(expression);
        }
        let field = match self.peek() {
            Token::Text(text) if !["AND", "OR", "NOT"].contains(&text.as_str()) => text.clone(),
            Token::Quoted(text) => text.clone(),
            _ => return Err(self.syntax_error()),
        };
        self.take();
        let Token::Comparator(comparator) = *self.peek() else {
            return Err(Error::Unsupported("search across every field"));
        };
        self.take();
        let operand = match self.peek() {
            Token::Text(text) | Token::Quoted(text) => text.clone(),
            _ => return Err(self.syntax_error()),
        };
        self.take();
        restriction(&field, comparator, &operand)
    }
}
