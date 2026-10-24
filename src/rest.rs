use std::ops::Range;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::uri::PathAndQuery;
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use prost::Message;
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, MethodDescriptor, SerializeOptions,
};
use prost_types::Any;
use serde_json::{Map, Value};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::service::Routes;

use crate::DESCRIPTORS;
use crate::proto::google::api::{HttpRule, http_rule};
use crate::proto::google::rpc;

/// Serves HTTP/JSON mapping of API by calling `grpc` in process
pub fn router(grpc: Routes) -> axum::Router {
    let http = DESCRIPTORS
        .get_extension_by_name("google.api.http")
        .expect("descriptors include google/api/annotations.proto");
    let service = DESCRIPTORS
        .get_service_by_name("google.cloud.tasks.v2.CloudTasks")
        .expect("descriptors include the Cloud Tasks service");
    let mut rules: Vec<Rule> = service
        .methods()
        .map(|rpc| {
            let rule: HttpRule = rpc
                .options()
                .get_extension(&http)
                .as_message()
                .expect("every Cloud Tasks method has an HTTP rule")
                .transcode_to()
                .expect("HTTP rules transcode");
            let (method, template) = match rule.pattern {
                Some(http_rule::Pattern::Get(template)) => (Method::GET, template),
                Some(http_rule::Pattern::Post(template)) => (Method::POST, template),
                Some(http_rule::Pattern::Patch(template)) => (Method::PATCH, template),
                Some(http_rule::Pattern::Delete(template)) => (Method::DELETE, template),
                pattern => panic!("Cloud Tasks maps no method as {pattern:?}"),
            };
            Rule::new(rpc, method, &template, &rule.body)
        })
        .collect();
    // Cloud Tasks maps GetOperation in its service config rather than its protos, and maps no other Operations RPC
    let get_operation = DESCRIPTORS
        .get_service_by_name("google.longrunning.Operations")
        .and_then(|service| {
            service
                .methods()
                .find(|method| method.name() == "GetOperation")
        })
        .expect("descriptors include the Operations service");
    rules.push(Rule::new(
        get_operation,
        Method::GET,
        "/v2/{name=projects/*/locations/*/operations/*}",
        "",
    ));
    let status = DESCRIPTORS
        .get_message_by_name("google.rpc.Status")
        .expect("descriptors include google/rpc/status.proto");
    let rest = Arc::new(Rest {
        rules,
        grpc,
        status,
    });
    axum::Router::new()
        .route("/v2/{*path}", any(handle))
        .with_state(rest)
}

struct Rest {
    rules: Vec<Rule>,
    grpc: Routes,
    status: MessageDescriptor,
}

/// RPC's HTTP rule, as `google.api.http` defines it
struct Rule {
    method: Method,
    /// Template's segments, where `*` matches any one segment
    segments: Vec<String>,
    verb: Option<String>,
    /// Each variable's field path and segments it binds
    variables: Vec<(String, Range<usize>)>,
    body: Body,
    rpc: PathAndQuery,
    input: MessageDescriptor,
    output: MessageDescriptor,
}

enum Body {
    None,
    Whole,
    Field(String),
}

impl Rule {
    fn new(rpc: MethodDescriptor, method: Method, template: &str, body: &str) -> Self {
        let template = template.strip_prefix('/').expect("templates are absolute");
        let (mut remaining, verb) = match template.rsplit_once(':') {
            Some((path, verb)) if !verb.contains(['/', '}']) => (path, Some(verb.to_owned())),
            _ => (template, None),
        };
        let mut segments = Vec::new();
        let mut variables = Vec::new();
        while !remaining.is_empty() {
            if let Some(variable) = remaining.strip_prefix('{') {
                let (variable, after) = variable.split_once('}').expect("variables close");
                let (field, pattern) = variable.split_once('=').unwrap_or((variable, "*"));
                let start = segments.len();
                segments.extend(pattern.split('/').map(str::to_owned));
                variables.push((field.to_owned(), start..segments.len()));
                remaining = after;
            } else {
                let end = remaining.find('/').unwrap_or(remaining.len());
                segments.push(remaining[..end].to_owned());
                remaining = &remaining[end..];
            }
            remaining = remaining.strip_prefix('/').unwrap_or(remaining);
        }
        let body = match body {
            "" => Body::None,
            "*" => Body::Whole,
            field => Body::Field(field.to_owned()),
        };
        Self {
            method,
            segments,
            verb,
            variables,
            body,
            rpc: format!("/{}/{}", rpc.parent_service().full_name(), rpc.name())
                .parse()
                .expect("RPC paths are valid"),
            input: rpc.input(),
            output: rpc.output(),
        }
    }

    fn bind<'a>(&'a self, method: &Method, path: &str) -> Option<Vec<(&'a str, String)>> {
        let path = path.strip_prefix('/')?;
        let (path, verb) = match path.rsplit_once(':') {
            Some((path, verb)) if !verb.contains('/') => (path, Some(verb)),
            _ => (path, None),
        };
        let segments: Vec<&str> = path.split('/').collect();
        let matches = *method == self.method
            && verb == self.verb.as_deref()
            && segments.len() == self.segments.len()
            && segments
                .iter()
                .zip(&self.segments)
                .all(|(segment, template)| match template.as_str() {
                    "*" => !segment.is_empty(),
                    literal => *segment == literal,
                });
        matches.then(|| {
            self.variables
                .iter()
                .map(|(field, range)| (field.as_str(), segments[range.clone()].join("/")))
                .collect()
        })
    }
}

async fn handle(
    State(rest): State<Arc<Rest>>,
    method: Method,
    uri: Uri,
    Query(query): Query<Vec<(String, String)>>,
    body: Bytes,
) -> Response {
    let Some((rule, bindings)) = rest
        .rules
        .iter()
        .find_map(|rule| Some((rule, rule.bind(&method, uri.path())?)))
    else {
        let page = include_str!("not_found.html").replace("{path}", uri.path());
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/html; charset=UTF-8")],
            page,
        )
            .into_response();
    };
    let pretty = !query
        .iter()
        .any(|(key, value)| key == "prettyPrint" && matches!(value.as_str(), "false" | "0"));
    let mut enum_numbers = false;
    let mut fields = Vec::new();
    for (key, value) in query {
        match key.as_str() {
            "prettyPrint" => {}
            "alt" | "$alt" => match value.as_str() {
                "json" => {}
                "json;enum-encoding=int" => enum_numbers = true,
                _ => {
                    let status = rpc::Status {
                        code: rpc::Code::InvalidArgument as i32,
                        message: format!("Invalid value \"{value}\" for query parameter 'alt'"),
                        details: Vec::new(),
                    };
                    return respond(error(&rest.status, status), pretty);
                }
            },
            _ => fields.push((key, value)),
        }
    }
    let result = match request(rule, bindings, fields, &body) {
        Ok(request) => call(&rest.grpc, rule, request).await,
        Err(status) => Err(status),
    };
    let json = match result {
        Ok(response) => {
            let options = SerializeOptions::new().use_enum_numbers(enum_numbers);
            (StatusCode::OK, to_json(&response, &options))
        }
        Err(status) => error(&rest.status, status),
    };
    respond(json, pretty)
}

/// Builds request from body, query and path, rejecting what Google's front end rejects
fn request(
    rule: &Rule,
    bindings: Vec<(&str, String)>,
    query: Vec<(String, String)>,
    body: &[u8],
) -> Result<DynamicMessage, rpc::Status> {
    let mut request = match &rule.body {
        Body::None => Map::new(),
        Body::Whole => object(body)?,
        Body::Field(field) => Map::from_iter([(field.clone(), Value::Object(object(body)?))]),
    };
    for (key, value) in query {
        let path = field_path(&rule.input, &key).ok_or_else(|| {
            bad_request(vec![violation(
                "",
                format!(
                    "Invalid JSON payload received. Unknown name \"{key}\": Cannot bind query parameter. Field '{key}' could not be found in request message."
                ),
            )])
        })?;
        insert(&mut request, &path, value);
    }
    for (path, value) in bindings {
        insert(&mut request, path, value);
    }
    let request = Value::Object(request);
    let mut violations = Vec::new();
    check_message(&request, &rule.input, "", &mut violations);
    if !violations.is_empty() {
        return Err(bad_request(violations));
    }
    DynamicMessage::deserialize(rule.input.clone(), request).map_err(invalid_payload)
}

fn object(body: &[u8]) -> Result<Map<String, Value>, rpc::Status> {
    if body.is_empty() {
        return Ok(Map::new());
    }
    match serde_json::from_slice(body).map_err(invalid_payload)? {
        Value::Object(object) => Ok(object),
        _ => Err(bad_request(vec![violation(
            "",
            "Invalid JSON payload received. Unknown name \"\": Root element must be a message."
                .to_owned(),
        )])),
    }
}

/// Resolves query parameter's dotted field names, in JSON or proto form, to proto field names
fn field_path(message: &MessageDescriptor, key: &str) -> Option<String> {
    let mut message = Some(message.clone());
    let mut path = Vec::new();
    for name in key.split('.') {
        let parent = message.take()?;
        let field = parent
            .get_field_by_json_name(name)
            .or_else(|| parent.get_field_by_name(name))?;
        if let Kind::Message(child) = field.kind() {
            message = Some(child);
        }
        path.push(field.name().to_owned());
    }
    Some(path.join("."))
}

/// Sets field at dotted path of proto field names, replacing anything in the way
fn insert(request: &mut Map<String, Value>, path: &str, value: String) {
    let (parents, field) = path.rsplit_once('.').unwrap_or(("", path));
    let mut object = request;
    for parent in parents.split('.').filter(|parent| !parent.is_empty()) {
        let child = object
            .entry(parent)
            .or_insert_with(|| Value::Object(Map::new()));
        if !child.is_object() {
            *child = Value::Object(Map::new());
        }
        object = child.as_object_mut().expect("child is an object");
    }
    object.insert(field.to_owned(), Value::String(value));
}

/// Collects unknown names and mistyped values in message, with Google's wording
fn check_message(
    value: &Value,
    message: &MessageDescriptor,
    path: &str,
    violations: &mut Vec<rpc::bad_request::FieldViolation>,
) {
    let Value::Object(object) = value else {
        violations.push(invalid_value(path, &Kind::Message(message.clone()), value));
        return;
    };
    for (key, child) in object {
        let Some(field) = message
            .get_field_by_json_name(key)
            .or_else(|| message.get_field_by_name(key))
        else {
            let at = if path.is_empty() {
                String::new()
            } else {
                format!(" at '{path}'")
            };
            violations.push(violation(
                path,
                format!(
                    "Invalid JSON payload received. Unknown name \"{key}\"{at}: Cannot find field."
                ),
            ));
            continue;
        };
        let path = if path.is_empty() {
            field.name().to_owned()
        } else {
            format!("{path}.{}", field.name())
        };
        check_field(child, &field, &path, violations);
    }
}

fn check_field(
    value: &Value,
    field: &FieldDescriptor,
    path: &str,
    violations: &mut Vec<rpc::bad_request::FieldViolation>,
) {
    if value.is_null() {
        return;
    }
    let kind = field.kind();
    if field.is_map() {
        let Kind::Message(entry) = &kind else {
            unreachable!("map fields hold entry messages");
        };
        let value_kind = entry.map_entry_value_field().kind();
        match value.as_object() {
            Some(entries) => entries
                .values()
                .for_each(|entry| check_value(entry, &value_kind, path, violations)),
            None => violations.push(invalid_value(path, &kind, value)),
        }
    } else if field.is_list() {
        match value.as_array() {
            Some(items) => items
                .iter()
                .for_each(|item| check_value(item, &kind, path, violations)),
            None => violations.push(invalid_value(path, &kind, value)),
        }
    } else {
        check_value(value, &kind, path, violations);
    }
}

fn check_value(
    value: &Value,
    kind: &Kind,
    path: &str,
    violations: &mut Vec<rpc::bad_request::FieldViolation>,
) {
    let valid = match kind {
        // Left to JSON mapping, which knows each well-known type's own form
        Kind::Message(message) if message.full_name().starts_with("google.protobuf.") => true,
        Kind::Message(message) => return check_message(value, message, path, violations),
        Kind::Enum(values) => {
            value.is_i64()
                || value
                    .as_str()
                    .is_some_and(|name| values.get_value_by_name(name).is_some())
        }
        Kind::String | Kind::Bytes => value.is_string(),
        Kind::Bool => value.is_boolean(),
        Kind::Double | Kind::Float => {
            value.is_number()
                || value
                    .as_str()
                    .is_some_and(|text| text.parse::<f64>().is_ok())
        }
        _ => {
            value.is_i64()
                || value.is_u64()
                || value
                    .as_str()
                    .is_some_and(|text| text.parse::<i128>().is_ok())
        }
    };
    if !valid {
        violations.push(invalid_value(path, kind, value));
    }
}

fn invalid_value(path: &str, kind: &Kind, value: &Value) -> rpc::bad_request::FieldViolation {
    let kind = match kind {
        Kind::Message(message) => format!("type.googleapis.com/{}", message.full_name()),
        Kind::Enum(values) => format!("type.googleapis.com/{}", values.full_name()),
        // Debug names scalars as their descriptor type does, such as Int32 for TYPE_INT32
        scalar => format!("TYPE_{}", format!("{scalar:?}").to_uppercase()),
    };
    violation(path, format!("Invalid value at '{path}' ({kind}), {value}"))
}

fn violation(field: &str, description: String) -> rpc::bad_request::FieldViolation {
    rpc::bad_request::FieldViolation {
        field: field.to_owned(),
        description,
        ..Default::default()
    }
}

fn bad_request(violations: Vec<rpc::bad_request::FieldViolation>) -> rpc::Status {
    let message = violations
        .iter()
        .map(|violation| violation.description.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let details = rpc::BadRequest {
        field_violations: violations,
    };
    rpc::Status {
        code: rpc::Code::InvalidArgument as i32,
        message,
        details: vec![Any::from_msg(&details).expect("Vec grows to fit")],
    }
}

fn invalid_payload(error: impl std::fmt::Display) -> rpc::Status {
    rpc::Status {
        code: rpc::Code::InvalidArgument as i32,
        message: format!("Invalid JSON payload received. {error}"),
        details: Vec::new(),
    }
}

async fn call(
    grpc: &Routes,
    rule: &Rule,
    request: DynamicMessage,
) -> Result<DynamicMessage, rpc::Status> {
    let mut client = tonic::client::Grpc::new(grpc.clone());
    let Ok(()) = client.ready().await;
    let codec = DynamicCodec(rule.output.clone());
    match client
        .unary(tonic::Request::new(request), rule.rpc.clone(), codec)
        .await
    {
        Ok(response) => Ok(response.into_inner()),
        Err(status) => Err(rpc::Status {
            code: status.code() as i32,
            message: status.message().to_owned(),
            details: rpc::Status::decode(status.details())
                .expect("the API sends details as a google.rpc.Status")
                .details,
        }),
    }
}

#[derive(Clone)]
struct DynamicCodec(MessageDescriptor);

impl Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicCodec;
    type Decoder = DynamicCodec;

    fn encoder(&mut self) -> Self::Encoder {
        self.clone()
    }

    fn decoder(&mut self) -> Self::Decoder {
        self.clone()
    }
}

impl Encoder for DynamicCodec {
    type Item = DynamicMessage;
    type Error = tonic::Status;

    fn encode(&mut self, item: DynamicMessage, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(dst).expect("the buffer grows to fit");
        Ok(())
    }
}

impl Decoder for DynamicCodec {
    type Item = DynamicMessage;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<DynamicMessage>, Self::Error> {
        let message =
            DynamicMessage::decode(self.0.clone(), src).expect("the API sends its declared type");
        Ok(Some(message))
    }
}

fn error(descriptor: &MessageDescriptor, status: rpc::Status) -> (StatusCode, Value) {
    let code = rpc::Code::try_from(status.code).expect("statuses carry google.rpc codes");
    let http = match code {
        rpc::Code::Ok => 200,
        rpc::Code::Cancelled => 499,
        rpc::Code::Unknown => 500,
        rpc::Code::InvalidArgument => 400,
        rpc::Code::DeadlineExceeded => 504,
        rpc::Code::NotFound => 404,
        rpc::Code::AlreadyExists => 409,
        rpc::Code::PermissionDenied => 403,
        rpc::Code::ResourceExhausted => 429,
        rpc::Code::FailedPrecondition => 400,
        rpc::Code::Aborted => 409,
        rpc::Code::OutOfRange => 400,
        rpc::Code::Unimplemented => 501,
        rpc::Code::Internal => 500,
        rpc::Code::Unavailable => 503,
        rpc::Code::DataLoss => 500,
        rpc::Code::Unauthenticated => 401,
    };
    let mut error = Map::new();
    error.insert("code".to_owned(), http.into());
    error.insert("message".to_owned(), status.message.clone().into());
    error.insert("status".to_owned(), code.as_str_name().into());
    if !status.details.is_empty() {
        let status = DynamicMessage::decode(descriptor.clone(), status.encode_to_vec().as_slice())
            .expect("a google.rpc.Status encodes as one");
        let mut status = to_json(&status, &SerializeOptions::new());
        error.insert("details".to_owned(), status["details"].take());
    }
    let status = StatusCode::from_u16(http).expect("each code maps to a valid HTTP status");
    (
        status,
        Value::Object(Map::from_iter([("error".to_owned(), error.into())])),
    )
}

fn to_json(message: &DynamicMessage, options: &SerializeOptions) -> Value {
    let mut json = message
        .serialize_with_options(serde_json::value::Serializer, options)
        .expect("every message has a JSON form");
    like_google(&mut json);
    json
}

/// Prints whole doubles without fraction, and Empty in Any as plain message, as Google does
fn like_google(json: &mut Value) {
    match json {
        Value::Number(number) => {
            if let Some(float) = number.as_f64()
                && number.is_f64()
                && float.fract() == 0.0
                && float.abs() < i64::MAX as f64
            {
                *json = Value::from(float as i64);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(like_google),
        Value::Object(object) => {
            if object
                .get("@type")
                .is_some_and(|url| url == "type.googleapis.com/google.protobuf.Empty")
            {
                object.shift_remove("value");
            }
            object.values_mut().for_each(like_google);
        }
        _ => {}
    }
}

fn respond((status, json): (StatusCode, Value), pretty: bool) -> Response {
    let body = if pretty {
        serde_json::to_string_pretty(&json).expect("JSON values print") + "\n"
    } else {
        json.to_string()
    };
    (
        status,
        [(header::CONTENT_TYPE, "application/json; charset=UTF-8")],
        body,
    )
        .into_response()
}
