//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Declarative JSON reshaping: field mapping, then schema-directed coercion.
//!
//! Both stages run over a single `serde_json::Value` parsed once per message, so a
//! message is never round-tripped through bytes between stages. Everything derived from
//! configuration (source paths, the schema) is compiled once in `new()`.
//!
//! Only the JSON Schema subset that matters for message integration is honoured:
//! `type`, `properties`, `required`, `default`, `items`, `nullable`, `enum`. Other
//! keywords are ignored rather than rejected, so a fuller schema can be pointed at
//! without being rewritten.

use crate::models::{MappingRule, TransformErrorPolicy, TransformMiddleware};
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, Received, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{Map, Value};
use std::any::Any;
use std::sync::Arc;

/// Metadata key carrying the failure description when `on_error: pass_through`.
pub const TRANSFORM_ERROR_KEY: &str = "mqb.transform_error";

// --- Errors ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorKind {
    Parse,
    Coercion,
    MissingRequired,
    TypeMismatch,
    Enum,
}

impl ErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            ErrorKind::Parse => "parse",
            ErrorKind::Coercion => "coercion",
            ErrorKind::MissingRequired => "missing_required",
            ErrorKind::TypeMismatch => "type_mismatch",
            ErrorKind::Enum => "enum",
        }
    }
}

/// A transformation failure, always permanent: the same bytes will fail the same way, so
/// these surface as `NonRetryable` and are meant to be routed to a DLQ.
#[derive(Debug)]
struct TransformError {
    path: String,
    kind: ErrorKind,
    detail: String,
}

impl TransformError {
    fn new(path: String, kind: ErrorKind, detail: impl Into<String>) -> Self {
        Self {
            path,
            kind,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "transform failed at {} [{}]: {}",
            self.path,
            self.kind.as_str(),
            self.detail
        )
    }
}

impl std::error::Error for TransformError {}

impl From<TransformError> for PublisherError {
    fn from(err: TransformError) -> Self {
        PublisherError::NonRetryable(anyhow::Error::new(err))
    }
}

// --- Paths ---

#[derive(Debug, Clone, PartialEq, Eq)]
enum Seg {
    Key(String),
    Index(usize),
}

/// A source path compiled once from `$.a.b[0]` into segments. Lookups walk borrowed
/// `Value` references and allocate nothing.
#[derive(Debug, Clone)]
struct CompiledPath {
    segs: Vec<Seg>,
    /// The original spec, kept for error messages.
    spec: String,
}

impl CompiledPath {
    fn parse(spec: &str) -> anyhow::Result<Self> {
        let trimmed = spec.trim();
        let body = trimmed.strip_prefix('$').unwrap_or(trimmed);
        let body = body.strip_prefix('.').unwrap_or(body);

        let mut segs = Vec::new();
        if !body.is_empty() {
            for part in body.split('.') {
                if part.is_empty() {
                    anyhow::bail!("empty segment in path '{spec}'");
                }
                let (name, mut brackets) = match part.find('[') {
                    Some(i) => (&part[..i], &part[i..]),
                    None => (part, ""),
                };
                if !name.is_empty() {
                    segs.push(Seg::Key(name.to_string()));
                }
                while !brackets.is_empty() {
                    let close = brackets
                        .find(']')
                        .ok_or_else(|| anyhow::anyhow!("unclosed '[' in path '{spec}'"))?;
                    let raw = &brackets[1..close];
                    let idx: usize = raw.parse().map_err(|_| {
                        anyhow::anyhow!("invalid array index '{raw}' in path '{spec}'")
                    })?;
                    segs.push(Seg::Index(idx));
                    brackets = &brackets[close + 1..];
                }
            }
        }
        Ok(Self {
            segs,
            spec: trimmed.to_string(),
        })
    }

    fn get<'a>(&self, root: &'a Value) -> Option<&'a Value> {
        let mut cur = root;
        for seg in &self.segs {
            cur = match seg {
                Seg::Key(k) => cur.get(k.as_str())?,
                Seg::Index(i) => cur.get(*i)?,
            };
        }
        Some(cur)
    }
}

// --- Mapping stage ---

#[derive(Debug)]
struct CompiledRule {
    /// Output location, split on dots so `address.city` nests.
    out: Vec<String>,
    from: CompiledPath,
    default: Option<Value>,
    required: bool,
}

/// Writes `value` at `path`, creating intermediate objects as needed.
fn insert_at(root: &mut Value, path: &[String], value: Value) -> Result<(), TransformError> {
    let (last, parents) = match path.split_last() {
        Some(split) => split,
        // Compilation rejects empty output keys, so this is unreachable in practice.
        None => return Ok(()),
    };
    let mut cur = root;
    for key in parents {
        let obj = match cur {
            Value::Object(map) => map,
            other => {
                return Err(TransformError::new(
                    format!("$.{}", path.join(".")),
                    ErrorKind::TypeMismatch,
                    format!(
                        "cannot nest under '{key}': it is already a {}",
                        type_name(other)
                    ),
                ))
            }
        };
        cur = obj
            .entry(key.as_str())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    match cur {
        Value::Object(map) => {
            map.insert(last.clone(), value);
            Ok(())
        }
        other => Err(TransformError::new(
            format!("$.{}", path.join(".")),
            ErrorKind::TypeMismatch,
            format!("cannot set '{last}': parent is a {}", type_name(other)),
        )),
    }
}

// --- Schema stage ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ty {
    String,
    Integer,
    Number,
    Boolean,
    Object,
    Array,
    Null,
}

impl Ty {
    fn parse(s: &str) -> Option<Ty> {
        Some(match s {
            "string" => Ty::String,
            "integer" => Ty::Integer,
            "number" => Ty::Number,
            "boolean" => Ty::Boolean,
            "object" => Ty::Object,
            "array" => Ty::Array,
            "null" => Ty::Null,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            Ty::String => "string",
            Ty::Integer => "integer",
            Ty::Number => "number",
            Ty::Boolean => "boolean",
            Ty::Object => "object",
            Ty::Array => "array",
            Ty::Null => "null",
        }
    }

    fn matches(self, v: &Value) -> bool {
        match (self, v) {
            (Ty::String, Value::String(_)) => true,
            (Ty::Integer, Value::Number(n)) => n.is_i64() || n.is_u64(),
            (Ty::Number, Value::Number(_)) => true,
            (Ty::Boolean, Value::Bool(_)) => true,
            (Ty::Object, Value::Object(_)) => true,
            (Ty::Array, Value::Array(_)) => true,
            (Ty::Null, Value::Null) => true,
            _ => false,
        }
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// The supported JSON Schema subset, pre-resolved so the hot path never inspects raw
/// schema JSON. `properties` is a sorted `Vec` rather than a map: it is only ever
/// iterated, and sorting keeps error reporting deterministic.
#[derive(Debug, Default)]
struct CompiledSchema {
    ty: Option<Ty>,
    nullable: bool,
    properties: Vec<(String, CompiledSchema)>,
    required: Vec<String>,
    default: Option<Value>,
    items: Option<Box<CompiledSchema>>,
    enum_values: Option<Vec<Value>>,
}

impl CompiledSchema {
    fn compile(schema: &Value) -> anyhow::Result<Self> {
        let obj = schema
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("schema must be a JSON object"))?;

        let mut ty = None;
        let mut nullable = obj
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        match obj.get("type") {
            Some(Value::String(s)) => {
                ty = Some(
                    Ty::parse(s).ok_or_else(|| anyhow::anyhow!("unsupported schema type '{s}'"))?,
                );
            }
            // `["string", "null"]` is the JSON Schema way of spelling nullable.
            Some(Value::Array(types)) => {
                for entry in types {
                    let s = entry
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("schema 'type' entries must be strings"))?;
                    let parsed = Ty::parse(s)
                        .ok_or_else(|| anyhow::anyhow!("unsupported schema type '{s}'"))?;
                    if parsed == Ty::Null {
                        nullable = true;
                    } else if ty.is_none() {
                        ty = Some(parsed);
                    } else {
                        anyhow::bail!("schema 'type' lists more than one non-null type");
                    }
                }
            }
            Some(_) => anyhow::bail!("schema 'type' must be a string or array of strings"),
            None => {}
        }

        let mut properties = Vec::new();
        if let Some(props) = obj.get("properties") {
            let props = props
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("schema 'properties' must be an object"))?;
            for (name, sub) in props {
                properties.push((name.clone(), CompiledSchema::compile(sub)?));
            }
            properties.sort_by(|a, b| a.0.cmp(&b.0));
        }

        let required = match obj.get("required") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| anyhow::anyhow!("schema 'required' entries must be strings"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            Some(_) => anyhow::bail!("schema 'required' must be an array"),
            None => Vec::new(),
        };

        let items = match obj.get("items") {
            Some(sub) => Some(Box::new(CompiledSchema::compile(sub)?)),
            None => None,
        };

        let enum_values = match obj.get("enum") {
            Some(Value::Array(values)) => Some(values.clone()),
            Some(_) => anyhow::bail!("schema 'enum' must be an array"),
            None => None,
        };

        Ok(Self {
            ty,
            nullable,
            properties,
            required,
            default: obj.get("default").cloned(),
            items,
            enum_values,
        })
    }

    /// Coerce, fill defaults and validate in a single walk.
    ///
    /// `crumbs` is a reusable breadcrumb stack borrowed from the schema, so the error
    /// path is only materialised into a `String` when something actually fails.
    fn apply<'s>(
        &'s self,
        value: &mut Value,
        crumbs: &mut Vec<Crumb<'s>>,
        opts: Opts,
    ) -> Result<(), TransformError> {
        if value.is_null() {
            // A nullable null is explicitly allowed and needs no type/enum check.
            if self.nullable {
                return Ok(());
            }
            match (opts.apply_defaults, &self.default) {
                (true, Some(default)) => *value = default.clone(),
                // Reported directly rather than falling through to coercion, which would
                // render the far less helpful "cannot coerce null null to integer".
                _ => {
                    if self.ty.is_some_and(|ty| ty != Ty::Null) {
                        return Err(TransformError::new(
                            render_path(crumbs),
                            ErrorKind::TypeMismatch,
                            "field is null but is not nullable and has no default",
                        ));
                    }
                }
            }
        }

        if let Some(ty) = self.ty {
            if !ty.matches(value) {
                if !opts.coerce {
                    return Err(TransformError::new(
                        render_path(crumbs),
                        ErrorKind::TypeMismatch,
                        format!("expected {}, found {}", ty.name(), type_name(value)),
                    ));
                }
                coerce(value, ty, crumbs)?;
            }
        }

        if let Some(allowed) = &self.enum_values {
            if !allowed.contains(value) {
                return Err(TransformError::new(
                    render_path(crumbs),
                    ErrorKind::Enum,
                    format!(
                        "value {} is not one of {}",
                        value,
                        Value::Array(allowed.clone())
                    ),
                ));
            }
        }

        match value {
            Value::Object(map) => {
                if opts.apply_defaults {
                    for (name, sub) in &self.properties {
                        if !map.contains_key(name) {
                            if let Some(default) = &sub.default {
                                map.insert(name.clone(), default.clone());
                            }
                        }
                    }
                }
                // Checked after defaults, so a default satisfies `required`.
                for name in &self.required {
                    if !map.contains_key(name) {
                        crumbs.push(Crumb::Key(name));
                        let path = render_path(crumbs);
                        crumbs.pop();
                        return Err(TransformError::new(
                            path,
                            ErrorKind::MissingRequired,
                            "required field is missing and no default is defined",
                        ));
                    }
                }
                for (name, sub) in &self.properties {
                    if let Some(field) = map.get_mut(name) {
                        crumbs.push(Crumb::Key(name));
                        let result = sub.apply(field, crumbs, opts);
                        crumbs.pop();
                        result?;
                    }
                }
            }
            Value::Array(items) => {
                if let Some(item_schema) = &self.items {
                    for (i, item) in items.iter_mut().enumerate() {
                        crumbs.push(Crumb::Index(i));
                        let result = item_schema.apply(item, crumbs, opts);
                        crumbs.pop();
                        result?;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

/// Applies the one safe coercion for `ty`, or fails. Never best-effort: a value that
/// cannot be converted losslessly is an error, not a silent substitution.
fn coerce(value: &mut Value, ty: Ty, crumbs: &[Crumb<'_>]) -> Result<(), TransformError> {
    let coerced = match (ty, &*value) {
        (Ty::Integer, Value::String(s)) => s.trim().parse::<i64>().ok().map(Value::from),
        (Ty::Number, Value::String(s)) => s
            .trim()
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        (Ty::Boolean, Value::String(s)) => match s.trim() {
            "true" | "1" => Some(Value::Bool(true)),
            "false" | "0" => Some(Value::Bool(false)),
            _ => None,
        },
        (Ty::String, Value::Number(n)) => Some(Value::String(n.to_string())),
        _ => None,
    };

    match coerced {
        Some(new_value) => {
            *value = new_value;
            Ok(())
        }
        None => Err(TransformError::new(
            render_path(crumbs),
            ErrorKind::Coercion,
            format!(
                "cannot coerce {} {} to {}",
                type_name(value),
                value,
                ty.name()
            ),
        )),
    }
}

#[derive(Debug, Clone, Copy)]
enum Crumb<'a> {
    Key(&'a str),
    Index(usize),
}

fn render_path(crumbs: &[Crumb<'_>]) -> String {
    let mut out = String::from("$");
    for crumb in crumbs {
        match crumb {
            Crumb::Key(k) => {
                out.push('.');
                out.push_str(k);
            }
            Crumb::Index(i) => {
                out.push('[');
                out.push_str(&i.to_string());
                out.push(']');
            }
        }
    }
    out
}

#[derive(Debug, Clone, Copy)]
struct Opts {
    coerce: bool,
    apply_defaults: bool,
}

// --- Compiled configuration ---

/// Everything derived from config, built once and shared by every message.
#[derive(Debug)]
struct Compiled {
    /// Empty when no mapping stage is configured.
    rules: Vec<CompiledRule>,
    schema: Option<CompiledSchema>,
    opts: Opts,
    on_error: TransformErrorPolicy,
}

impl Compiled {
    fn new(config: &TransformMiddleware) -> anyhow::Result<Self> {
        if config.schema.is_some() && config.schema_file.is_some() {
            anyhow::bail!("transform middleware: set either 'schema' or 'schema_file', not both");
        }

        let mut rules = Vec::with_capacity(config.mapping.len());
        for (out_key, rule) in &config.mapping {
            if out_key.is_empty() {
                anyhow::bail!("transform middleware: mapping output key must not be empty");
            }
            let out: Vec<String> = out_key.split('.').map(str::to_string).collect();
            if out.iter().any(String::is_empty) {
                anyhow::bail!("transform middleware: empty segment in output key '{out_key}'");
            }
            let (default, required) = match rule {
                MappingRule::Path(_) => (None, false),
                MappingRule::Detailed {
                    default, required, ..
                } => (default.clone(), *required),
            };
            rules.push(CompiledRule {
                out,
                from: CompiledPath::parse(rule.path())?,
                default,
                required,
            });
        }
        // Deterministic order: config is a map, so iteration order is otherwise random
        // and overlapping keys ("a" and "a.b") would resolve inconsistently.
        rules.sort_by(|a, b| a.out.cmp(&b.out));

        // Read once, at startup. The hot path never touches the filesystem.
        let schema_value = match (&config.schema, &config.schema_file) {
            (Some(inline), _) => Some(inline.clone()),
            (_, Some(path)) => {
                let raw = std::fs::read_to_string(path).map_err(|e| {
                    anyhow::anyhow!("transform middleware: cannot read schema file '{path}': {e}")
                })?;
                Some(serde_json::from_str(&raw).map_err(|e| {
                    anyhow::anyhow!(
                        "transform middleware: schema file '{path}' is not valid JSON: {e}"
                    )
                })?)
            }
            _ => None,
        };
        let schema = match &schema_value {
            Some(v) => Some(CompiledSchema::compile(v)?),
            None => None,
        };

        Ok(Self {
            rules,
            schema,
            opts: Opts {
                coerce: config.coerce,
                apply_defaults: config.apply_defaults,
            },
            on_error: config.on_error,
        })
    }

    /// True when neither stage is configured; the message is then never parsed.
    fn is_noop(&self) -> bool {
        self.rules.is_empty() && self.schema.is_none()
    }

    fn apply_mapping(&self, input: &Value) -> Result<Value, TransformError> {
        let mut out = Value::Object(Map::new());
        for rule in &self.rules {
            let picked = match rule.from.get(input) {
                Some(found) => found.clone(),
                None => match &rule.default {
                    Some(default) => default.clone(),
                    None if rule.required => {
                        return Err(TransformError::new(
                            rule.from.spec.clone(),
                            ErrorKind::MissingRequired,
                            format!(
                                "required source field is missing (mapped to '{}')",
                                rule.out.join(".")
                            ),
                        ))
                    }
                    // Optional and absent: leave the output key out entirely.
                    None => continue,
                },
            };
            insert_at(&mut out, &rule.out, picked)?;
        }
        Ok(out)
    }

    /// Parses once, reshapes, serialises once.
    fn transform(&self, message: &mut CanonicalMessage) -> Result<(), TransformError> {
        let input: Value = serde_json::from_slice(&message.payload).map_err(|e| {
            TransformError::new(
                "$".to_string(),
                ErrorKind::Parse,
                format!("payload is not valid JSON: {e}"),
            )
        })?;

        let mut value = if self.rules.is_empty() {
            input
        } else {
            self.apply_mapping(&input)?
        };

        if let Some(schema) = &self.schema {
            let mut crumbs = Vec::new();
            schema.apply(&mut value, &mut crumbs, self.opts)?;
        }

        let bytes = serde_json::to_vec(&value).map_err(|e| {
            TransformError::new(
                "$".to_string(),
                ErrorKind::Parse,
                format!("transformed value could not be serialized: {e}"),
            )
        })?;
        message.payload = Bytes::from(bytes);
        Ok(())
    }

    /// Applies the configured policy to a failure. `Ok` keeps the message (annotated),
    /// `Err` rejects it.
    fn handle_failure(
        &self,
        message: &mut CanonicalMessage,
        error: TransformError,
    ) -> Result<(), TransformError> {
        match self.on_error {
            TransformErrorPolicy::PassThrough => {
                message
                    .metadata
                    .insert(TRANSFORM_ERROR_KEY.to_string(), error.to_string());
                Ok(())
            }
            TransformErrorPolicy::Reject => Err(error),
        }
    }
}

// --- Publisher attach point ---

pub struct TransformPublisher {
    inner: Box<dyn MessagePublisher>,
    compiled: Arc<Compiled>,
}

impl TransformPublisher {
    pub fn new(
        inner: Box<dyn MessagePublisher>,
        config: &TransformMiddleware,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            compiled: Arc::new(Compiled::new(config)?),
        })
    }
}

#[async_trait]
impl MessagePublisher for TransformPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if self.compiled.is_noop() {
            return self.inner.send_batch(messages).await;
        }

        let mut forwarded = Vec::with_capacity(messages.len());
        let mut failed: Vec<(CanonicalMessage, PublisherError)> = Vec::new();
        for mut message in messages {
            match self.compiled.transform(&mut message) {
                Ok(()) => forwarded.push(message),
                Err(error) => match self.compiled.handle_failure(&mut message, error) {
                    Ok(()) => forwarded.push(message),
                    Err(error) => failed.push((message, error.into())),
                },
            }
        }

        if failed.is_empty() {
            return self.inner.send_batch(forwarded).await;
        }

        let mut responses = None;
        if !forwarded.is_empty() {
            // A transport-level error covers the whole batch, so it takes precedence:
            // the route retries it, and the rejected messages come back through here.
            match self.inner.send_batch(forwarded).await? {
                SentBatch::Ack => {}
                SentBatch::Partial {
                    responses: inner_responses,
                    failed: inner_failed,
                } => {
                    responses = inner_responses;
                    failed.extend(inner_failed);
                }
            }
        }

        Ok(SentBatch::Partial { responses, failed })
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// --- Consumer attach point ---

pub struct TransformConsumer {
    inner: Box<dyn MessageConsumer>,
    compiled: Arc<Compiled>,
}

impl TransformConsumer {
    pub fn new(
        inner: Box<dyn MessageConsumer>,
        config: &TransformMiddleware,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            compiled: Arc::new(Compiled::new(config)?),
        })
    }
}

#[async_trait]
impl MessageConsumer for TransformConsumer {
    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }

    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        loop {
            let received = self.inner.receive().await?;
            if self.compiled.is_noop() {
                return Ok(received);
            }
            let Received {
                mut message,
                commit,
            } = received;
            let outcome = self
                .compiled
                .transform(&mut message)
                .or_else(|error| self.compiled.handle_failure(&mut message, error));
            match outcome {
                Ok(()) => return Ok(Received { message, commit }),
                Err(error) => {
                    tracing::error!(
                        message_id = format_args!("{:032x}", message.message_id),
                        "Rejecting invalid input message: {error}"
                    );
                    // Ack the rejected message so it is not redelivered forever, then wait
                    // for the next one. A data problem must not surface as a consumer
                    // error, which the route would treat as a reason to reconnect.
                    commit(MessageDisposition::Ack)
                        .await
                        .map_err(ConsumerError::Connection)?;
                }
            }
        }
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let batch = self.inner.receive_batch(max_messages).await?;
        if self.compiled.is_noop() {
            return Ok(batch);
        }

        let ReceivedBatch { messages, commit } = batch;
        let original_len = messages.len();
        let mut kept = Vec::with_capacity(original_len);
        let mut kept_indices: Vec<usize> = Vec::with_capacity(original_len);

        for (index, mut message) in messages.into_iter().enumerate() {
            match self.compiled.transform(&mut message) {
                Ok(()) => {
                    kept_indices.push(index);
                    kept.push(message);
                }
                Err(error) => match self.compiled.handle_failure(&mut message, error) {
                    Ok(()) => {
                        kept_indices.push(index);
                        kept.push(message);
                    }
                    Err(error) => {
                        tracing::error!(
                            message_id = format_args!("{:032x}", message.message_id),
                            "Rejecting invalid input message: {error}"
                        );
                    }
                },
            }
        }

        if kept.len() == original_len {
            return Ok(ReceivedBatch {
                messages: kept,
                commit,
            });
        }

        // Rejected slots are acked so they are not redelivered forever; the caller's
        // dispositions are placed back at the indices they actually came from, which
        // keeps at-least-once intact for the surviving messages.
        let remapped = Box::new(move |dispositions: Vec<MessageDisposition>| {
            let mut full = vec![MessageDisposition::Ack; original_len];
            for (slot, disposition) in kept_indices.into_iter().zip(dispositions) {
                full[slot] = disposition;
            }
            commit(full)
        });

        Ok(ReceivedBatch {
            messages: kept,
            commit: remapped,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::memory::{MemoryConsumer, MemoryPublisher};
    use serde_json::json;
    use std::sync::Mutex;

    fn config(value: Value) -> TransformMiddleware {
        serde_json::from_value(value).expect("test config should deserialize")
    }

    fn compiled(value: Value) -> Compiled {
        Compiled::new(&config(value)).expect("test config should compile")
    }

    /// Runs a payload through the engine and returns the resulting JSON.
    fn run(cfg: &Compiled, payload: Value) -> Result<Value, TransformError> {
        let mut message = CanonicalMessage::from(payload.to_string());
        cfg.transform(&mut message)?;
        Ok(serde_json::from_slice(&message.payload).expect("output should be valid JSON"))
    }

    // --- Path parsing ---

    #[test]
    fn test_path_parse_accepts_dollar_prefix_dots_and_indices() {
        let path = CompiledPath::parse("$.a.b[0]").unwrap();
        assert_eq!(
            path.segs,
            vec![
                Seg::Key("a".to_string()),
                Seg::Key("b".to_string()),
                Seg::Index(0)
            ]
        );

        // The `$.` prefix is optional.
        assert_eq!(
            CompiledPath::parse("a.b").unwrap().segs,
            vec![Seg::Key("a".to_string()), Seg::Key("b".to_string())]
        );

        // Consecutive indices, and an index directly on the root.
        assert_eq!(
            CompiledPath::parse("$.a[1][2]").unwrap().segs,
            vec![Seg::Key("a".to_string()), Seg::Index(1), Seg::Index(2)]
        );
        assert_eq!(
            CompiledPath::parse("$[3]").unwrap().segs,
            vec![Seg::Index(3)]
        );
    }

    #[test]
    fn test_path_get_returns_none_for_missing_or_wrong_shape() {
        let doc = json!({ "a": { "b": [10, 20] } });

        assert_eq!(
            CompiledPath::parse("$.a.b[1]").unwrap().get(&doc),
            Some(&json!(20))
        );
        assert_eq!(CompiledPath::parse("$.a.missing").unwrap().get(&doc), None);
        assert_eq!(CompiledPath::parse("$.a.b[9]").unwrap().get(&doc), None);
        // Indexing an object, or keying an array, simply misses rather than erroring.
        assert_eq!(CompiledPath::parse("$.a[0]").unwrap().get(&doc), None);
    }

    #[test]
    fn test_path_parse_rejects_malformed_specs() {
        assert!(CompiledPath::parse("$.a..b").is_err());
        assert!(CompiledPath::parse("$.a[1").is_err());
        assert!(CompiledPath::parse("$.a[x]").is_err());
    }

    // --- Mapping stage ---

    #[test]
    fn test_mapping_renames_fields() {
        // The exact example from the feature request.
        let cfg = compiled(json!({
            "mapping": {
                "firstName": "$.first_name",
                "lastName": "$.last_name",
                "id": "$.user_id",
            }
        }));

        let out = run(
            &cfg,
            json!({ "first_name": "John", "last_name": "Smith", "user_id": "42" }),
        )
        .unwrap();

        assert_eq!(
            out,
            json!({ "firstName": "John", "lastName": "Smith", "id": "42" })
        );
    }

    #[test]
    fn test_mapping_reads_nested_and_writes_nested() {
        let cfg = compiled(json!({
            "mapping": {
                "user.name": "$.profile.details.name",
                "user.city": "$.addresses[0].city",
                "flat": "$.top",
            }
        }));

        let out = run(
            &cfg,
            json!({
                "profile": { "details": { "name": "Ada" } },
                "addresses": [{ "city": "London" }, { "city": "Paris" }],
                "top": 1,
            }),
        )
        .unwrap();

        assert_eq!(
            out,
            json!({ "user": { "name": "Ada", "city": "London" }, "flat": 1 })
        );
    }

    #[test]
    fn test_mapping_omits_absent_optional_and_uses_defaults() {
        let cfg = compiled(json!({
            "mapping": {
                "present": "$.here",
                "absent": "$.nope",
                "defaulted": { "path": "$.nope", "default": "fallback" },
            }
        }));

        let out = run(&cfg, json!({ "here": "yes" })).unwrap();

        // `absent` is omitted entirely rather than emitted as null.
        assert_eq!(out, json!({ "present": "yes", "defaulted": "fallback" }));
    }

    #[test]
    fn test_mapping_required_missing_is_rejected() {
        let cfg = compiled(json!({
            "mapping": { "id": { "path": "$.user_id", "required": true } }
        }));

        let error = run(&cfg, json!({ "other": 1 })).unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingRequired);
        assert!(error.to_string().contains("$.user_id"), "{error}");
    }

    // --- Coercion ---

    #[test]
    fn test_coercion_matrix_accepts_every_safe_conversion() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": {
                    "int": { "type": "integer" },
                    "float": { "type": "number" },
                    "flag": { "type": "boolean" },
                    "text": { "type": "string" },
                }
            }
        }));

        let out = run(
            &cfg,
            json!({ "int": "42", "float": "3.5", "flag": "true", "text": 7 }),
        )
        .unwrap();

        assert_eq!(
            out,
            json!({ "int": 42, "float": 3.5, "flag": true, "text": "7" })
        );
    }

    #[test]
    fn test_coercion_accepts_both_boolean_spellings() {
        let cfg = compiled(json!({
            "schema": { "type": "object", "properties": { "flag": { "type": "boolean" } } }
        }));

        for (input, expected) in [("true", true), ("1", true), ("false", false), ("0", false)] {
            let out = run(&cfg, json!({ "flag": input })).unwrap();
            assert_eq!(out, json!({ "flag": expected }), "input {input}");
        }
    }

    #[test]
    fn test_coercion_failure_reports_field_path_and_is_non_retryable() {
        let cfg = compiled(json!({
            "schema": { "type": "object", "properties": { "user_id": { "type": "integer" } } }
        }));

        let error = run(&cfg, json!({ "user_id": "abc" })).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Coercion);
        assert_eq!(error.path, "$.user_id");
        assert!(error.to_string().contains("cannot coerce"), "{error}");

        // The DLQ path depends on this classification.
        let publisher_error: PublisherError = error.into();
        assert!(matches!(publisher_error, PublisherError::NonRetryable(_)));
    }

    #[test]
    fn test_coercion_disabled_reports_type_mismatch_instead() {
        let cfg = compiled(json!({
            "coerce": false,
            "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
        }));

        let error = run(&cfg, json!({ "n": "42" })).unwrap_err();
        assert_eq!(error.kind, ErrorKind::TypeMismatch);
    }

    #[test]
    fn test_nested_error_path_includes_array_index() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": { "qty": { "type": "integer" } }
                        }
                    }
                }
            }
        }));

        let error = run(
            &cfg,
            json!({ "items": [{ "qty": "1" }, { "qty": "oops" }] }),
        )
        .unwrap_err();
        assert_eq!(error.path, "$.items[1].qty");
    }

    // --- Defaults, required, nullable, enum ---

    #[test]
    fn test_defaults_are_applied_and_satisfy_required() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "required": ["status"],
                "properties": { "status": { "type": "string", "default": "new" } }
            }
        }));

        let out = run(&cfg, json!({})).unwrap();
        assert_eq!(out, json!({ "status": "new" }));
    }

    #[test]
    fn test_defaults_can_be_disabled() {
        let cfg = compiled(json!({
            "apply_defaults": false,
            "schema": {
                "type": "object",
                "properties": { "status": { "type": "string", "default": "new" } }
            }
        }));

        assert_eq!(run(&cfg, json!({})).unwrap(), json!({}));
    }

    #[test]
    fn test_required_without_default_is_rejected() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "required": ["id"],
                "properties": { "id": { "type": "integer" } }
            }
        }));

        let error = run(&cfg, json!({ "other": 1 })).unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingRequired);
        assert_eq!(error.path, "$.id");
    }

    #[test]
    fn test_nullable_accepts_null_in_both_spellings() {
        for schema in [
            json!({ "type": "object", "properties": { "note": { "type": "string", "nullable": true } } }),
            json!({ "type": "object", "properties": { "note": { "type": ["string", "null"] } } }),
        ] {
            let cfg = compiled(json!({ "schema": schema }));
            let out = run(&cfg, json!({ "note": null })).unwrap();
            assert_eq!(out, json!({ "note": null }));
        }
    }

    #[test]
    fn test_non_nullable_null_falls_back_to_default_then_fails() {
        let with_default = compiled(json!({
            "schema": {
                "type": "object",
                "properties": { "n": { "type": "integer", "default": 0 } }
            }
        }));
        assert_eq!(
            run(&with_default, json!({ "n": null })).unwrap(),
            json!({ "n": 0 })
        );

        let without_default = compiled(json!({
            "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
        }));
        let error = run(&without_default, json!({ "n": null })).unwrap_err();
        assert_eq!(error.kind, ErrorKind::TypeMismatch);
        assert_eq!(error.path, "$.n");
        assert!(
            error.to_string().contains("not nullable"),
            "null should be reported plainly, not as a coercion failure: {error}"
        );
    }

    #[test]
    fn test_invalid_enum_value_is_rejected() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": { "status": { "type": "string", "enum": ["new", "done"] } }
            }
        }));

        assert!(run(&cfg, json!({ "status": "new" })).is_ok());

        let error = run(&cfg, json!({ "status": "bogus" })).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Enum);
        assert_eq!(error.path, "$.status");
    }

    #[test]
    fn test_unknown_schema_keywords_are_ignored_not_rejected() {
        // A fuller schema can be pointed at without being rewritten.
        let cfg = compiled(json!({
            "schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "User",
                "additionalProperties": false,
                "type": "object",
                "properties": { "id": { "type": "integer", "minimum": 0 } }
            }
        }));

        assert_eq!(run(&cfg, json!({ "id": "5" })).unwrap(), json!({ "id": 5 }));
    }

    #[test]
    fn test_mapping_then_schema_run_in_order() {
        let cfg = compiled(json!({
            "mapping": { "id": "$.user_id", "name": "$.first_name" },
            "schema": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" }
                }
            }
        }));

        // "42" survives the mapping as a string, then the schema coerces it.
        let out = run(&cfg, json!({ "user_id": "42", "first_name": "John" })).unwrap();
        assert_eq!(out, json!({ "id": 42, "name": "John" }));
    }

    // --- Config plumbing ---

    #[test]
    fn test_non_json_payload_is_rejected_as_parse_error() {
        let cfg = compiled(json!({
            "schema": { "type": "object" }
        }));

        let mut message = CanonicalMessage::from("not json at all");
        let error = cfg.transform(&mut message).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Parse);
    }

    #[test]
    fn test_rust_default_matches_parsed_empty_config() {
        // A derived Default would make these false, so `TransformMiddleware::default()` in
        // Rust would silently disable coercion while the same empty YAML enables it.
        let from_rust = TransformMiddleware::default();
        let from_config = config(json!({}));

        assert!(from_rust.coerce);
        assert!(from_rust.apply_defaults);
        assert_eq!(from_rust.coerce, from_config.coerce);
        assert_eq!(from_rust.apply_defaults, from_config.apply_defaults);
        assert_eq!(from_rust.on_error, from_config.on_error);
    }

    #[test]
    fn test_config_with_neither_stage_is_a_noop() {
        assert!(compiled(json!({})).is_noop());
        // A stage being present is what disables the fast path.
        assert!(!compiled(json!({ "mapping": { "a": "$.b" } })).is_noop());
        assert!(!compiled(json!({ "schema": { "type": "object" } })).is_noop());
    }

    #[test]
    fn test_schema_and_schema_file_together_are_rejected() {
        let error = Compiled::new(&config(json!({
            "schema": { "type": "object" },
            "schema_file": "/tmp/does-not-matter.json",
        })))
        .unwrap_err();
        assert!(error.to_string().contains("not both"), "{error}");
    }

    #[test]
    fn test_schema_file_is_read_once_at_construction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.json");
        std::fs::write(
            &path,
            json!({ "type": "object", "properties": { "id": { "type": "integer" } } }).to_string(),
        )
        .unwrap();

        let cfg = compiled(json!({ "schema_file": path.to_str().unwrap() }));
        assert_eq!(run(&cfg, json!({ "id": "7" })).unwrap(), json!({ "id": 7 }));

        // Deleting the file afterwards must not affect the hot path.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(run(&cfg, json!({ "id": "8" })).unwrap(), json!({ "id": 8 }));
    }

    #[test]
    fn test_missing_schema_file_fails_at_construction() {
        let error = Compiled::new(&config(json!({
            "schema_file": "/definitely/not/here.json"
        })))
        .unwrap_err();
        assert!(
            error.to_string().contains("cannot read schema file"),
            "{error}"
        );
    }

    #[test]
    fn test_documented_yaml_config_deserializes_and_compiles() {
        // Mirrors the README example, so the documented surface stays honest.
        let yaml = r#"
middlewares:
  - transform:
      mapping:
        firstName: "$.first_name"
        lastName: "$.last_name"
        id: "$.user_id"
        "address.city": { path: "$.city", default: "unknown" }
      schema:
        type: object
        required: ["firstName", "id"]
        properties:
          firstName: { type: string }
          id: { type: integer }
          address:
            type: object
            properties:
              city: { type: string }
  - dlq:
      endpoint:
        memory: { topic: "rejected" }
memory:
  topic: "users"
"#;
        let endpoint: crate::models::Endpoint = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(endpoint.middlewares.len(), 2);

        let crate::models::Middleware::Transform(cfg) = &endpoint.middlewares[0] else {
            panic!("first middleware should be transform");
        };
        let compiled = Compiled::new(cfg).unwrap();

        let out = run(
            &compiled,
            json!({ "first_name": "John", "last_name": "Smith", "user_id": "42" }),
        )
        .unwrap();
        assert_eq!(
            out,
            json!({
                "firstName": "John",
                "lastName": "Smith",
                "id": 42,
                "address": { "city": "unknown" }
            })
        );
    }

    // --- Publisher attach point ---

    #[tokio::test]
    async fn test_publisher_forwards_transformed_payloads() {
        let inner = MemoryPublisher::new_local("transform_pub_ok", 10);
        let channel = inner.channel();
        let publisher = TransformPublisher::new(
            Box::new(inner),
            &config(json!({ "mapping": { "id": "$.user_id" } })),
        )
        .unwrap();

        publisher
            .send_batch(vec![CanonicalMessage::from(r#"{"user_id":"42"}"#)])
            .await
            .unwrap();

        let sent = channel.drain_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].get_payload_str(), r#"{"id":"42"}"#);
    }

    #[tokio::test]
    async fn test_publisher_reports_bad_message_as_non_retryable_and_sends_the_rest() {
        let inner = MemoryPublisher::new_local("transform_pub_partial", 10);
        let channel = inner.channel();
        let publisher = TransformPublisher::new(
            Box::new(inner),
            &config(json!({
                "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
            })),
        )
        .unwrap();

        let outcome = publisher
            .send_batch(vec![
                CanonicalMessage::from(r#"{"n":"1"}"#),
                CanonicalMessage::from(r#"{"n":"abc"}"#),
                CanonicalMessage::from(r#"{"n":"3"}"#),
            ])
            .await
            .unwrap();

        match outcome {
            SentBatch::Partial { failed, .. } => {
                assert_eq!(failed.len(), 1);
                assert_eq!(failed[0].0.get_payload_str(), r#"{"n":"abc"}"#);
                assert!(matches!(failed[0].1, PublisherError::NonRetryable(_)));
            }
            other => panic!("expected Partial, got {other:?}"),
        }

        // The two valid messages still went through.
        let sent = channel.drain_messages();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].get_payload_str(), r#"{"n":1}"#);
        assert_eq!(sent[1].get_payload_str(), r#"{"n":3}"#);
    }

    #[tokio::test]
    async fn test_publisher_pass_through_policy_annotates_instead_of_failing() {
        let inner = MemoryPublisher::new_local("transform_pub_passthrough", 10);
        let channel = inner.channel();
        let publisher = TransformPublisher::new(
            Box::new(inner),
            &config(json!({
                "on_error": "pass_through",
                "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
            })),
        )
        .unwrap();

        publisher
            .send_batch(vec![CanonicalMessage::from(r#"{"n":"abc"}"#)])
            .await
            .unwrap();

        let sent = channel.drain_messages();
        assert_eq!(sent.len(), 1);
        // Payload is untouched, and the reason is carried for downstream routing.
        assert_eq!(sent[0].get_payload_str(), r#"{"n":"abc"}"#);
        assert!(sent[0].metadata.contains_key(TRANSFORM_ERROR_KEY));
    }

    #[tokio::test]
    async fn test_noop_publisher_passes_invalid_json_straight_through() {
        let inner = MemoryPublisher::new_local("transform_pub_noop", 10);
        let channel = inner.channel();
        let publisher = TransformPublisher::new(Box::new(inner), &config(json!({}))).unwrap();

        publisher
            .send_batch(vec![CanonicalMessage::from("not json at all")])
            .await
            .unwrap();

        let sent = channel.drain_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].get_payload_str(), "not json at all");
    }

    #[tokio::test]
    async fn test_rejected_message_reaches_the_dlq_through_the_config_wiring() {
        use crate::models::{DeadLetterQueueMiddleware, Endpoint, Middleware};

        let dlq_endpoint = Endpoint::new_memory("transform_dlq_rejects", 10);
        let inner = MemoryPublisher::new_local("transform_dlq_main", 10);
        let main_channel = inner.channel();

        // Publisher middlewares are wrapped in list order, so the *last* entry is the
        // outermost layer: `dlq` must follow `transform` to catch its rejections.
        let mut output = Endpoint::new_memory("transform_dlq_main", 10);
        output.middlewares = vec![
            Middleware::Transform(config(json!({
                "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
            }))),
            Middleware::Dlq(Box::new(DeadLetterQueueMiddleware {
                endpoint: dlq_endpoint.clone(),
            })),
        ];

        let publisher = crate::middleware::apply_middlewares_to_publisher(
            Box::new(inner),
            &output,
            "test_route",
        )
        .await
        .unwrap();

        publisher
            .send(CanonicalMessage::from(r#"{"n":"not-a-number"}"#))
            .await
            .unwrap();
        publisher
            .send(CanonicalMessage::from(r#"{"n":"5"}"#))
            .await
            .unwrap();

        let dlq_channel = dlq_endpoint.channel().unwrap();
        let dlq_messages = dlq_channel.drain_messages();
        assert_eq!(
            dlq_messages.len(),
            1,
            "the invalid message should be dead-lettered"
        );
        // The DLQ receives the original payload, not a half-transformed one.
        assert_eq!(dlq_messages[0].get_payload_str(), r#"{"n":"not-a-number"}"#);

        let delivered = main_channel.drain_messages();
        assert_eq!(
            delivered.len(),
            1,
            "the valid message should still be delivered"
        );
        assert_eq!(delivered[0].get_payload_str(), r#"{"n":5}"#);
    }

    // --- Consumer attach point ---

    /// Inner consumer that yields one prepared batch and records the dispositions its
    /// commit is called with, so the index remapping can be asserted.
    struct RecordingConsumer {
        batch: Option<Vec<CanonicalMessage>>,
        recorded: Arc<Mutex<Option<Vec<MessageDisposition>>>>,
    }

    #[async_trait]
    impl MessageConsumer for RecordingConsumer {
        async fn receive(&mut self) -> Result<Received, ConsumerError> {
            Err(ConsumerError::EndOfStream)
        }

        async fn receive_batch(&mut self, _max: usize) -> Result<ReceivedBatch, ConsumerError> {
            let messages = self.batch.take().ok_or(ConsumerError::EndOfStream)?;
            let recorded = self.recorded.clone();
            Ok(ReceivedBatch {
                messages,
                commit: Box::new(move |dispositions| {
                    *recorded.lock().unwrap() = Some(dispositions);
                    Box::pin(async { Ok(()) })
                }),
            })
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_consumer_drops_invalid_messages_and_remaps_commit_indices() {
        let recorded = Arc::new(Mutex::new(None));
        let inner = RecordingConsumer {
            // Index 1 is invalid and will be dropped.
            batch: Some(vec![
                CanonicalMessage::from(r#"{"n":"1"}"#),
                CanonicalMessage::from(r#"{"n":"bad"}"#),
                CanonicalMessage::from(r#"{"n":"3"}"#),
            ]),
            recorded: recorded.clone(),
        };

        let mut consumer = TransformConsumer::new(
            Box::new(inner),
            &config(json!({
                "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
            })),
        )
        .unwrap();

        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].get_payload_str(), r#"{"n":1}"#);
        assert_eq!(batch.messages[1].get_payload_str(), r#"{"n":3}"#);

        // Nack the second surviving message: it must land on original index 2, and the
        // dropped index 1 must be acked rather than left to redeliver forever.
        (batch.commit)(vec![MessageDisposition::Ack, MessageDisposition::Nack])
            .await
            .unwrap();

        let dispositions = recorded.lock().unwrap().take().expect("commit was called");
        assert_eq!(dispositions.len(), 3);
        assert!(matches!(dispositions[0], MessageDisposition::Ack));
        assert!(matches!(dispositions[1], MessageDisposition::Ack));
        assert!(matches!(dispositions[2], MessageDisposition::Nack));
    }

    #[tokio::test]
    async fn test_consumer_passes_commit_through_untouched_when_nothing_is_dropped() {
        let recorded = Arc::new(Mutex::new(None));
        let inner = RecordingConsumer {
            batch: Some(vec![
                CanonicalMessage::from(r#"{"n":"1"}"#),
                CanonicalMessage::from(r#"{"n":"2"}"#),
            ]),
            recorded: recorded.clone(),
        };

        let mut consumer = TransformConsumer::new(
            Box::new(inner),
            &config(json!({
                "schema": { "type": "object", "properties": { "n": { "type": "integer" } } }
            })),
        )
        .unwrap();

        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        (batch.commit)(vec![MessageDisposition::Nack, MessageDisposition::Ack])
            .await
            .unwrap();

        let dispositions = recorded.lock().unwrap().take().expect("commit was called");
        assert_eq!(dispositions.len(), 2);
        assert!(matches!(dispositions[0], MessageDisposition::Nack));
    }

    #[tokio::test]
    async fn test_consumer_transforms_from_a_real_memory_endpoint() {
        let inner = MemoryConsumer::new_local("transform_consumer_in", 10);
        let channel = inner.channel();
        channel
            .send_message(CanonicalMessage::from(
                r#"{"first_name":"John","user_id":"42"}"#,
            ))
            .await
            .unwrap();

        let mut consumer = TransformConsumer::new(
            Box::new(inner),
            &config(json!({
                "mapping": { "firstName": "$.first_name", "id": "$.user_id" },
                "schema": {
                    "type": "object",
                    "required": ["firstName", "id"],
                    "properties": { "firstName": { "type": "string" }, "id": { "type": "integer" } }
                }
            })),
        )
        .unwrap();

        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 1);
        let out: Value = serde_json::from_slice(&batch.messages[0].payload).unwrap();
        assert_eq!(out, json!({ "firstName": "John", "id": 42 }));
    }
}
