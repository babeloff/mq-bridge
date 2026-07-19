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
//! Building that `Value` is the dominant cost — a seven-field object costs about fifteen
//! allocations, most of them for fields the schema leaves exactly as they were. So when
//! the configuration allows it (`fast_eligible`), `transform_fast` walks the payload's
//! top-level fields as borrowed spans and copies the untouched ones straight to the
//! output, parsing only the fields that actually need work and handing those to the very
//! same `apply`. It decides *whether* a field is worth parsing, never *how* it is
//! transformed, so the two routes cannot drift apart in behaviour; `fast_path_equivalence`
//! holds them to that, including the two differences it documents.
//!
//! Only the JSON Schema subset that matters for message integration is honoured:
//! `type`, `properties`, `required`, `default`, `items`, `nullable`, `enum`, plus
//! `contentMediaType`/`contentSchema` for embedded JSON. Other keywords are ignored
//! rather than rejected, so a fuller schema can be pointed at without being rewritten.

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
    Content,
    MissingRequired,
    TypeMismatch,
    Enum,
}

impl ErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            ErrorKind::Parse => "parse",
            ErrorKind::Coercion => "coercion",
            ErrorKind::Content => "content",
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
    /// Set when `contentMediaType` names a JSON media type: the string is parsed in place.
    /// `Some(None)` parses without validating the result, `Some(Some(_))` also applies
    /// `contentSchema` to it.
    content: Option<Option<Box<CompiledSchema>>>,
}

/// Whether `contentMediaType` names something we can parse as JSON. Covers the `+json`
/// structured suffix (`application/vnd.acme.order+json`) alongside `application/json`.
/// Parameters (`; charset=utf-8`) are stripped.
fn is_json_media_type(media_type: &str) -> bool {
    let base = media_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    base == "application/json" || base == "text/json" || base.ends_with("+json")
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

        // Embedded JSON is opt-in per field, never implied by `coerce`: parsing a string as
        // a document is a different operation from widening `"42"` to `42`, and the JSON
        // Schema spec requires the consumer to ask for it. A media type we cannot decode
        // (or one paired with a `contentEncoding` we do not implement) is ignored like any
        // other unsupported keyword, leaving the string untouched.
        let encoded = obj.contains_key("contentEncoding");
        let content = match obj.get("contentMediaType") {
            Some(Value::String(mt)) if is_json_media_type(mt) && !encoded => {
                let inner = match obj.get("contentSchema") {
                    Some(sub) => Some(Box::new(CompiledSchema::compile(sub)?)),
                    None => None,
                };
                Some(inner)
            }
            Some(Value::String(_)) | None => None,
            Some(_) => anyhow::bail!("schema 'contentMediaType' must be a string"),
        };

        Ok(Self {
            ty,
            nullable,
            properties,
            required,
            default: obj.get("default").cloned(),
            items,
            enum_values,
            content,
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

        // Decode embedded JSON before recursing: the decoded document, not the string that
        // carried it, is what `contentSchema` describes.
        if let Some(content_schema) = &self.content {
            if let Value::String(raw) = value {
                *value = serde_json::from_str(raw).map_err(|e| {
                    TransformError::new(
                        render_path(crumbs),
                        ErrorKind::Content,
                        format!("contentMediaType is JSON but the string does not parse: {e}"),
                    )
                })?;
                if let Some(schema) = content_schema {
                    // Same crumbs, so a failure inside reads as `$.payload.qty`.
                    schema.apply(value, crumbs, opts)?;
                }
                // `properties`/`items` here belong to the string, not to what it decoded to.
                return Ok(());
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

    /// True when `apply` would leave `raw` — one field's verbatim JSON span — byte for
    /// byte as it is, so the field can be copied straight to the output and never has to
    /// become a `Value`. Deliberately conservative: anything with an obligation attached
    /// (`enum`, `contentMediaType`, sub-schemas, a default that a null could pull in)
    /// answers `false` and takes the normal path.
    fn is_passthrough(&self, raw: &str) -> bool {
        if self.enum_values.is_some()
            || self.content.is_some()
            || self.default.is_some()
            || self.items.is_some()
            || !self.properties.is_empty()
            || !self.required.is_empty()
        {
            return false;
        }
        // Nothing declared: `apply` would only recurse, and there is nothing to recurse
        // into without `properties` or `items`.
        let Some(ty) = self.ty else {
            return true;
        };
        let bytes = raw.as_bytes();
        match (ty, bytes.first()) {
            (Ty::String, Some(b'"'))
            | (Ty::Object, Some(b'{'))
            | (Ty::Array, Some(b'['))
            | (Ty::Boolean, Some(b't' | b'f'))
            | (Ty::Null, Some(b'n')) => true,
            // Looking like a number is not enough for either numeric type: `Ty::matches`
            // runs against a `Value`, which holds integers as i64/u64 and everything else
            // as f64. A 24-digit id or `1e400` is well-formed JSON that no `Value` can
            // represent, and the normal path rejects it — so parse before waving it
            // through. This also rules out fractions and exponents for `integer`.
            (Ty::Integer, Some(b'-' | b'0'..=b'9')) => {
                raw.parse::<i64>().is_ok() || raw.parse::<u64>().is_ok()
            }
            (Ty::Number, Some(b'-' | b'0'..=b'9')) => raw.parse::<f64>().is_ok_and(f64::is_finite),
            _ => false,
        }
    }

    /// True when decoding an embedded JSON document is the *only* thing `apply` would do
    /// to `raw`. That decode is a string unescape followed by a well-formedness check,
    /// both of which work on bytes, so no `Value` has to be built for the document —
    /// which for a document of any size is the bulk of the field's cost.
    ///
    /// A `contentSchema` means the document is inspected afterwards and does not qualify.
    fn is_plain_content_decode(&self, raw: &str) -> bool {
        matches!(self.content, Some(None))
            && matches!(self.ty, None | Some(Ty::String))
            && self.enum_values.is_none()
            && self.default.is_none()
            && self.items.is_none()
            && self.properties.is_empty()
            && self.required.is_empty()
            && raw.as_bytes().first() == Some(&b'"')
    }
}

/// The top-level fields of an object, borrowed from the payload: each key as written and
/// the verbatim JSON span of its value. Keys carrying escapes cannot be borrowed, and
/// deserialising then fails, which is exactly when the caller should fall back.
struct RawPairs<'a>(Vec<(&'a str, &'a serde_json::value::RawValue)>);

impl<'de> serde::Deserialize<'de> for RawPairs<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PairVisitor;

        impl<'de> serde::de::Visitor<'de> for PairVisitor {
            type Value = RawPairs<'de>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<RawPairs<'de>, A::Error> {
                let mut pairs = Vec::with_capacity(map.size_hint().unwrap_or(8));
                while let Some(entry) =
                    map.next_entry::<&'de str, &'de serde_json::value::RawValue>()?
                {
                    pairs.push(entry);
                }
                Ok(RawPairs(pairs))
            }
        }

        deserializer.deserialize_map(PairVisitor)
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
    /// Decided once: whether `transform_fast` may be tried at all.
    fast_eligible: bool,
    /// Decided once: see `map_sorts_keys`.
    sort_keys: bool,
}

/// Whether `serde_json::Map` iterates its keys in sorted order.
///
/// `Map` is a `BTreeMap` (sorted) normally and an `IndexMap` (insertion order) when
/// anything in the build enables `serde_json/preserve_order` — a decision made by feature
/// unification, which this crate cannot see with `cfg`. `transform_fast` writes its keys
/// directly rather than through a `Map`, so it has to emit them the way the normal path
/// would, and the only dependable way to learn which `Map` is compiled in is to ask one.
/// Called once per middleware, never on the hot path.
fn map_sorts_keys() -> bool {
    let mut probe = Map::new();
    probe.insert("b".to_string(), Value::Null);
    probe.insert("a".to_string(), Value::Null);
    probe.keys().next().is_some_and(|first| first == "a")
}

/// Whether the root schema is a plain object carrying no obligation of its own, which is
/// what lets a field-by-field rewrite stand in for parsing the whole payload. Root-level
/// `required` and defaults need to know which fields are *absent*, which copying spans
/// never learns, so they rule the shortcut out.
fn fast_eligible(rules: &[CompiledRule], schema: Option<&CompiledSchema>, opts: Opts) -> bool {
    if !rules.is_empty() {
        return false;
    }
    let Some(schema) = schema else {
        return false;
    };
    matches!(schema.ty, None | Some(Ty::Object))
        && schema.enum_values.is_none()
        && schema.content.is_none()
        && schema.items.is_none()
        && schema.default.is_none()
        && schema.required.is_empty()
        && !(opts.apply_defaults && schema.properties.iter().any(|(_, s)| s.default.is_some()))
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

        let opts = Opts {
            coerce: config.coerce,
            apply_defaults: config.apply_defaults,
        };
        Ok(Self {
            fast_eligible: fast_eligible(&rules, schema.as_ref(), opts),
            sort_keys: map_sorts_keys(),
            rules,
            schema,
            opts,
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

    /// Rewrites the payload field by field, copying the verbatim JSON span of every field
    /// the schema would not change and parsing only the ones that need work. Those go
    /// through the same `apply` as the normal path, so nesting, `items`, `enum` and
    /// `contentSchema` all behave identically — this decides *whether* a field is worth
    /// parsing, never *how* it is transformed.
    ///
    /// `None` means the payload's shape rules the shortcut out and the caller should fall
    /// back; `Some(Err(_))` is a real transform failure and must not be retried slowly.
    fn transform_fast(
        &self,
        schema: &CompiledSchema,
        payload: &[u8],
    ) -> Option<Result<Vec<u8>, TransformError>> {
        // Not an object, or a key we cannot borrow because it carried escapes.
        let RawPairs(mut pairs) = serde_json::from_slice::<RawPairs>(payload).ok()?;

        // The output has to carry its keys the way the normal path's `Map` would order
        // them. Insertion order needs no work: it is the order they were just read in.
        if self.sort_keys {
            pairs.sort_by(|a, b| a.0.cmp(b.0));
        }

        // A `Value` parse collapses duplicate keys (last wins) where copying spans would
        // emit both, so those rare payloads go the normal way. Quadratic, but objects are
        // narrow and this runs once per message.
        if pairs
            .iter()
            .enumerate()
            .any(|(i, (key, _))| pairs[..i].iter().any(|(seen, _)| seen == key))
        {
            return None;
        }

        let mut out = Vec::with_capacity(payload.len() + payload.len() / 2);
        out.push(b'{');
        for (i, (key, raw)) in pairs.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.push(b'"');
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(b"\":");

            let sub = schema
                .properties
                .binary_search_by(|(name, _)| name.as_str().cmp(key))
                .ok()
                .map(|idx| &schema.properties[idx].1);

            match sub {
                // Embedded JSON with nothing to check afterwards: unescape the string and
                // emit the document it carried, without ever building it.
                Some(sub) if sub.is_plain_content_decode(raw.get()) => {
                    // serde_json does the unescaping, so `😀` and friends are
                    // handled exactly as the normal path handles them.
                    let text: std::borrow::Cow<'_, str> = match serde_json::from_str(raw.get()) {
                        Ok(text) => text,
                        Err(e) => {
                            return Some(Err(TransformError::new(
                                format!("$.{key}"),
                                ErrorKind::Parse,
                                format!("field is not valid JSON: {e}"),
                            )))
                        }
                    };
                    match serde_json::from_str::<&serde_json::value::RawValue>(&text) {
                        Ok(document) => out.extend_from_slice(document.get().as_bytes()),
                        Err(e) => {
                            return Some(Err(TransformError::new(
                                format!("$.{key}"),
                                ErrorKind::Content,
                                format!(
                                    "contentMediaType is JSON but the string does not parse: {e}"
                                ),
                            )))
                        }
                    }
                }
                // The schema has something to say about this field and the raw bytes do
                // not already satisfy it: parse just this field and transform it.
                Some(sub) if !sub.is_passthrough(raw.get()) => {
                    let mut value: Value = match serde_json::from_str(raw.get()) {
                        Ok(value) => value,
                        Err(e) => {
                            return Some(Err(TransformError::new(
                                format!("$.{key}"),
                                ErrorKind::Parse,
                                format!("field is not valid JSON: {e}"),
                            )))
                        }
                    };
                    let mut crumbs = vec![Crumb::Key(key)];
                    if let Err(e) = sub.apply(&mut value, &mut crumbs, self.opts) {
                        return Some(Err(e));
                    }
                    if let Err(e) = serde_json::to_writer(&mut out, &value) {
                        return Some(Err(TransformError::new(
                            format!("$.{key}"),
                            ErrorKind::Parse,
                            format!("transformed value could not be serialized: {e}"),
                        )));
                    }
                }
                // Unmentioned by the schema, or already satisfying it.
                _ => out.extend_from_slice(raw.get().as_bytes()),
            }
        }
        out.push(b'}');
        Some(Ok(out))
    }

    /// Parses once, reshapes, serialises once.
    fn transform(&self, message: &mut CanonicalMessage) -> Result<(), TransformError> {
        if self.fast_eligible {
            if let Some(schema) = &self.schema {
                if let Some(result) = self.transform_fast(schema, &message.payload) {
                    message.payload = Bytes::from(result?);
                    return Ok(());
                }
            }
        }

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

        // Sized from the input rather than left at serde_json's 128-byte default: the
        // output tracks the input closely, so this is usually the only allocation the
        // write side makes.
        let mut bytes = Vec::with_capacity(message.payload.len() + message.payload.len() / 2);
        serde_json::to_writer(&mut bytes, &value).map_err(|e| {
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

    // --- Embedded JSON (contentMediaType / contentSchema) ---

    #[test]
    fn test_content_schema_parses_embedded_json_and_applies_the_inner_schema() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "string",
                        "contentMediaType": "application/json",
                        "contentSchema": {
                            "type": "object",
                            "properties": { "qty": { "type": "integer" } },
                        },
                    },
                },
            }
        }));

        // The inner `"7"` proves the decoded document goes through the same coercion pass.
        let out = run(&cfg, json!({ "payload": "{\"qty\": \"7\"}" })).unwrap();

        assert_eq!(out, json!({ "payload": { "qty": 7 } }));
    }

    #[test]
    fn test_content_media_type_alone_parses_without_validating() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": {
                    "payload": { "type": "string", "contentMediaType": "application/json" },
                },
            }
        }));

        let out = run(&cfg, json!({ "payload": "[1, 2]" })).unwrap();

        assert_eq!(out, json!({ "payload": [1, 2] }));
    }

    #[test]
    fn test_content_schema_rejects_a_string_that_is_not_json() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": {
                    "payload": { "type": "string", "contentMediaType": "application/json" },
                },
            }
        }));

        let error = run(&cfg, json!({ "payload": "not json" })).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Content);
        assert!(error.to_string().contains("$.payload"), "{error}");
    }

    #[test]
    fn test_structured_suffix_media_type_is_parsed() {
        let cfg = compiled(json!({
            "schema": {
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "string",
                        "contentMediaType": "application/vnd.acme.order+json; charset=utf-8",
                    },
                },
            }
        }));

        let out = run(&cfg, json!({ "payload": "{\"a\": 1}" })).unwrap();

        assert_eq!(out, json!({ "payload": { "a": 1 } }));
    }

    #[test]
    fn test_unparseable_content_keywords_leave_the_string_untouched() {
        // A non-JSON media type, an encoding we do not implement, and a `contentSchema`
        // with no media type are all ignored like any other unsupported keyword, so a
        // fuller pre-existing schema stays usable.
        for schema in [
            json!({ "type": "string", "contentMediaType": "text/csv" }),
            json!({
                "type": "string",
                "contentMediaType": "application/json",
                "contentEncoding": "base64",
            }),
            json!({ "type": "string", "contentSchema": { "type": "object" } }),
        ] {
            let cfg = compiled(json!({
                "schema": { "type": "object", "properties": { "payload": schema } }
            }));

            let out = run(&cfg, json!({ "payload": "{\"a\": 1}" })).unwrap();

            assert_eq!(out, json!({ "payload": "{\"a\": 1}" }));
        }
    }

    #[test]
    fn test_root_schema_decodes_a_double_encoded_body() {
        let cfg = compiled(json!({
            "schema": {
                "type": "string",
                "contentMediaType": "application/json",
                "contentSchema": {
                    "type": "object",
                    "properties": { "id": { "type": "integer" } },
                },
            }
        }));

        let out = run(&cfg, json!("{\"id\": \"5\"}")).unwrap();

        assert_eq!(out, json!({ "id": 5 }));
    }

    #[test]
    fn test_coerce_alone_never_turns_a_string_into_an_object() {
        // The guarantee that keeps embedded JSON opt-in: `coerce` widens scalars only.
        let cfg = compiled(json!({
            "schema": { "type": "object", "properties": { "payload": { "type": "object" } } }
        }));

        let error = run(&cfg, json!({ "payload": "{\"a\": 1}" })).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Coercion);
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

#[cfg(test)]
mod fast_path_equivalence {
    use super::*;
    use serde_json::json;

    /// What the three runs of a payload produced.
    struct Outcomes {
        slow: Result<String, String>,
        fast: Result<String, String>,
        /// The fast path with `sort_keys` inverted — that is, how it behaves in a build
        /// whose `serde_json/preserve_order` setting differs from this one. `mq-bridge-app`
        /// is such a build (`rmcp` pulls the feature in), so without this the configuration
        /// the binary actually ships would never be exercised by these tests.
        fast_other_order: Result<String, String>,
        eligible: bool,
    }

    fn both(schema: Value, payload: &str) -> Outcomes {
        let config = TransformMiddleware {
            schema: Some(schema),
            ..Default::default()
        };
        let mut compiled = Compiled::new(&config).unwrap();
        let eligible = compiled.fast_eligible;
        let sort_keys = compiled.sort_keys;

        let run = |compiled: &Compiled| {
            let mut message = CanonicalMessage::new(payload.as_bytes().to_vec(), None);
            match compiled.transform(&mut message) {
                Ok(()) => Ok(String::from_utf8(message.payload.to_vec()).unwrap()),
                Err(e) => Err(format!("{}:{}", e.kind.as_str(), e.path)),
            }
        };

        compiled.fast_eligible = false;
        let slow = run(&compiled);

        compiled.fast_eligible = eligible;
        let fast = run(&compiled);

        compiled.sort_keys = !sort_keys;
        let fast_other_order = run(&compiled);

        Outcomes {
            slow,
            fast,
            fast_other_order,
            eligible,
        }
    }

    fn as_json(r: &Result<String, String>) -> Result<Value, String> {
        match r {
            Ok(s) => Ok(serde_json::from_str(s).expect("valid JSON out")),
            Err(e) => Err(e.clone()),
        }
    }

    /// Compares outcomes as parsed JSON: object key order and escape spelling are
    /// serialization choices, not data. Both key orderings must agree with the normal path.
    #[track_caller]
    fn assert_same(schema: Value, payload: &str) {
        let out = both(schema, payload);
        assert_eq!(
            as_json(&out.slow),
            as_json(&out.fast),
            "paths disagree on payload: {payload}"
        );
        assert_eq!(
            as_json(&out.slow),
            as_json(&out.fast_other_order),
            "paths disagree under the opposite key ordering on payload: {payload}"
        );
    }

    /// Same as `assert_same`, and additionally requires the fast path to have been taken —
    /// so a case meant to exercise it cannot silently start falling back and still pass.
    #[track_caller]
    fn assert_same_via_fast(schema: Value, payload: &str) {
        let out = both(schema.clone(), payload);
        assert!(
            out.eligible,
            "expected the fast path to be eligible for {schema}"
        );
        assert_same(schema, payload);
    }

    /// Stronger than `assert_same`: byte for byte, for the key ordering this build uses.
    #[track_caller]
    fn assert_byte_identical(schema: Value, payload: &str) {
        let out = both(schema.clone(), payload);
        assert!(
            out.eligible,
            "expected the fast path to be eligible for {schema}"
        );
        assert_eq!(
            out.slow, out.fast,
            "byte output differs for payload: {payload}"
        );
    }

    /// `transform_fast` writes keys itself instead of going through a `serde_json::Map`, so
    /// it consults `map_sorts_keys` to order them the way the normal path would. That probe
    /// has to describe the `Map` this build actually compiled in, whichever it is.
    #[test]
    fn map_sort_probe_matches_reality() {
        let mut map = Map::new();
        map.insert("b".to_string(), Value::from(1));
        map.insert("a".to_string(), Value::from(2));
        let serialized = serde_json::to_string(&Value::Object(map)).unwrap();
        assert_eq!(
            map_sorts_keys(),
            serialized.starts_with(r#"{"a""#),
            "map_sorts_keys disagrees with how this build's Map serialises: {serialized}"
        );
    }
    fn scalars() -> Value {
        json!({"type":"object","properties":{
            "s":{"type":"string"},
            "i":{"type":"integer"},
            "n":{"type":"number"},
            "b":{"type":"boolean"},
            "o":{"type":"object"},
            "a":{"type":"array"}}})
    }

    #[test]
    fn values_already_matching_their_type_are_untouched() {
        assert_same_via_fast(
            scalars(),
            r#"{"s":"x","i":42,"n":1.5,"b":true,"o":{"k":[1,2]},"a":[1,"two",null]}"#,
        );
    }

    #[test]
    fn every_coercion_agrees() {
        assert_same_via_fast(
            scalars(),
            r#"{"s":7,"i":"42","n":"1.5","b":"true","o":{},"a":[]}"#,
        );
        assert_same_via_fast(scalars(), r#"{"b":"0","i":"-8","n":"-2.5e3"}"#);
    }

    #[test]
    fn a_float_is_not_an_integer_even_though_it_starts_like_one() {
        // The byte check must not wave `1.5` or `1e3` through as integers.
        assert_same_via_fast(scalars(), r#"{"i":1.5}"#);
        assert_same_via_fast(scalars(), r#"{"i":1e3}"#);
        assert_same_via_fast(scalars(), r#"{"i":-0.0}"#);
    }

    #[test]
    fn coercion_failures_agree() {
        assert_same_via_fast(scalars(), r#"{"i":"not-a-number"}"#);
        assert_same_via_fast(scalars(), r#"{"b":"maybe"}"#);
        assert_same_via_fast(scalars(), r#"{"i":{}}"#);
    }

    #[test]
    fn embedded_documents_agree() {
        let schema = json!({"type":"object","properties":{
            "p":{"type":"string","contentMediaType":"application/json"}}});
        assert_same_via_fast(schema.clone(), r#"{"p":"{\"a\":1,\"b\":[1,2]}"}"#);
        assert_same_via_fast(schema.clone(), r#"{"p":"[1,2,3]"}"#);
        assert_same_via_fast(schema.clone(), r#"{"p":"null"}"#);
        assert_same_via_fast(schema.clone(), r#"{"p":"\"just a string\""}"#);
        // Malformed embedded JSON must fail the same way on both paths.
        assert_same_via_fast(schema.clone(), r#"{"p":"{not json}"}"#);
        // Escapes inside the embedded document, including a surrogate pair.
        assert_same_via_fast(schema.clone(), r#"{"p":"{\"e\":\"a\\\"b\\nc\"}"}"#);
        assert_same_via_fast(schema, r#"{"p":"{\"e\":\"\\ud83d\\ude00\"}"}"#);
    }

    #[test]
    fn a_content_schema_still_validates_the_decoded_document() {
        let schema = json!({"type":"object","properties":{
            "p":{"type":"string","contentMediaType":"application/json",
                 "contentSchema":{"type":"object","properties":{"n":{"type":"integer"}}}}}});
        assert_same(schema.clone(), r#"{"p":"{\"n\":\"5\"}"}"#);
        assert_same(schema, r#"{"p":"{\"n\":\"oops\"}"}"#);
    }

    #[test]
    fn nested_schemas_agree() {
        let schema = json!({"type":"object","properties":{
            "outer":{"type":"object","properties":{
                "inner":{"type":"integer"},
                "deep":{"type":"object","properties":{"x":{"type":"boolean"}}}}},
            "list":{"type":"array","items":{"type":"integer"}}}});
        assert_same_via_fast(
            schema.clone(),
            r#"{"outer":{"inner":"3","deep":{"x":"true"}},"list":["1","2"]}"#,
        );
        // A single violation is reported identically.
        assert_same_via_fast(schema, r#"{"outer":{"inner":"bad"},"list":["1","2"]}"#);
    }

    /// A documented, deliberate difference. The normal path looks for violations in
    /// schema order; the fast path finds them in the order the payload lists its fields,
    /// which is not the same when keys are not sorted. A message violating the schema in
    /// more than one place is rejected either way — only the field named in the error
    /// differs. Making these agree would mean transforming in schema order and emitting in
    /// payload order, i.e. buffering every field, which costs more than the diagnostic is
    /// worth. Asserted so it cannot change unnoticed.
    #[test]
    fn known_difference_which_violation_is_reported_when_several() {
        let schema = json!({"type":"object","properties":{
            "outer":{"type":"object","properties":{"inner":{"type":"integer"}}},
            "list":{"type":"array","items":{"type":"integer"}}}});
        let out = both(schema, r#"{"outer":{"inner":"bad"},"list":["1","x"]}"#);
        assert_eq!(out.slow, Err("coercion:$.list[1]".to_string()));
        // Sorted keys visit `list` first, matching the normal path exactly.
        assert_eq!(out.fast, Err("coercion:$.list[1]".to_string()));
        // Insertion order reaches `outer` first and names that instead. Still rejected.
        assert_eq!(
            out.fast_other_order,
            Err("coercion:$.outer.inner".to_string())
        );
    }

    #[test]
    fn enums_agree() {
        let schema = json!({"type":"object","properties":{
            "e":{"type":"string","enum":["a","b"]}}});
        assert_same_via_fast(schema.clone(), r#"{"e":"a"}"#);
        assert_same_via_fast(schema, r#"{"e":"z"}"#);
    }

    #[test]
    fn nullability_agrees() {
        let schema = json!({"type":"object","properties":{
            "n":{"type":["string","null"]},
            "s":{"type":"string"}}});
        assert_same_via_fast(schema.clone(), r#"{"n":null,"s":"x"}"#);
        // Null against a non-nullable field must fail identically.
        assert_same_via_fast(schema, r#"{"n":"x","s":null}"#);
    }

    #[test]
    fn fields_the_schema_never_mentions_are_carried_through() {
        assert_same_via_fast(
            scalars(),
            r#"{"s":"x","extra":{"deep":[1,{"k":"v"}]},"another":null}"#,
        );
    }

    #[test]
    fn string_escapes_and_unicode_survive_the_byte_copy() {
        assert_same_via_fast(
            scalars(),
            r#"{"s":"tab\there \"quoted\" \\ back / slash é 😀"}"#,
        );
    }

    #[test]
    fn shapes_that_must_fall_back_still_agree() {
        // Root-level `required` and defaults need to know what is absent.
        let required = json!({"type":"object","required":["a"],
                              "properties":{"a":{"type":"string"}}});
        assert_same(required.clone(), r#"{"a":"x"}"#);
        assert_same(required, r#"{"b":"x"}"#);

        let defaulted = json!({"type":"object","properties":{
            "a":{"type":"string","default":"filled"}}});
        assert_same(defaulted.clone(), r#"{"b":1}"#);
        assert_same(defaulted, r#"{"a":null}"#);

        // A non-object payload has no fields to walk.
        assert_same(scalars(), r#"[1,2,3]"#);
        assert_same(scalars(), r#""bare string""#);

        // Duplicate keys collapse in a `Value`; copying spans would emit both.
        assert_same(scalars(), r#"{"s":"first","s":"second"}"#);

        // An escaped key cannot be borrowed, so the fast path declines it. These are
        // genuinely escaped: a quote, a newline, and a \u sequence inside the key.
        assert_same(scalars(), r#"{"a\"b":1,"s":"x"}"#);
        assert_same(scalars(), r#"{"a\nb":1,"s":"x"}"#);
        assert_same(scalars(), r#"{"a\u0041b":1,"s":"x"}"#);
        // A key that looks like it could close the object or inject a field.
        assert_same(scalars(), r#"{"a\":1,\"injected":1}"#);
    }

    #[test]
    fn integers_beyond_i64_agree() {
        // A `Value` holds integers as i64/u64, so a 24-digit id is not an `integer` and
        // must be rejected identically rather than waved through on a byte check.
        assert_same(scalars(), r#"{"i":999999999999999999999999}"#);
        assert_same(scalars(), r#"{"i":-999999999999999999999999}"#);
        // Just past i64::MAX but still a u64: accepted by both.
        assert_same(scalars(), r#"{"i":9223372036854775808}"#);
        assert_same(scalars(), r#"{"i":18446744073709551615}"#);
    }

    #[test]
    fn numbers_beyond_f64_are_rejected_by_both() {
        // Both paths reject these; only the reported path differs, because the fast path
        // can name the offending field where a whole-payload parse cannot.
        for payload in [r#"{"n":1e400}"#, r#"{"n":-1e400}"#] {
            let out = both(scalars(), payload);
            assert!(out.slow.is_err(), "normal path accepted {payload}");
            assert!(out.fast.is_err(), "fast path accepted {payload}");
        }
    }

    /// A documented, deliberate difference. A number too large for f64 sitting in a field
    /// the schema never mentions is copied through as bytes, because the fast path never
    /// parses fields it has nothing to say about — where a whole-payload parse rejects the
    /// message. Catching this would mean parsing every field and giving up the entire
    /// point of the fast path. Asserted so it cannot change unnoticed.
    #[test]
    fn known_difference_unrepresentable_number_in_an_unmentioned_field() {
        let out = both(scalars(), r#"{"unmentioned":1e400}"#);
        assert!(out.slow.is_err(), "normal path used to reject this");
        assert_eq!(out.fast, Ok(r#"{"unmentioned":1e400}"#.to_string()));
    }

    #[test]
    fn byte_output_is_identical_for_ordinary_payloads() {
        assert_byte_identical(scalars(), r#"{"s":"x","i":"42","n":"1.5","b":"true"}"#);
        assert_byte_identical(scalars(), r#"{"z":1,"a":2,"m":{"nested":[1,2]},"s":7}"#);
        assert_byte_identical(scalars(), r#"{"s":"quote \" and back \\ slash é"}"#);
    }

    #[test]
    fn malformed_payloads_agree() {
        assert_same(scalars(), r#"{"s":}"#);
        assert_same(scalars(), r#"not json at all"#);
        assert_same(scalars(), r#""#);
    }

    #[test]
    fn a_root_schema_without_properties_is_still_consistent() {
        assert_same(json!({"type":"object"}), r#"{"anything":[1,2]}"#);
        assert_same(json!({}), r#"{"anything":[1,2]}"#);
    }
}
