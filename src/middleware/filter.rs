//! Expression predicates: the `filter` middleware and the `switch` endpoint's
//! `when` cases.
//!
//! An expression reads the payload's top-level JSON fields as bare names
//! (`amount > 100`) and the message metadata under the reserved `meta` prefix
//! (`meta.http_status_code == '200'`). Metadata values are always strings, so a
//! numeric comparison needs `number(meta.retries) > 3`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use serde_json::{Map, Value};
use zen_expression::compiler::{FetchFastTarget, Opcode};
use zen_expression::expression::Standard;
use zen_expression::{compile_expression, Expression, Variable};

use super::deferred_commit::{run_all, DeferredCommits};
use super::raw_json::RawPairs;
use crate::traits::{
    BatchCommitFunc, BoxFuture, CommitFunc, ConsumerError, EndpointStatus, MessageConsumer,
    MessageDisposition, MessagePublisher, PublisherError, Received, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;

/// The reserved context key under which message metadata is exposed.
///
/// It shadows a payload field of the same name.
const METADATA_PREFIX: &str = "meta";

/// A compiled predicate over a message's payload and metadata.
pub(crate) struct CompiledFilter {
    expression: Expression<Standard>,
    fast_predicate: Option<FastPredicate>,
    /// Payload field paths the expression reads, e.g. `["order", "status"]`.
    payload_paths: Vec<Vec<String>>,
    /// Metadata keys the expression reads via the `meta` prefix.
    metadata_keys: Vec<String>,
    uses_all_metadata: bool,
    warned_unusable_field: AtomicBool,
}

struct FastPredicate {
    path: Vec<String>,
    expected: FastLiteral,
    negate: bool,
}

enum FastLiteral {
    Null,
    Bool(bool),
    Number(rust_decimal::Decimal),
    String(Arc<str>),
}

impl FastPredicate {
    fn evaluate(&self, document: &Value) -> bool {
        self.evaluate_value(resolve_any(document, &self.path))
    }

    fn evaluate_value(&self, actual: Option<&Value>) -> bool {
        let equal = match (&self.expected, actual) {
            (FastLiteral::Null, None | Some(Value::Null)) => true,
            (FastLiteral::Bool(expected), Some(Value::Bool(actual))) => expected == actual,
            (FastLiteral::Number(expected), Some(Value::Number(actual))) => {
                Variable::from(&Value::Number(actual.clone()))
                    .as_number()
                    .is_some_and(|actual| actual == *expected)
            }
            (FastLiteral::String(expected), Some(Value::String(actual))) => {
                expected.as_ref() == actual
            }
            _ => false,
        };
        equal ^ self.negate
    }

    fn metadata_key(&self) -> Option<&str> {
        (self.path.len() == 2 && self.path[0] == METADATA_PREFIX).then(|| self.path[1].as_str())
    }

    fn top_level_payload_key(&self) -> Option<&str> {
        (self.path.len() == 1 && self.path[0] != METADATA_PREFIX).then(|| self.path[0].as_str())
    }

    fn evaluate_metadata(&self, actual: Option<&String>) -> bool {
        let equal = match (&self.expected, actual) {
            (FastLiteral::Null, None) => true,
            (FastLiteral::String(expected), Some(actual)) => expected.as_ref() == actual,
            _ => false,
        };
        equal ^ self.negate
    }
}

/// Lazily prepared input shared by predicate cases.
pub(crate) struct FilterContext {
    document: Value,
    payload_loaded: bool,
}

impl FilterContext {
    pub(crate) fn new() -> Self {
        Self {
            document: Value::Object(Map::new()),
            payload_loaded: false,
        }
    }

    fn load_payload(&mut self, message: &CanonicalMessage) -> anyhow::Result<()> {
        if self.payload_loaded {
            return Ok(());
        }
        let mut document: Value = serde_json::from_slice(message.payload.as_ref())
            .context("filter requires a structured JSON object payload")?;
        let object = document
            .as_object_mut()
            .context("filter requires a structured JSON object payload")?;
        if let Some(meta) = self
            .document
            .as_object_mut()
            .and_then(|current| current.remove(METADATA_PREFIX))
        {
            object.insert(METADATA_PREFIX.to_string(), meta);
        }
        self.document = document;
        self.payload_loaded = true;
        Ok(())
    }

    fn add_metadata(&mut self, message: &CanonicalMessage, keys: &[String], all: bool) {
        if keys.is_empty() && !all {
            return;
        }
        let object = self
            .document
            .as_object_mut()
            .expect("filter context is an object");
        let meta = object
            .entry(METADATA_PREFIX)
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("filter metadata context is an object");
        // Overwrites: `meta` shadows a payload field of the same name, and re-inserting
        // the same message's metadata on a reused context is idempotent.
        if all {
            for (key, value) in &message.metadata {
                meta.insert(key.clone(), Value::String(value.clone()));
            }
        } else {
            for key in keys {
                let value = message
                    .metadata
                    .get(key)
                    .map_or(Value::Null, |value| Value::String(value.clone()));
                meta.insert(key.clone(), value);
            }
        }
    }
}

impl CompiledFilter {
    pub(crate) fn new(expression: &str) -> anyhow::Result<Self> {
        let normalized = normalize_expression(expression);
        let expression =
            compile_expression(&normalized).map_err(|error| anyhow!(error.to_string()))?;
        let fast_predicate = compile_fast_predicate(&expression);
        let (payload_paths, metadata_keys, uses_all_metadata, has_unsupported_path) =
            referenced_paths(&expression);
        // An indexed path never resolves, so tolerating it per message would drop every
        // message while the route reported itself healthy. Refuse to start instead.
        if has_unsupported_path {
            bail!(
                "filter expression uses an indexed path, which is unsupported; \
                 index into the array before the filter, or compare a scalar field"
            );
        }
        Ok(Self {
            expression,
            fast_predicate,
            payload_paths,
            metadata_keys,
            uses_all_metadata,
            warned_unusable_field: AtomicBool::new(false),
        })
    }

    /// Whether this message satisfies the expression.
    ///
    /// A field that is absent, null, or not a scalar means "does not match", the
    /// way a SQL `WHERE` treats NULL: one heterogeneous document should not end a
    /// route that is otherwise running fine. A payload that is not a JSON object
    /// is still an error, because that means the expression was pointed at data
    /// it cannot read at all, and silently dropping everything would be worse.
    pub(crate) fn matches(&self, message: &CanonicalMessage) -> anyhow::Result<bool> {
        self.matches_with_context(message, &mut FilterContext::new())
    }

    pub(crate) fn matches_with_context(
        &self,
        message: &CanonicalMessage,
        context: &mut FilterContext,
    ) -> anyhow::Result<bool> {
        if let Some(predicate) = &self.fast_predicate {
            if let Some(key) = predicate.metadata_key() {
                let actual = message.metadata.get(key);
                if actual.is_none() {
                    self.warn_unusable_field(&format!("{METADATA_PREFIX}.{key}"));
                }
                return Ok(predicate.evaluate_metadata(actual));
            }
            if let Some(key) = predicate.top_level_payload_key() {
                if let Ok(RawPairs(pairs)) = serde_json::from_slice(&message.payload) {
                    let raw = pairs
                        .iter()
                        .rev()
                        .find(|(candidate, _)| *candidate == key)
                        .map(|(_, value)| *value);
                    let value: Option<Value> =
                        raw.map(|raw| serde_json::from_str(raw.get())).transpose()?;
                    if value.as_ref().is_none_or(|value| {
                        value.is_null() || value.is_array() || value.is_object()
                    }) {
                        self.warn_unusable_field(key);
                    }
                    return Ok(predicate.evaluate_value(value.as_ref()));
                }
            }
        }

        if !self.payload_paths.is_empty() {
            context.load_payload(message)?;
        }

        let mut has_unusable_field = false;
        let mut synthesized = Vec::new();
        for path in &self.payload_paths {
            if resolve(&context.document, path).is_none() {
                has_unusable_field = true;
                self.warn_unusable_field(&path.join("."));
                if let Some(created) = insert_null_if_absent(&mut context.document, path) {
                    synthesized.push(created);
                }
            }
        }

        for key in &self.metadata_keys {
            if !message.metadata.contains_key(key) {
                has_unusable_field = true;
                self.warn_unusable_field(&format!("{METADATA_PREFIX}.{key}"));
            }
        }

        context.add_metadata(message, &self.metadata_keys, self.uses_all_metadata);

        let result = self.evaluate(&context.document, has_unusable_field);
        // A context is shared across a `switch`'s predicates, so the nulls this one
        // synthesized must not make the next one see an object where a field is absent.
        for path in synthesized {
            remove_path(&mut context.document, &path);
        }
        result
    }

    fn evaluate(&self, document: &Value, has_unusable_field: bool) -> anyhow::Result<bool> {
        if let Some(predicate) = &self.fast_predicate {
            return Ok(predicate.evaluate(document));
        }

        let evaluated = match self.expression.evaluate(Variable::from(document)) {
            Ok(evaluated) => evaluated,
            Err(_) if has_unusable_field => return Ok(false),
            Err(error) => {
                let mut text_fields = self
                    .payload_paths
                    .iter()
                    .filter(|path| resolve(document, path).is_some_and(Value::is_string))
                    .map(|path| path.join("."))
                    .collect::<Vec<_>>();
                text_fields.extend(
                    self.metadata_keys
                        .iter()
                        .map(|key| format!("{METADATA_PREFIX}.{key}")),
                );
                return Err(text_typed_field_error(&error.to_string(), &text_fields));
            }
        };
        match evaluated {
            Variable::Bool(value) => Ok(value),
            _ => bail!("filter expression did not evaluate to a boolean"),
        }
    }

    /// Warns once per route: a field that is never usable drops every message,
    /// and a typo in the expression should not just look like an empty source.
    fn warn_unusable_field(&self, field: &str) {
        if !self.warned_unusable_field.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                field,
                "filter field is absent, null, or not a scalar; those messages do not match"
            );
        }
    }
}

fn compile_fast_predicate(expression: &Expression<Standard>) -> Option<FastPredicate> {
    let opcodes = expression.bytecode();
    let (left, right, negate) = match opcodes.as_ref() {
        [left, right, Opcode::Equal] => (left, right, false),
        [left, right, Opcode::Equal, Opcode::Not] => (left, right, true),
        _ => return None,
    };

    let (path, expected) = parse_fast_fetch(left)
        .zip(parse_fast_literal(right))
        .or_else(|| parse_fast_fetch(right).zip(parse_fast_literal(left)))?;
    Some(FastPredicate {
        path,
        expected,
        negate,
    })
}

fn parse_fast_fetch(opcode: &Opcode) -> Option<Vec<String>> {
    match opcode {
        Opcode::FetchEnv(name) => Some(vec![name.to_string()]),
        Opcode::FetchFast(targets) => targets
            .iter()
            .map(|target| match target {
                FetchFastTarget::Root | FetchFastTarget::Begin => Some(None),
                FetchFastTarget::String(name) => Some(Some(name.to_string())),
                FetchFastTarget::Number(_) => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(|segments| segments.into_iter().flatten().collect()),
        _ => None,
    }
}

fn parse_fast_literal(opcode: &Opcode) -> Option<FastLiteral> {
    match opcode {
        Opcode::PushNull => Some(FastLiteral::Null),
        Opcode::PushBool(value) => Some(FastLiteral::Bool(*value)),
        Opcode::PushNumber(value) => Some(FastLiteral::Number(*value)),
        Opcode::PushString(value) => Some(FastLiteral::String(value.clone())),
        _ => None,
    }
}

/// Walks a dotted path, yielding the value only if it is a usable scalar.
fn resolve<'a>(document: &'a Value, path: &[String]) -> Option<&'a Value> {
    let current = resolve_any(document, path)?;
    let usable = !current.is_null() && !current.is_array() && !current.is_object();
    usable.then_some(current)
}

fn resolve_any<'a>(document: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = document;
    for segment in path {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

/// Makes a genuinely absent path visible to the expression VM as `null`.
///
/// Returns the shallowest prefix it had to create, so the caller can undo the whole
/// insertion with [`remove_path`].
fn insert_null_if_absent(document: &mut Value, path: &[String]) -> Option<Vec<String>> {
    let mut created = None;
    let mut current = document;
    for (index, segment) in path.iter().enumerate() {
        let Some(object) = current.as_object_mut() else {
            return created;
        };
        if created.is_none() && !object.contains_key(segment.as_str()) {
            created = Some(path[..=index].to_vec());
        }
        if index + 1 == path.len() {
            object.entry(segment.clone()).or_insert(Value::Null);
            return created;
        }
        current = object
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    created
}

/// Removes what [`insert_null_if_absent`] added.
fn remove_path(document: &mut Value, path: &[String]) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut current = document;
    for segment in parents {
        let Some(next) = current.as_object_mut().and_then(|o| o.get_mut(segment)) else {
            return;
        };
        current = next;
    }
    if let Some(object) = current.as_object_mut() {
        object.remove(last.as_str());
    }
}

/// Splits the paths the compiled expression reads into payload and metadata.
fn referenced_paths(
    expression: &Expression<Standard>,
) -> (Vec<Vec<String>>, Vec<String>, bool, bool) {
    let mut payload = Vec::new();
    let mut metadata = Vec::new();
    let mut uses_all_metadata = false;
    let mut has_unsupported_path = false;

    for opcode in expression.bytecode().iter() {
        let path: Vec<String> = match opcode {
            Opcode::FetchEnv(name) => vec![name.to_string()],
            Opcode::FetchFast(targets) => {
                if targets
                    .iter()
                    .any(|target| matches!(target, FetchFastTarget::Number(_)))
                {
                    has_unsupported_path = true;
                    continue;
                }
                targets
                    .iter()
                    .filter_map(|target| match target {
                        FetchFastTarget::String(name) => Some(name.to_string()),
                        FetchFastTarget::Root | FetchFastTarget::Begin => None,
                        FetchFastTarget::Number(_) => {
                            unreachable!("numeric targets were rejected above")
                        }
                    })
                    .collect()
            }
            _ => continue,
        };
        if path.is_empty() {
            continue;
        }

        if path[0] == METADATA_PREFIX {
            // A bare `meta` with no key reads the whole map; nothing to check.
            if let Some(key) = path.get(1) {
                if !metadata.contains(key) {
                    metadata.push(key.clone());
                }
            } else {
                uses_all_metadata = true;
            }
        } else if !payload.contains(&path) {
            payload.push(path);
        }
    }

    (payload, metadata, uses_all_metadata, has_unsupported_path)
}

/// Turns the engine's opcode-level type error into one naming the field and the fix.
///
/// A text-typed column makes `amount > 100` compare a string against a number,
/// which the expression VM reports only as `Opcode Compare: Unsupported type`.
/// That names neither the column nor `number()`, so the route looks broken rather
/// than under-specified.
///
/// Which fields arrive as text depends on the source, so the hint names both
/// shapes rather than asserting one: CSV and most key-value stores type
/// everything as text, while a SQL source types most columns natively and
/// delivers only `numeric`/`timestamptz` as strings. Metadata is always text.
fn text_typed_field_error(error: &str, text_fields: &[String]) -> anyhow::Error {
    let Some(first) = text_fields.first() else {
        return anyhow!(error.to_string());
    };
    let fields = text_fields.join("`, `");
    anyhow!(
        "{error}; filter field `{fields}` holds text, not a number — compare it as \
         `number({first})` (metadata is always text, as are all CSV fields; SQL sources \
         deliver numeric and timestamp columns as strings)"
    )
}

/// Rewrites `&&` and `||` to the `and`/`or` the expression engine accepts.
///
/// The engine's lexer rejects the C-style spellings outright, and reaching for
/// them is the first thing anyone does.
fn normalize_expression(expression: &str) -> String {
    let mut normalized = String::with_capacity(expression.len());
    let mut chars = expression.chars().peekable();
    let mut quote = None;
    let mut escaped = false;

    while let Some(character) = chars.next() {
        if let Some(delimiter) = quote {
            normalized.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }

        match character {
            '\'' | '"' => {
                quote = Some(character);
                normalized.push(character);
            }
            '&' if chars.next_if_eq(&'&').is_some() => normalized.push_str(" and "),
            '|' if chars.next_if_eq(&'|').is_some() => normalized.push_str(" or "),
            _ => normalized.push(character),
        }
    }

    normalized
}

/// Drops messages that do not match, before anything downstream sees them.
pub struct FilterConsumer {
    inner: Box<dyn MessageConsumer>,
    filter: CompiledFilter,
    deferred: DeferredCommits,
}

impl FilterConsumer {
    pub fn new(inner: Box<dyn MessageConsumer>, expression: &str) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            filter: CompiledFilter::new(expression).context("invalid filter expression")?,
            deferred: DeferredCommits::new(),
        })
    }
}

#[async_trait]
impl MessageConsumer for FilterConsumer {
    /// Reads until a message is kept, holding the acks for what it dropped.
    ///
    /// The caller may ask for the next message before committing this one, so on a
    /// source with cumulative acks an inline drop ack would jump ahead of a retained
    /// message the caller still holds. Those acks run from inside its commit instead,
    /// exactly as [`Self::receive_batch`] does.
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        loop {
            let received = self.inner.receive().await?;
            if self
                .filter
                .matches(&received.message)
                .map_err(ConsumerError::Permanent)?
            {
                let held = self.deferred.take();
                if held.is_empty() {
                    return Ok(received);
                }
                let inner_commit = received.commit;
                let commit: CommitFunc = Box::new(move |disposition| {
                    Box::pin(async move {
                        run_all(held).await?;
                        inner_commit(disposition).await
                    })
                });
                return Ok(Received {
                    message: received.message,
                    commit,
                });
            }

            let ordered = self.inner.commit_requires_order();
            let dropped_commit = received.commit;
            let commit: BatchCommitFunc = Box::new(move |dispositions| {
                dropped_commit(
                    dispositions
                        .into_iter()
                        .next()
                        .unwrap_or(MessageDisposition::Ack),
                )
            });
            self.deferred
                .ack_emptied(ordered, commit, 1)
                .await
                .map_err(ConsumerError::Connection)?;
        }
    }

    /// Reads until a batch has something to keep, acknowledging what it drops.
    ///
    /// An empty batch is the drain signal, and nothing follows it to carry a held
    /// commit — so they are flushed there instead. A drain that fails to flush them
    /// simply re-reads and re-drops those messages next run; nothing reaches the
    /// destination twice.
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let target = max_messages.max(1);
        let mut messages = Vec::with_capacity(target);
        let mut commits: Vec<(usize, BatchCommitFunc)> = Vec::new();

        loop {
            let requested = target - messages.len();
            let batch = self.inner.receive_batch(requested).await?;
            if batch.messages.is_empty() {
                let held = self.deferred.take();
                if messages.is_empty() {
                    run_all(held).await.map_err(|error| {
                        ConsumerError::Connection(
                            error.context(
                                "failed to flush deferred filter acknowledgements on drain",
                            ),
                        )
                    })?;
                    return Ok(batch);
                }

                // Trailing filtered-out source batches must commit after the retained
                // batches already collected in this call, never ahead of them.
                let drain_commit = batch.commit;
                commits.push((
                    0,
                    Box::new(move |_| {
                        Box::pin(async move {
                            run_all(held).await?;
                            drain_commit(Vec::new()).await
                        })
                    }),
                ));
                break;
            }

            let source_count = batch.messages.len();
            let mut kept = Vec::with_capacity(source_count);
            let mut keep_flags = Vec::with_capacity(batch.messages.len());
            for message in batch.messages {
                let keep = self
                    .filter
                    .matches(&message)
                    .map_err(ConsumerError::Permanent)?;
                keep_flags.push(keep);
                if keep {
                    kept.push(message);
                }
            }

            if kept.is_empty() {
                let ordered = self.inner.commit_requires_order();
                self.deferred
                    .ack_emptied(ordered, batch.commit, keep_flags.len())
                    .await
                    .map_err(ConsumerError::Connection)?;
                continue;
            }

            let held = self.deferred.take();
            let expected = kept.len();
            let commit: BatchCommitFunc = Box::new(move |dispositions| {
                Box::pin(async move {
                    if dispositions.len() != expected {
                        bail!(
                            "filter commit received {} dispositions for {expected} retained messages",
                            dispositions.len()
                        );
                    }
                    run_all(held).await?;
                    let mut retained = dispositions.into_iter();
                    let expanded = keep_flags
                        .into_iter()
                        .map(|keep| {
                            if keep {
                                retained.next().unwrap_or(MessageDisposition::Nack)
                            } else {
                                MessageDisposition::Ack
                            }
                        })
                        .collect();
                    (batch.commit)(expanded).await
                })
            });
            messages.extend(kept);
            commits.push((expected, commit));

            // A short source batch is the transport's natural flush boundary. Do
            // not turn filtering into an unbounded wait on a live source.
            if messages.len() >= target || source_count < requested {
                break;
            }
        }

        let commit: BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                let expected: usize = commits.iter().map(|(count, _)| count).sum();
                if dispositions.len() != expected {
                    bail!(
                        "filter commit received {} dispositions for {expected} retained messages",
                        dispositions.len()
                    );
                }

                let mut offset = 0;
                for (count, commit) in commits {
                    let end = offset + count;
                    commit(dispositions[offset..end].to_vec()).await?;
                    offset = end;
                }
                Ok(())
            })
        });
        Ok(ReceivedBatch { messages, commit })
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.inner.set_exit_on_empty(exit_on_empty);
    }

    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }

    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    /// Releases any commit still held for an emptied batch before the source goes away.
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        let inner_hook = self.inner.on_disconnect_hook();
        let held = self.deferred.take_shared();
        if held.is_empty() {
            return inner_hook;
        }
        Some(Box::pin(async move {
            let mut first_error = run_all(held).await.err();
            if let Some(hook) = inner_hook {
                if let Err(error) = hook.await {
                    first_error.get_or_insert(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        }))
    }

    async fn status(&self) -> EndpointStatus {
        self.inner.status().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.inner.close().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Drops messages that do not match instead of publishing them.
///
/// Dropped messages count as sent: the route did what the configuration asked.
pub struct FilterPublisher {
    inner: Box<dyn MessagePublisher>,
    filter: CompiledFilter,
}

impl FilterPublisher {
    pub fn new(inner: Box<dyn MessagePublisher>, expression: &str) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            filter: CompiledFilter::new(expression).context("invalid filter expression")?,
        })
    }
}

#[async_trait]
impl MessagePublisher for FilterPublisher {
    /// Delegated, so wrapping a sink in a filter does not swallow its lifecycle. The
    /// route only ever runs the hooks of the *outermost* publisher, and the structural
    /// endpoints rely on theirs to reach their nested destinations.
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush().await
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        if self
            .filter
            .matches(&message)
            .map_err(PublisherError::NonRetryable)?
        {
            return self.inner.send(message).await;
        }
        Ok(Sent::Ack)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut kept = Vec::with_capacity(messages.len());
        for message in messages {
            if self
                .filter
                .matches(&message)
                .map_err(PublisherError::NonRetryable)?
            {
                kept.push(message);
            }
        }
        if kept.is_empty() {
            return Ok(SentBatch::Ack);
        }
        // `SentBatch::Partial.failed` carries the messages themselves, not
        // indices into the batch, so the dropped ones need no remapping.
        self.inner.send_batch(kept).await
    }

    fn requires_ordered_publish(&self) -> bool {
        self.inner.requires_ordered_publish()
    }

    async fn status(&self) -> EndpointStatus {
        self.inner.status().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn message(payload: &str, metadata: &[(&str, &str)]) -> CanonicalMessage {
        CanonicalMessage {
            message_id: 1,
            payload: Bytes::from(payload.to_string()),
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        }
    }

    #[test]
    fn a_payload_field_is_compared_by_value() {
        let filter = CompiledFilter::new("x > 100").unwrap();
        assert!(filter.matches(&message(r#"{"x": 150}"#, &[])).unwrap());
        assert!(!filter.matches(&message(r#"{"x": 50}"#, &[])).unwrap());
    }

    #[test]
    fn metadata_reads_through_the_meta_prefix() {
        let filter = CompiledFilter::new("meta.http_status_code == '200'").unwrap();
        assert!(filter
            .matches(&message("{}", &[("http_status_code", "200")]))
            .unwrap());
        assert!(!filter
            .matches(&message("{}", &[("http_status_code", "404")]))
            .unwrap());
    }

    /// The whole point of splitting the paths: a metadata-only predicate is the
    /// hot path for `switch`, and must not pay for a JSON parse.
    #[test]
    fn a_metadata_only_predicate_never_parses_the_payload() {
        let filter = CompiledFilter::new("meta.kind == 'order'").unwrap();
        assert!(filter.payload_paths.is_empty());
        let not_json = message("this is not JSON at all", &[("kind", "order")]);
        assert!(filter.matches(&not_json).unwrap());
    }

    #[test]
    fn payload_and_metadata_combine_in_one_expression() {
        let filter = CompiledFilter::new("x > 100 and meta.kind == 'order'").unwrap();
        assert!(filter
            .matches(&message(r#"{"x": 150}"#, &[("kind", "order")]))
            .unwrap());
        assert!(!filter
            .matches(&message(r#"{"x": 150}"#, &[("kind", "refund")]))
            .unwrap());
    }

    /// Metadata is always `String`, so a numeric comparison needs `number()` —
    /// and the bare form must produce the error that says so.
    #[test]
    fn a_numeric_metadata_comparison_needs_an_explicit_cast() {
        let bare = CompiledFilter::new("meta.retries > 3").unwrap();
        let error = bare
            .matches(&message("{}", &[("retries", "5")]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("number(meta.retries)"), "got: {error}");

        let cast = CompiledFilter::new("number(meta.retries) > 3").unwrap();
        assert!(cast.matches(&message("{}", &[("retries", "5")])).unwrap());
    }

    /// The app's original implementation rejected these outright, because it
    /// checked the *root* of the path and found an object.
    #[test]
    fn a_nested_payload_path_resolves_instead_of_dropping_everything() {
        let filter = CompiledFilter::new("order.status == 'open'").unwrap();
        assert!(filter
            .matches(&message(r#"{"order": {"status": "open"}}"#, &[]))
            .unwrap());
        assert!(!filter
            .matches(&message(r#"{"order": {"status": "shipped"}}"#, &[]))
            .unwrap());
    }

    #[test]
    fn an_absent_or_non_scalar_field_does_not_match() {
        let filter = CompiledFilter::new("x > 100").unwrap();
        assert!(!filter.matches(&message(r#"{"y": 1}"#, &[])).unwrap());
        assert!(!filter.matches(&message(r#"{"x": null}"#, &[])).unwrap());
        assert!(!filter.matches(&message(r#"{"x": [1, 2]}"#, &[])).unwrap());
        assert!(!filter.matches(&message("{}", &[])).unwrap());
    }

    #[test]
    fn an_absent_metadata_key_does_not_match() {
        let filter = CompiledFilter::new("meta.kind == 'order'").unwrap();
        assert!(!filter.matches(&message("{}", &[])).unwrap());
    }

    /// Pointing a payload predicate at data it cannot read at all is an error,
    /// not a silent drop of everything.
    #[test]
    fn an_unstructured_payload_is_an_error() {
        let filter = CompiledFilter::new("x > 100").unwrap();
        assert!(filter.matches(&message("not json", &[])).is_err());
        assert!(filter.matches(&message("[1, 2, 3]", &[])).is_err());
    }

    #[test]
    fn c_style_boolean_operators_are_accepted() {
        assert_eq!(normalize_expression("a > 1 && b < 2"), "a > 1  and  b < 2");
        assert_eq!(normalize_expression("a || b"), "a  or  b");
        let filter = CompiledFilter::new("x > 100 && meta.kind == 'order'").unwrap();
        assert!(filter
            .matches(&message(r#"{"x": 150}"#, &[("kind", "order")]))
            .unwrap());
    }

    /// A `&&` inside a string literal is data, not an operator.
    #[test]
    fn boolean_normalization_leaves_string_literals_alone() {
        assert_eq!(normalize_expression("a == 'x && y'"), "a == 'x && y'");
        let filter = CompiledFilter::new("name == 'Ben && Jerry'").unwrap();
        assert!(filter
            .matches(&message(r#"{"name": "Ben && Jerry"}"#, &[]))
            .unwrap());
    }

    #[test]
    fn a_non_boolean_expression_is_rejected_at_evaluation() {
        let filter = CompiledFilter::new("x + 1").unwrap();
        let error = filter
            .matches(&message(r#"{"x": 1}"#, &[]))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("did not evaluate to a boolean"),
            "got: {error}"
        );
    }

    #[test]
    fn an_invalid_expression_fails_to_compile() {
        assert!(CompiledFilter::new("x >").is_err());
        assert!(CompiledFilter::new("((").is_err());
    }

    #[test]
    fn referenced_paths_split_payload_from_metadata() {
        let filter = CompiledFilter::new("order.status == 'x' and meta.kind == 'y'").unwrap();
        assert_eq!(
            filter.payload_paths,
            vec![vec!["order".to_string(), "status".to_string()]]
        );
        assert_eq!(filter.metadata_keys, vec!["kind".to_string()]);
    }

    #[test]
    fn missing_fields_are_null_so_other_boolean_branches_can_match() {
        let filter = CompiledFilter::new("missing == null or x == 1").unwrap();
        assert!(filter.matches(&message(r#"{"x": 1}"#, &[])).unwrap());

        let filter = CompiledFilter::new("meta.missing == null or x == 1").unwrap();
        assert!(filter.matches(&message(r#"{"x": 1}"#, &[])).unwrap());
    }

    #[test]
    fn indexed_payload_paths_are_rejected_at_compile_time() {
        let error = CompiledFilter::new("items[0].qty == 1")
            .err()
            .expect("an indexed path must not compile")
            .to_string();
        assert!(error.contains("indexed path"), "unexpected error: {error}");
    }

    #[test]
    fn simple_equality_predicates_compile_to_the_fast_path() {
        for expression in [
            "x == 1",
            "1 == x",
            "x != null",
            "enabled == true",
            "order.status == 'open'",
            "meta.kind != 'ignored'",
        ] {
            assert!(
                CompiledFilter::new(expression)
                    .unwrap()
                    .fast_predicate
                    .is_some(),
                "{expression}"
            );
        }

        for expression in [
            "x > 1",
            "number(meta.count) >= 2",
            "x == 1 or y == 2",
            "x + 1 == 2",
        ] {
            assert!(
                CompiledFilter::new(expression)
                    .unwrap()
                    .fast_predicate
                    .is_none(),
                "{expression}"
            );
        }
    }

    #[test]
    fn fast_equality_matches_zen_across_values_and_missing_fields() {
        let cases = [
            ("x == 1", r#"{"x":1}"#, &[][..]),
            ("x == 1", r#"{"x":1.0}"#, &[][..]),
            ("x == 1", r#"{"x":"1"}"#, &[][..]),
            ("x != 1", r#"{}"#, &[][..]),
            ("x == null", r#"{}"#, &[][..]),
            ("enabled == true", r#"{"enabled":true}"#, &[][..]),
            (
                "order.status == 'open'",
                r#"{"order":{"status":"open"}}"#,
                &[][..],
            ),
            ("meta.kind == 'order'", "not json", &[("kind", "order")][..]),
            ("meta.kind != 'order'", "not json", &[][..]),
        ];

        for (expression, payload, metadata) in cases {
            let fast = CompiledFilter::new(expression).unwrap();
            assert!(fast.fast_predicate.is_some(), "{expression}");
            let mut zen = CompiledFilter::new(expression).unwrap();
            zen.fast_predicate = None;
            let message = message(payload, metadata);
            assert_eq!(
                fast.matches(&message).unwrap(),
                zen.matches(&message).unwrap(),
                "{expression} with {payload}"
            );
        }
    }

    /// The documented rule: `meta` is the metadata namespace, so a payload field of
    /// the same name must not decide the predicate.
    #[test]
    fn message_metadata_shadows_a_payload_field_named_meta() {
        let filter = CompiledFilter::new("x > 1 and meta.kind == 'real'").unwrap();
        let payload = r#"{"x": 2, "meta": {"kind": "payload"}}"#;
        assert!(filter
            .matches(&message(payload, &[("kind", "real")]))
            .unwrap());
        assert!(!filter
            .matches(&message(payload, &[("kind", "other")]))
            .unwrap());
    }

    /// A `switch` runs every `when` case against one context, so the nulls one
    /// predicate synthesizes for an absent field must not change the next one's answer.
    #[test]
    fn a_synthesized_null_does_not_leak_into_the_next_predicate() {
        let first = CompiledFilter::new("a.b == 1").unwrap();
        let mut second = CompiledFilter::new("a == null or x == 1").unwrap();
        second.fast_predicate = None;
        let message = message(r#"{"x": 2}"#, &[]);

        let alone = second
            .matches_with_context(&message, &mut FilterContext::new())
            .unwrap();

        let mut shared = FilterContext::new();
        assert!(!first.matches_with_context(&message, &mut shared).unwrap());
        assert_eq!(
            second.matches_with_context(&message, &mut shared).unwrap(),
            alone,
            "predicate order changed the answer"
        );
    }

    #[test]
    fn top_level_fast_equality_does_not_build_a_json_document() {
        let filter = CompiledFilter::new("wanted == 'yes'").unwrap();
        let mut context = FilterContext::new();
        let ignored = "x".repeat(10_000);
        let payload = format!(r#"{{"ignored":"{ignored}","wanted":"yes"}}"#);

        assert!(filter
            .matches_with_context(&message(&payload, &[]), &mut context)
            .unwrap());
        assert!(!context.payload_loaded);

        let metadata = CompiledFilter::new("meta.kind == 'order'").unwrap();
        assert!(metadata
            .matches_with_context(
                &message("not json", &[("kind", "order")]),
                &mut FilterContext::new(),
            )
            .unwrap());
    }

    /// A source whose commits must stay in order, handing out one prepared batch per read
    /// and an empty batch once they run out.
    struct OrderedSource {
        batches: std::collections::VecDeque<Vec<CanonicalMessage>>,
        committed: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl OrderedSource {
        fn new(
            batches: Vec<Vec<CanonicalMessage>>,
        ) -> (Self, std::sync::Arc<std::sync::Mutex<Vec<usize>>>) {
            let committed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let source = Self {
                batches: batches.into(),
                committed: committed.clone(),
            };
            (source, committed)
        }
    }

    #[async_trait]
    impl MessageConsumer for OrderedSource {
        async fn receive(&mut self) -> Result<Received, ConsumerError> {
            unimplemented!("batch-only test source")
        }

        async fn receive_batch(&mut self, _max: usize) -> Result<ReceivedBatch, ConsumerError> {
            let messages = self.batches.pop_front().unwrap_or_default();
            let committed = self.committed.clone();
            Ok(ReceivedBatch {
                messages,
                commit: Box::new(move |dispositions| {
                    Box::pin(async move {
                        committed.lock().unwrap().push(dispositions.len());
                        Ok(())
                    })
                }),
            })
        }

        fn commit_requires_order(&self) -> bool {
            true
        }

        async fn close(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn amount(value: i64) -> CanonicalMessage {
        message(&format!(r#"{{"amount":{value}}}"#), &[])
    }

    /// An emptied batch must not ack ahead of the route's ordered sequencer, so its commit
    /// is held and runs from inside the next retained batch's.
    #[tokio::test]
    async fn an_emptied_batch_is_acked_in_front_of_the_batch_that_followed_it() {
        let (source, committed) =
            OrderedSource::new(vec![vec![amount(1), amount(2)], vec![amount(500)]]);
        let mut consumer = FilterConsumer::new(Box::new(source), "amount > 100").unwrap();

        let batch = consumer.receive_batch(16).await.unwrap();
        assert_eq!(
            batch.messages.len(),
            1,
            "only the matching message survives"
        );
        assert!(
            committed.lock().unwrap().is_empty(),
            "the emptied batch must not ack ahead of the route"
        );

        (batch.commit)(vec![MessageDisposition::Ack]).await.unwrap();
        assert_eq!(
            committed.lock().unwrap().as_slice(),
            [2, 1],
            "the emptied batch is acked first, then the retained one"
        );
    }

    #[tokio::test]
    async fn filtered_full_source_batches_are_refilled_to_the_requested_size() {
        let (source, committed) = OrderedSource::new(vec![
            vec![amount(1), amount(200), amount(2), amount(300)],
            vec![amount(400), amount(3), amount(500), amount(4)],
        ]);
        let mut consumer = FilterConsumer::new(Box::new(source), "amount > 100").unwrap();

        let batch = consumer.receive_batch(4).await.unwrap();
        assert_eq!(
            batch
                .messages
                .iter()
                .map(CanonicalMessage::get_payload_str)
                .collect::<Vec<_>>(),
            [
                r#"{"amount":200}"#,
                r#"{"amount":300}"#,
                r#"{"amount":400}"#,
                r#"{"amount":500}"#,
            ]
        );

        (batch.commit)(vec![MessageDisposition::Ack; 4])
            .await
            .unwrap();
        assert_eq!(
            committed.lock().unwrap().as_slice(),
            [4, 4],
            "source batch commits stay in source order"
        );
    }

    #[tokio::test]
    async fn merged_filter_commit_validates_count_before_committing_any_source_batch() {
        let (source, committed) = OrderedSource::new(vec![
            vec![amount(200), amount(1), amount(300), amount(2)],
            vec![amount(400), amount(3), amount(500), amount(4)],
        ]);
        let mut consumer = FilterConsumer::new(Box::new(source), "amount > 100").unwrap();

        let batch = consumer.receive_batch(4).await.unwrap();
        let error = (batch.commit)(vec![MessageDisposition::Ack; 3])
            .await
            .unwrap_err();

        assert!(error.to_string().contains("3 dispositions for 4 retained"));
        assert!(
            committed.lock().unwrap().is_empty(),
            "an invalid merged commit must not partially commit its first source batch"
        );
    }

    /// Nothing follows a drain to carry the held commit, so the drain itself must flush it —
    /// otherwise a route whose tail matches nothing never advances its source position.
    #[tokio::test]
    async fn a_final_emptied_batch_is_acked_when_the_source_drains() {
        let (source, committed) = OrderedSource::new(vec![vec![amount(1), amount(2)]]);
        let mut consumer = FilterConsumer::new(Box::new(source), "amount > 100").unwrap();

        let drained = consumer.receive_batch(16).await.unwrap();
        assert!(drained.messages.is_empty(), "the source is drained");
        assert_eq!(
            committed.lock().unwrap().as_slice(),
            [2],
            "the dropped-only final batch is acknowledged before drain returns"
        );
    }

    /// A route torn down without reading past the emptied batch still releases its commit.
    #[tokio::test]
    async fn a_held_commit_is_released_on_disconnect() {
        let (source, committed) = OrderedSource::new(vec![vec![amount(1)], vec![amount(2)]]);
        let mut consumer = FilterConsumer::new(Box::new(source), "amount > 100").unwrap();

        let batch = consumer.receive_batch(16).await.unwrap();
        assert!(
            batch.messages.is_empty(),
            "both batches are dropped, then drain"
        );
        assert_eq!(committed.lock().unwrap().len(), 2, "flushed by the drain");

        let (source, committed) = OrderedSource::new(vec![vec![amount(1)]]);
        let mut consumer = FilterConsumer::new(Box::new(source), "amount > 100").unwrap();
        // Stop after the emptied batch, before the read that would drain the source.
        let batch = consumer.inner.receive_batch(16).await.unwrap();
        consumer
            .deferred
            .ack_emptied(true, batch.commit, batch.messages.len())
            .await
            .unwrap();
        assert!(committed.lock().unwrap().is_empty());

        consumer.on_disconnect_hook().unwrap().await.unwrap();
        assert_eq!(committed.lock().unwrap().as_slice(), [1]);
    }

    /// The route runs the hooks of the outermost publisher only, so a filter that did not
    /// delegate them would silently disable an endpoint's connect and teardown — including
    /// the structural endpoints that reach their nested destinations that way.
    #[tokio::test]
    async fn publisher_lifecycle_is_delegated_to_the_wrapped_sink() {
        #[derive(Default)]
        struct HookedSink {
            connected: Arc<AtomicBool>,
            disconnected: Arc<AtomicBool>,
            flushed: Arc<AtomicBool>,
        }

        #[async_trait]
        impl MessagePublisher for HookedSink {
            fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
                Some(Box::pin(async move {
                    self.connected.store(true, Ordering::Relaxed);
                    Ok(())
                }))
            }

            fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
                Some(Box::pin(async move {
                    self.disconnected.store(true, Ordering::Relaxed);
                    Ok(())
                }))
            }

            async fn flush(&self) -> anyhow::Result<()> {
                self.flushed.store(true, Ordering::Relaxed);
                Ok(())
            }

            async fn send_batch(
                &self,
                _messages: Vec<CanonicalMessage>,
            ) -> Result<SentBatch, PublisherError> {
                Ok(SentBatch::Ack)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        let sink = HookedSink::default();
        let (connected, disconnected, flushed) = (
            sink.connected.clone(),
            sink.disconnected.clone(),
            sink.flushed.clone(),
        );
        let publisher = FilterPublisher::new(Box::new(sink), "amount > 100").unwrap();

        publisher.on_connect_hook().unwrap().await.unwrap();
        publisher.flush().await.unwrap();
        publisher.on_disconnect_hook().unwrap().await.unwrap();

        assert!(
            connected.load(Ordering::Relaxed),
            "connect hook reached the sink"
        );
        assert!(
            disconnected.load(Ordering::Relaxed),
            "disconnect hook reached the sink"
        );
        assert!(flushed.load(Ordering::Relaxed), "flush reached the sink");
    }
}
