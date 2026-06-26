use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use ::mq_bridge as core;
use anyhow::Context;
use async_trait::async_trait;
use core::models::Endpoint;
use core::traits::Handler;
use core::type_handler::TypeHandler;
use core::{
    CanonicalMessage, Handled, HandlerError, Publisher as CorePublisher, Route as CoreRoute, Sent,
};
use mq_bridge_bindings_common as common;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use serde_json::Value as JsonValue;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

#[napi(js_name = "version")]
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// JSON Schema for the route/config mapping, generated on demand from the
/// compiled Rust models (no checked-in copy, so it cannot drift).
#[cfg(feature = "schema")]
#[napi(js_name = "configSchema")]
pub fn config_schema() -> Result<JsonValue> {
    let schema = schemars::schema_for!(core::models::Config);
    serde_json::to_value(schema).map_err(to_napi_error)
}

#[napi(object)]
pub struct NativeMessage {
    pub payload: Buffer,
    pub metadata: Option<HashMap<String, String>>,
    pub id: Option<String>,
}

impl NativeMessage {
    fn from_canonical(message: &CanonicalMessage) -> Self {
        Self {
            payload: message.payload.to_vec().into(),
            metadata: Some(message.metadata.clone()),
            id: Some(core::canonical_message::format_message_id(message.message_id)),
        }
    }

    fn into_canonical(self) -> Result<CanonicalMessage> {
        build_message(self.payload.to_vec(), self.metadata, self.id.as_deref())
    }
}

#[napi]
pub fn create_message(
    payload: Buffer,
    metadata: Option<HashMap<String, String>>,
    id: Option<String>,
) -> Result<NativeMessage> {
    id.as_deref().map(parse_message_id).transpose()?;
    Ok(NativeMessage {
        payload,
        metadata,
        id,
    })
}

#[napi]
pub fn message_from_json(
    data: JsonValue,
    metadata: Option<HashMap<String, String>>,
    id: Option<String>,
) -> Result<NativeMessage> {
    id.as_deref().map(parse_message_id).transpose()?;
    Ok(NativeMessage {
        payload: serde_json::to_vec(&data).map_err(to_napi_error)?.into(),
        metadata,
        id,
    })
}

#[napi]
pub fn message_json(message: NativeMessage) -> Result<JsonValue> {
    serde_json::from_slice(&message.payload).map_err(to_napi_error)
}

#[napi]
pub fn message_text(message: NativeMessage) -> Result<String> {
    std::str::from_utf8(&message.payload)
        .map(str::to_string)
        .map_err(to_napi_error)
}

struct JsMessageHandler {
    label: String,
    callback: ThreadsafeFunction<
        NativeMessage,
        Promise<Option<NativeMessage>>,
        NativeMessage,
        Status,
        true,
        true,
    >,
}

struct JsJsonHandler {
    label: String,
    callback: ThreadsafeFunction<
        JsonValue,
        Promise<Option<NativeMessage>>,
        JsonValue,
        Status,
        true,
        true,
    >,
}

#[async_trait]
impl Handler for JsMessageHandler {
    async fn handle(&self, msg: CanonicalMessage) -> std::result::Result<Handled, HandlerError> {
        let message = NativeMessage::from_canonical(&msg);
        let result = self
            .callback
            .call_async(Ok(message))
            .await
            .map_err(|err| handler_error(&self.label, err))?
            .await
            .map_err(|err| handler_error(&self.label, err))?;
        message_result_to_handled(result)
    }
}

#[async_trait]
impl Handler for JsJsonHandler {
    async fn handle(&self, msg: CanonicalMessage) -> std::result::Result<Handled, HandlerError> {
        let data = msg.parse::<JsonValue>().map_err(|err| {
            HandlerError::NonRetryable(anyhow::anyhow!("JSON handler parse failed: {err}"))
        })?;
        let result = self
            .callback
            .call_async(Ok(data))
            .await
            .map_err(|err| handler_error(&self.label, err))?
            .await
            .map_err(|err| handler_error(&self.label, err))?;
        message_result_to_handled(result)
    }
}

fn message_result_to_handled(
    result: Option<NativeMessage>,
) -> std::result::Result<Handled, HandlerError> {
    match result {
        Some(message) => message
            .into_canonical()
            .map(Handled::Publish)
            .map_err(|err| HandlerError::NonRetryable(anyhow::anyhow!(err.to_string()))),
        None => Ok(Handled::Ack),
    }
}

fn handler_error(label: &str, err: napi::Error) -> HandlerError {
    HandlerError::NonRetryable(anyhow::anyhow!("Node handler '{label}' failed: {err}"))
}

#[napi]
pub struct Route {
    runtime: Arc<Runtime>,
    route: Arc<Mutex<CoreRoute>>,
    name: String,
    run_state: Arc<Mutex<RouteRunState>>,
}

#[derive(Default)]
struct RouteRunState {
    running: bool,
    stop_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

#[napi]
impl Route {
    /// Build a route from a YAML or JSON config file. Accepts a `routes:`
    /// document, a bare `{name: route}` map, or a single route body. Omit
    /// `name` (or pass `""`) when the file is a single bare route body.
    #[napi(factory)]
    pub fn from_file(path: String, name: Option<String>) -> Result<Self> {
        let name = common::normalize_name(name.as_deref()).map(str::to_string);
        let route =
            common::load_named_route(Path::new(&path), name.as_deref()).map_err(to_napi_error)?;
        Self::build(route, name.unwrap_or_else(common::default_route_name))
    }

    /// Deprecated alias for `from_file`.
    #[napi(factory)]
    pub fn from_yaml(path: String, name: Option<String>) -> Result<Self> {
        Self::from_file(path, name)
    }

    /// Build a route from an in-memory YAML or JSON string. Accepts the same
    /// shapes as `from_file`. Omit `name` (or pass `""`) when the string is a
    /// single bare route body.
    #[napi(factory)]
    pub fn from_str(text: String, name: Option<String>) -> Result<Self> {
        let name = common::normalize_name(name.as_deref()).map(str::to_string);
        let value = serde_yaml_ng::from_str(&text)
            .context("failed to parse YAML config")
            .map_err(to_napi_error)?;
        let route =
            common::named_route_from_value(value, name.as_deref()).map_err(to_napi_error)?;
        Self::build(route, name.unwrap_or_else(common::default_route_name))
    }

    /// Deprecated alias for `from_str`.
    #[napi(factory)]
    pub fn from_yaml_str(text: String, name: Option<String>) -> Result<Self> {
        Self::from_str(text, name)
    }

    /// Build a route from an in-memory mapping (e.g. a JS object). Accepts a
    /// `routes:` document, a bare `{name: route}` map, or a single route body.
    /// Omit `name` (or pass `""`) to treat the mapping as a single bare route
    /// body, in which case a name is generated automatically.
    #[napi(factory)]
    pub fn from_config(config: JsonValue, name: Option<String>) -> Result<Self> {
        let name = common::normalize_name(name.as_deref()).map(str::to_string);
        let value = serde_yaml_ng::to_value(config)
            .context("failed to convert config mapping")
            .map_err(to_napi_error)?;
        let route =
            common::named_route_from_value(value, name.as_deref()).map_err(to_napi_error)?;
        Self::build(route, name.unwrap_or_else(common::default_route_name))
    }

    #[napi]
    pub fn with_handler(
        &self,
        callback: ThreadsafeFunction<
            NativeMessage,
            Promise<Option<NativeMessage>>,
            NativeMessage,
            Status,
            true,
            true,
        >,
    ) -> Result<()> {
        self.ensure_not_running()?;
        let mut route = self.lock_route()?;
        let updated = route.clone().with_handler(JsMessageHandler {
            label: self.name.clone(),
            callback,
        });
        *route = updated;
        Ok(())
    }

    #[napi]
    pub fn add_handler(
        &self,
        kind: String,
        callback: ThreadsafeFunction<
            JsonValue,
            Promise<Option<NativeMessage>>,
            JsonValue,
            Status,
            true,
            true,
        >,
    ) -> Result<()> {
        self.ensure_not_running()?;
        let mut route = self.lock_route()?;
        let prev_handler = route.output.handler.take();
        let js_handler: Arc<dyn Handler> = Arc::new(JsJsonHandler {
            label: format!("{}:{kind}", self.name),
            callback,
        });

        let new_handler = if let Some(existing) = prev_handler {
            if let Some(extended) = existing.register_handler(&kind, Arc::clone(&js_handler)) {
                extended
            } else {
                Arc::new(
                    TypeHandler::new()
                        .with_fallback(existing)
                        .add_handler(&kind, js_handler),
                )
            }
        } else {
            Arc::new(TypeHandler::new().add_handler(&kind, js_handler))
        };

        route.output.handler = Some(new_handler);
        Ok(())
    }

    #[napi]
    pub fn start(&self) -> Result<()> {
        let route = self.lock_route()?.clone();
        let stop_rx = self.begin_run()?;
        let name = self.name.clone();
        let runtime = Arc::clone(&self.runtime);
        let run_state = Arc::clone(&self.run_state);

        let deploy_name = name.clone();
        let deploy_runtime = Arc::clone(&runtime);
        if let Err(err) = deploy_runtime.block_on(async move { route.deploy(&deploy_name).await }) {
            finish_run(&run_state, &name);
            return Err(to_napi_error(err));
        }

        let wait_name = name.clone();
        let wait_run_state = Arc::clone(&run_state);
        let handle = match thread::Builder::new()
            .name(format!("mqb-node-route-{name}"))
            .spawn(move || {
                let stop_name = wait_name.clone();
                runtime.block_on(async move {
                    let _ = stop_rx.await;
                    core::Route::stop(&stop_name).await;
                });
                finish_run(&wait_run_state, &wait_name);
            }) {
            Ok(handle) => handle,
            Err(err) => {
                // Spawn failed after the route was deployed; stop it and clear
                // run state so the name isn't orphaned and can be started again.
                deploy_runtime.block_on(async { core::Route::stop(&name).await });
                finish_run(&run_state, &name);
                return Err(to_napi_error(err));
            }
        };

        self.lock_run_state()?.join_handle = Some(handle);
        Ok(())
    }

    #[napi]
    pub fn stop(&self) -> Result<()> {
        let stop_tx = self.lock_run_state()?.stop_tx.take();
        if let Some(stop_tx) = stop_tx {
            let _ = stop_tx.send(());
        }
        Ok(())
    }

    #[napi]
    pub fn join(&self) -> Result<()> {
        let handle = self.lock_run_state()?.join_handle.take();
        if let Some(handle) = handle {
            handle
                .join()
                .map_err(|_| Error::from_reason("Route background thread panicked"))?;
        }
        Ok(())
    }
}

impl Route {
    fn build(route: CoreRoute, name: String) -> Result<Self> {
        Ok(Self {
            runtime: Arc::new(common::build_runtime().map_err(to_napi_error)?),
            route: Arc::new(Mutex::new(route)),
            name,
            run_state: Arc::new(Mutex::new(RouteRunState::default())),
        })
    }

    fn begin_run(&self) -> Result<oneshot::Receiver<()>> {
        let mut state = self.lock_run_state()?;
        if state.running {
            return Err(Error::from_reason("Route is already running"));
        }
        let mut active_route_names = lock_active_route_names()?;
        if active_route_names.contains(&self.name) || core::Route::get(&self.name).is_some() {
            return Err(Error::from_reason(format!(
                "A route named '{}' is already running",
                self.name
            )));
        }
        active_route_names.insert(self.name.clone());
        let (stop_tx, stop_rx) = oneshot::channel();
        state.running = true;
        state.stop_tx = Some(stop_tx);
        Ok(stop_rx)
    }

    fn lock_route(&self) -> Result<std::sync::MutexGuard<'_, CoreRoute>> {
        self.route
            .lock()
            .map_err(|_| Error::from_reason("Route lock poisoned"))
    }

    fn lock_run_state(&self) -> Result<std::sync::MutexGuard<'_, RouteRunState>> {
        self.run_state
            .lock()
            .map_err(|_| Error::from_reason("Route state lock poisoned"))
    }

    fn ensure_not_running(&self) -> Result<()> {
        if self.lock_run_state()?.running {
            Err(Error::from_reason(
                "Route handlers cannot be modified while the route is running",
            ))
        } else {
            Ok(())
        }
    }
}

#[napi]
pub struct Publisher {
    runtime: Arc<Runtime>,
    publisher: CorePublisher,
}

#[napi]
impl Publisher {
    /// Build a publisher endpoint from a YAML or JSON config file. Omit `name`
    /// (or pass `""`) when the file is a single bare endpoint body.
    #[napi(factory)]
    pub fn from_file(path: String, name: Option<String>) -> Result<Self> {
        let name = common::normalize_name(name.as_deref());
        Self::build(
            common::load_named_publisher(Path::new(&path), name).map_err(to_napi_error)?,
        )
    }

    /// Deprecated alias for `from_file`.
    #[napi(factory)]
    pub fn from_yaml(path: String, name: Option<String>) -> Result<Self> {
        Self::from_file(path, name)
    }

    /// Build a publisher endpoint from an in-memory YAML or JSON string. Omit
    /// `name` (or pass `""`) when the string is a single bare endpoint body.
    #[napi(factory)]
    pub fn from_str(text: String, name: Option<String>) -> Result<Self> {
        let name = common::normalize_name(name.as_deref());
        let value = serde_yaml_ng::from_str(&text)
            .context("failed to parse YAML config")
            .map_err(to_napi_error)?;
        Self::build(common::named_publisher_from_value(value, name).map_err(to_napi_error)?)
    }

    /// Deprecated alias for `from_str`.
    #[napi(factory)]
    pub fn from_yaml_str(text: String, name: Option<String>) -> Result<Self> {
        Self::from_str(text, name)
    }

    /// Build a publisher endpoint from an in-memory mapping (e.g. a JS object).
    /// Omit `name` (or pass `""`) to treat the mapping as a single bare endpoint
    /// body.
    #[napi(factory)]
    pub fn from_config(config: JsonValue, name: Option<String>) -> Result<Self> {
        let name = common::normalize_name(name.as_deref());
        let value = serde_yaml_ng::to_value(config)
            .context("failed to convert config mapping")
            .map_err(to_napi_error)?;
        Self::build(common::named_publisher_from_value(value, name).map_err(to_napi_error)?)
    }

    #[napi]
    pub async fn send(&self, message: NativeMessage) -> Result<()> {
        self.send_on_runtime(message.into_canonical()?).await
    }

    #[napi]
    pub async fn request(&self, message: NativeMessage) -> Result<NativeMessage> {
        self.request_on_runtime(message.into_canonical()?).await
    }

    #[napi]
    pub async fn send_json(
        &self,
        data: JsonValue,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> Result<()> {
        self.send_on_runtime(json_input_to_canonical(data, metadata, id.as_deref())?)
            .await
    }

    #[napi]
    pub async fn request_json(
        &self,
        data: JsonValue,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> Result<NativeMessage> {
        self.request_on_runtime(json_input_to_canonical(data, metadata, id.as_deref())?)
            .await
    }
}

impl Publisher {
    fn build(endpoint: Endpoint) -> Result<Self> {
        let runtime = Arc::new(common::build_runtime().map_err(to_napi_error)?);
        let publisher = runtime
            .block_on(CorePublisher::new(endpoint))
            .map_err(to_napi_error)?;
        Ok(Self { runtime, publisher })
    }

    // Run the work on this publisher's own runtime (where its transport is
    // bound) and await the result without blocking the JS thread. Awaiting the
    // JoinHandle is runtime-agnostic, so napi's executor only parks on it.
    async fn send_on_runtime(&self, message: CanonicalMessage) -> Result<()> {
        let publisher = self.publisher.clone();
        let handle = self
            .runtime
            .spawn(async move { publisher.send(message).await });
        match handle
            .await
            .map_err(to_napi_error)?
            .map_err(to_napi_error)?
        {
            Sent::Ack | Sent::Response(_) => Ok(()),
        }
    }

    async fn request_on_runtime(&self, message: CanonicalMessage) -> Result<NativeMessage> {
        let publisher = self.publisher.clone();
        let handle = self
            .runtime
            .spawn(async move { publisher.request(message).await });
        let response = handle
            .await
            .map_err(to_napi_error)?
            .map_err(to_napi_error)?;
        Ok(NativeMessage::from_canonical(&response))
    }
}

fn build_message(
    payload: Vec<u8>,
    metadata: Option<HashMap<String, String>>,
    id: Option<&str>,
) -> Result<CanonicalMessage> {
    let message_id = id.map(parse_message_id).transpose()?;
    let mut message = CanonicalMessage::new(payload, message_id);
    if let Some(metadata) = metadata {
        message.metadata.extend(metadata);
    }
    Ok(message)
}

fn json_input_to_canonical(
    data: JsonValue,
    metadata: Option<HashMap<String, String>>,
    id: Option<&str>,
) -> Result<CanonicalMessage> {
    build_message(
        serde_json::to_vec(&data).map_err(to_napi_error)?,
        metadata,
        id,
    )
}

fn parse_message_id(id: &str) -> Result<u128> {
    core::canonical_message::message_id_from_str(id).map_err(Error::from_reason)
}

fn active_route_names() -> &'static Mutex<HashSet<String>> {
    static ACTIVE_ROUTE_NAMES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    ACTIVE_ROUTE_NAMES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn lock_active_route_names() -> Result<std::sync::MutexGuard<'static, HashSet<String>>> {
    active_route_names()
        .lock()
        .map_err(|_| Error::from_reason("Active route registry lock poisoned"))
}

fn finish_run(run_state: &Arc<Mutex<RouteRunState>>, name: &str) {
    if let Ok(mut active) = active_route_names().lock() {
        active.remove(name);
    }
    if let Ok(mut state) = run_state.lock() {
        state.running = false;
        state.stop_tx = None;
    }
}

fn to_napi_error(err: impl std::fmt::Display) -> Error {
    Error::from_reason(err.to_string())
}
