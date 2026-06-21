use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use ::mq_bridge as core;
use anyhow::Context;
use async_trait::async_trait;
use core::models::{Endpoint, PublisherConfig};
use core::traits::Handler;
use core::type_handler::TypeHandler;
use core::{
    CanonicalMessage, Handled, HandlerError, Publisher as CorePublisher, Route as CoreRoute, Sent,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use serde::de::IntoDeserializer;
use serde_json::Value as JsonValue;
use tokio::runtime::{Builder, Runtime};
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
            id: Some(format_message_id(message.message_id)),
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
    #[napi(factory)]
    pub fn from_yaml(path: String, name: String) -> Result<Self> {
        Self::build(
            load_named_route(Path::new(&path), &name).map_err(to_napi_error)?,
            name,
        )
    }

    #[napi(factory)]
    pub fn from_yaml_str(text: String, name: String) -> Result<Self> {
        let value = unwrap_config_root(
            serde_yaml_ng::from_str(&text)
                .context("failed to parse YAML config")
                .map_err(to_napi_error)?,
        );
        Self::build(
            load_route_from_value(value, &name).map_err(to_napi_error)?,
            name,
        )
    }

    #[napi(factory)]
    pub fn from_config(config: JsonValue, name: String) -> Result<Self> {
        let value = serde_yaml_ng::to_value(config)
            .context("failed to convert config mapping")
            .map_err(to_napi_error)?;
        Self::build(
            load_route_from_value(unwrap_config_root(value), &name).map_err(to_napi_error)?,
            name,
        )
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
        let handle = thread::Builder::new()
            .name(format!("mqb-node-route-{name}"))
            .spawn(move || {
                let stop_name = wait_name.clone();
                runtime.block_on(async move {
                    let _ = stop_rx.await;
                    core::Route::stop(&stop_name).await;
                });
                finish_run(&wait_run_state, &wait_name);
            })
            .map_err(to_napi_error)?;

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
            runtime: Arc::new(build_runtime().map_err(to_napi_error)?),
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
    #[napi(factory)]
    pub fn from_yaml(path: String, name: String) -> Result<Self> {
        Self::build(load_named_publisher(Path::new(&path), &name).map_err(to_napi_error)?)
    }

    #[napi(factory)]
    pub fn from_yaml_str(text: String, name: String) -> Result<Self> {
        let value = unwrap_config_root(
            serde_yaml_ng::from_str(&text)
                .context("failed to parse YAML config")
                .map_err(to_napi_error)?,
        );
        Self::build(named_publisher_from_value(value, &name).map_err(to_napi_error)?)
    }

    #[napi(factory)]
    pub fn from_config(config: JsonValue, name: String) -> Result<Self> {
        let value = serde_yaml_ng::to_value(config)
            .context("failed to convert config mapping")
            .map_err(to_napi_error)?;
        Self::build(
            named_publisher_from_value(unwrap_config_root(value), &name).map_err(to_napi_error)?,
        )
    }

    #[napi]
    pub fn send(&self, message: NativeMessage) -> Result<()> {
        let message = message.into_canonical()?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        run_sync_task(&runtime, async move {
            match publisher.send(message).await? {
                Sent::Ack | Sent::Response(_) => Ok(()),
            }
        })
    }

    #[napi]
    pub fn request(&self, message: NativeMessage) -> Result<NativeMessage> {
        let message = message.into_canonical()?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        run_sync_task(&runtime, async move {
            let response = publisher.request(message).await?;
            Ok(NativeMessage::from_canonical(&response))
        })
    }

    #[napi]
    pub fn send_json(
        &self,
        data: JsonValue,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> Result<()> {
        let message = json_input_to_canonical(data, metadata, id.as_deref())?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        run_sync_task(&runtime, async move {
            match publisher.send(message).await? {
                Sent::Ack | Sent::Response(_) => Ok(()),
            }
        })
    }

    #[napi]
    pub fn request_json(
        &self,
        data: JsonValue,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> Result<NativeMessage> {
        let message = json_input_to_canonical(data, metadata, id.as_deref())?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        run_sync_task(&runtime, async move {
            let response = publisher.request(message).await?;
            Ok(NativeMessage::from_canonical(&response))
        })
    }
}

impl Publisher {
    fn build(endpoint: Endpoint) -> Result<Self> {
        let runtime = Arc::new(build_runtime().map_err(to_napi_error)?);
        let publisher = runtime
            .block_on(CorePublisher::new(endpoint))
            .map_err(to_napi_error)?;
        Ok(Self { runtime, publisher })
    }
}

fn build_runtime() -> anyhow::Result<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build Tokio runtime")
}

fn load_config_value(path: &Path) -> anyhow::Result<serde_yaml_ng::Value> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config '{}'", path.display()))?;
    serde_yaml_ng::from_str(&raw).context("failed to parse YAML config")
}

fn load_named_route(path: &Path, name: &str) -> anyhow::Result<CoreRoute> {
    load_route_from_value(load_config_value(path)?, name)
}

fn load_route_from_value(value: serde_yaml_ng::Value, name: &str) -> anyhow::Result<CoreRoute> {
    let config = unwrap_config_root(value);
    let document = load_document_from_value(config)?;
    let route = document
        .routes
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("route '{name}' not found in config"))?;
    route
        .try_into()
        .with_context(|| format!("failed to build route '{name}'"))
}

fn load_named_publisher(path: &Path, name: &str) -> anyhow::Result<Endpoint> {
    named_publisher_from_value(load_config_value(path)?, name)
}

fn named_publisher_from_value(value: serde_yaml_ng::Value, name: &str) -> anyhow::Result<Endpoint> {
    if let Ok(document) = load_document_from_value(value.clone()) {
        if let Some(endpoint) = document.publishers.get(name).cloned() {
            return Ok(endpoint);
        }
    }

    serde_yaml_ng::from_value(value).with_context(|| {
        format!(
            "No publisher named '{name}' found, and the config could not be parsed as a single publisher endpoint"
        )
    })
}

fn load_document_from_value(value: serde_yaml_ng::Value) -> anyhow::Result<ConfigDocument> {
    let section_key = |name: &str| serde_yaml_ng::Value::String(name.to_string());
    let routes_key = section_key("routes");
    let publishers_key = section_key("publishers");

    if let Some(map) = value.as_mapping() {
        if map.contains_key(&routes_key) || map.contains_key(&publishers_key) {
            let routes = map
                .get(&routes_key)
                .map_or_else(
                    || Ok(HashMap::new()),
                    |section| serde_yaml_ng::from_value(section.clone()),
                )
                .context("failed to parse 'routes' section")?;
            let publishers = map.get(&publishers_key).map_or_else(
                || Ok(PublisherConfig::new()),
                |section| parse_publishers_section(section.clone()),
            )?;
            return Ok(ConfigDocument { routes, publishers });
        }
    }

    let routes = serde_yaml_ng::from_value(value).context("failed to parse YAML as a route map")?;
    Ok(ConfigDocument {
        routes,
        publishers: PublisherConfig::new(),
    })
}

fn parse_publishers_section(value: serde_yaml_ng::Value) -> anyhow::Result<PublisherConfig> {
    serde_yaml_ng::from_value(value.clone()).or_else(|err| {
        let map = value
            .as_mapping()
            .ok_or_else(|| anyhow::anyhow!("failed to parse publishers section: {err}"))?;
        let mut publishers = PublisherConfig::new();
        for (key, endpoint_value) in map {
            let name = key
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("publisher names must be strings"))?;
            let endpoint: Endpoint = serde_yaml_ng::from_value(endpoint_value.clone())
                .with_context(|| format!("failed to parse publisher '{name}'"))?;
            publishers.insert(name.to_string(), endpoint);
        }
        Ok(publishers)
    })
}

struct ConfigDocument {
    routes: HashMap<String, CoreRoute>,
    publishers: PublisherConfig,
}

fn unwrap_config_root(value: serde_yaml_ng::Value) -> serde_yaml_ng::Value {
    if let Some(map) = value.as_mapping() {
        let config_key = serde_yaml_ng::Value::String("config".to_string());
        if let Some(config) = map.get(&config_key) {
            return config.clone();
        }
    }
    value
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
    core::canonical_message::deserialize_u128(JsonValue::String(id.to_string()).into_deserializer())
        .map_err(|err| Error::from_reason(format!("invalid message id '{id}': {err}")))
}

fn format_message_id(message_id: u128) -> String {
    fast_uuid_v7::format_uuid(message_id).to_string()
}

fn run_sync_task<F, T>(runtime: &Runtime, future: F) -> Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    runtime.block_on(future).map_err(to_napi_error)
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
