use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ::mq_bridge as core;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use core::endpoints::memory::MemoryConsumer;
use core::models::{Endpoint, PublisherConfig};
use core::traits::{Handler, MessageConsumer, MessageDisposition};
use core::type_handler::TypeHandler;
use core::{
    CanonicalMessage, Handled, HandlerError, Publisher as CorePublisher, Route as CoreRoute, Sent,
};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyList, PyModule, PyTuple, PyType};
use serde::de::{
    DeserializeSeed, Error as DeError, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};
use serde::ser::{Error as SerError, SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{oneshot, Semaphore};
use tracing::error;

const MAX_JSON_DEPTH: usize = 64;

static PYTHON_HANDLER_CONCURRENCY: OnceLock<Arc<Semaphore>> = OnceLock::new();
static ACTIVE_ROUTE_NAMES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

create_exception!(mq_bridge, RetryableError, PyException);
create_exception!(mq_bridge, NonRetryableError, PyException);

#[derive(Debug, Default, Deserialize)]
struct ConfigDocument {
    #[serde(default)]
    routes: HashMap<String, CoreRoute>,
    #[serde(default)]
    publishers: PublisherConfig,
}

#[derive(Debug, Deserialize)]
struct NamedPublisher {
    name: String,
    endpoint: Endpoint,
}

#[derive(Default)]
struct RouteRunState {
    running: bool,
    stop_tx: Option<oneshot::Sender<()>>,
}

#[derive(Clone, Copy, Debug)]
enum PythonHandlerMode {
    Message,
    Json,
}

#[derive(Clone)]
struct PythonHandler {
    label: String,
    mode: PythonHandlerMode,
    callable: Arc<Py<PyAny>>,
}

impl PythonHandler {
    fn message(label: impl Into<String>, callable: Py<PyAny>) -> Self {
        Self {
            label: label.into(),
            mode: PythonHandlerMode::Message,
            callable: Arc::new(callable),
        }
    }

    fn json(label: impl Into<String>, callable: Py<PyAny>) -> Self {
        Self {
            label: label.into(),
            mode: PythonHandlerMode::Json,
            callable: Arc::new(callable),
        }
    }
}

#[async_trait]
impl Handler for PythonHandler {
    async fn handle(&self, msg: CanonicalMessage) -> Result<Handled, HandlerError> {
        let label = self.label.clone();
        let mode = self.mode;
        let callable = Arc::clone(&self.callable);
        let permit = python_handler_semaphore()
            .acquire_owned()
            .await
            .map_err(|err| {
                HandlerError::NonRetryable(anyhow!("Python handler limit failed: {err}"))
            })?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            invoke_python_handler(callable, mode, label, msg)
        })
        .await
        .map_err(|err| HandlerError::NonRetryable(anyhow!("Python handler task failed: {err}")))?
    }

    async fn handle_many(&self, msgs: Vec<CanonicalMessage>) -> Vec<Result<Handled, HandlerError>> {
        let len = msgs.len();
        let label = self.label.clone();
        let mode = self.mode;
        let callable = Arc::clone(&self.callable);
        let permit = match python_handler_semaphore().acquire_owned().await {
            Ok(permit) => permit,
            Err(err) => {
                let error =
                    HandlerError::NonRetryable(anyhow!("Python handler limit failed: {err}"));
                return std::iter::repeat_with(|| {
                    Err(HandlerError::NonRetryable(anyhow!(error.to_string())))
                })
                .take(len)
                .collect();
            }
        };

        match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            invoke_python_handler_many(callable, mode, label, msgs)
        })
        .await
        {
            Ok(results) => results,
            Err(err) => std::iter::repeat_with(|| {
                Err(HandlerError::NonRetryable(anyhow!(
                    "Python handler task failed: {err}"
                )))
            })
            .take(len)
            .collect(),
        }
    }
}

#[pyclass(module = "mq_bridge")]
#[derive(Debug)]
struct Message {
    payload: Vec<u8>,
    metadata: HashMap<String, String>,
    id: Option<String>,
}

impl Message {
    fn from_canonical(message: &CanonicalMessage) -> Self {
        Self {
            payload: message.payload.to_vec(),
            metadata: message.metadata.clone(),
            id: Some(format_message_id(message.message_id)),
        }
    }

    fn to_canonical(&self) -> PyResult<CanonicalMessage> {
        build_message(
            self.payload.clone(),
            Some(self.metadata.clone()),
            self.id.as_deref(),
        )
    }
}

#[pymethods]
impl Message {
    #[new]
    #[pyo3(signature = (payload, metadata=None, id=None))]
    fn new(
        payload: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> PyResult<Self> {
        validate_message_id(id.as_deref())?;
        Ok(Self {
            payload,
            metadata: metadata.unwrap_or_default(),
            id,
        })
    }

    #[classmethod]
    #[pyo3(signature = (data, metadata=None, id=None))]
    fn from_json(
        _cls: &Bound<'_, PyType>,
        data: &Bound<'_, PyAny>,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> PyResult<Self> {
        validate_message_id(id.as_deref())?;
        let payload = python_to_json_bytes(data)?;
        Ok(Self {
            payload,
            metadata: metadata.unwrap_or_default(),
            id,
        })
    }

    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.metadata.clone()
    }

    #[getter]
    fn id(&self) -> Option<String> {
        self.id.clone()
    }

    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_bytes_to_python(py, &self.payload)
    }

    fn text(&self) -> PyResult<String> {
        std::str::from_utf8(&self.payload)
            .map(str::to_owned)
            .map_err(|err| PyValueError::new_err(format!("payload is not valid UTF-8: {err}")))
    }

    fn with_json(&self, data: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            payload: python_to_json_bytes(data)?,
            metadata: self.metadata.clone(),
            id: self.id.clone(),
        })
    }

    fn with_payload(&self, payload: Vec<u8>) -> Self {
        Self {
            payload,
            metadata: self.metadata.clone(),
            id: self.id.clone(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(id={:?}, metadata={:?}, payload_len={})",
            self.id,
            self.metadata,
            self.payload.len()
        )
    }
}

#[pyclass(module = "mq_bridge")]
struct Route {
    runtime: Arc<Runtime>,
    route: Arc<Mutex<CoreRoute>>,
    name: String,
    run_state: Arc<Mutex<RouteRunState>>,
}

#[pymethods]
impl Route {
    #[staticmethod]
    fn from_yaml(py: Python<'_>, path: &str, name: &str) -> PyResult<Self> {
        let path = path.to_string();
        let name = name.to_string();
        py.detach(move || -> anyhow::Result<Self> {
            let route = load_named_route(Path::new(&path), &name)?;

            Ok(Self {
                runtime: Arc::new(build_runtime()?),
                route: Arc::new(Mutex::new(route)),
                name,
                run_state: Arc::new(Mutex::new(RouteRunState::default())),
            })
        })
        .map_err(to_py_runtime_error)
    }

    fn with_handler<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        handler: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if !handler.bind(py).is_callable() {
            return Err(PyTypeError::new_err("handler must be callable"));
        }
        slf.ensure_not_running()?;

        let mut route = slf.lock_route()?;
        let updated = route
            .clone()
            .with_handler(PythonHandler::message(slf.name.clone(), handler));
        *route = updated;
        drop(route);
        Ok(slf)
    }

    fn add_handler<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        kind: &str,
        handler: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if !handler.bind(py).is_callable() {
            return Err(PyTypeError::new_err("handler must be callable"));
        }
        slf.ensure_not_running()?;

        let mut route = slf.lock_route()?;
        let prev_handler = route.output.handler.take();
        let python_handler = PythonHandler::json(format!("{}:{kind}", slf.name), handler);

        let new_handler = if let Some(existing) = prev_handler {
            if let Some(extended) =
                existing.register_handler(kind, Arc::new(python_handler.clone()))
            {
                extended
            } else {
                Arc::new(
                    TypeHandler::new()
                        .with_fallback(existing)
                        .add_handler(kind, python_handler),
                )
            }
        } else {
            Arc::new(TypeHandler::new().add_handler(kind, python_handler))
        };

        route.output.handler = Some(new_handler);
        drop(route);
        Ok(slf)
    }

    fn add_message_handler<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        kind: &str,
        handler: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if !handler.bind(py).is_callable() {
            return Err(PyTypeError::new_err("handler must be callable"));
        }
        slf.ensure_not_running()?;

        let mut route = slf.lock_route()?;
        let prev_handler = route.output.handler.take();
        let python_handler = PythonHandler::message(format!("{}:{kind}", slf.name), handler);

        let new_handler = if let Some(existing) = prev_handler {
            if let Some(extended) =
                existing.register_handler(kind, Arc::new(python_handler.clone()))
            {
                extended
            } else {
                Arc::new(
                    TypeHandler::new()
                        .with_fallback(existing)
                        .add_handler(kind, python_handler),
                )
            }
        } else {
            Arc::new(TypeHandler::new().add_handler(kind, python_handler))
        };

        route.output.handler = Some(new_handler);
        drop(route);
        Ok(slf)
    }

    fn run(&self, py: Python<'_>) -> PyResult<()> {
        let route = self.lock_route()?.clone();
        let stop_rx = {
            let mut state = self.lock_run_state()?;
            if state.running {
                return Err(PyRuntimeError::new_err("Route is already running"));
            }
            let mut active_route_names = lock_active_route_names()?;
            if active_route_names.contains(&self.name) || core::Route::get(&self.name).is_some() {
                return Err(PyRuntimeError::new_err(format!(
                    "A route named '{}' is already running",
                    self.name
                )));
            }
            active_route_names.insert(self.name.clone());
            let (stop_tx, stop_rx) = oneshot::channel();
            state.running = true;
            state.stop_tx = Some(stop_tx);
            stop_rx
        };
        let name = self.name.clone();
        let deployed_name = name.clone();
        let runtime = Arc::clone(&self.runtime);
        let run_state = Arc::clone(&self.run_state);

        py.detach(move || {
            let result = runtime.block_on(async move {
                route.deploy(&deployed_name).await?;
                let _ = stop_rx.await;
                core::Route::stop(&deployed_name).await;
                Ok::<(), anyhow::Error>(())
            });

            if let Ok(mut state) = run_state.lock() {
                state.running = false;
                state.stop_tx = None;
            }
            if let Ok(mut active_route_names) = active_route_names().lock() {
                active_route_names.remove(&name);
            }

            result
        })
        .map_err(to_py_runtime_error)
    }

    fn stop(&self) -> PyResult<()> {
        let stop_tx = self.lock_run_state()?.stop_tx.take();
        if let Some(stop_tx) = stop_tx {
            let _ = stop_tx.send(());
        }
        Ok(())
    }
}

impl Route {
    fn lock_route(&self) -> PyResult<std::sync::MutexGuard<'_, CoreRoute>> {
        self.route
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Route lock poisoned"))
    }

    fn lock_run_state(&self) -> PyResult<std::sync::MutexGuard<'_, RouteRunState>> {
        self.run_state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Route state lock poisoned"))
    }

    fn ensure_not_running(&self) -> PyResult<()> {
        if self.lock_run_state()?.running {
            Err(PyRuntimeError::new_err(
                "Route handlers cannot be modified while the route is running",
            ))
        } else {
            Ok(())
        }
    }
}

#[pyclass(module = "mq_bridge")]
struct Publisher {
    runtime: Arc<Runtime>,
    publisher: CorePublisher,
}

#[pymethods]
impl Publisher {
    #[staticmethod]
    fn from_yaml(py: Python<'_>, path: &str, name: &str) -> PyResult<Self> {
        let path = path.to_string();
        let name = name.to_string();
        py.detach(move || -> anyhow::Result<Self> {
            let endpoint = load_named_publisher(Path::new(&path), &name)?;

            let runtime = Arc::new(build_runtime()?);
            let publisher = runtime.block_on(CorePublisher::new(endpoint))?;

            Ok(Self { runtime, publisher })
        })
        .map_err(to_py_runtime_error)
    }

    #[pyo3(signature = (message, metadata=None))]
    fn send(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let message = message_input_to_canonical(message, metadata)?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        py.detach(move || {
            run_sync_task(&runtime, async move {
                match publisher.send(message).await? {
                    Sent::Ack | Sent::Response(_) => Ok(()),
                }
            })
        })
        .map_err(to_py_runtime_error)
    }

    #[pyo3(signature = (message, metadata=None))]
    fn request(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<Message> {
        let message = message_input_to_canonical(message, metadata)?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        py.detach(move || {
            run_sync_task(&runtime, async move {
                let response = publisher.request(message).await?;
                Ok(Message::from_canonical(&response))
            })
        })
        .map_err(to_py_runtime_error)
    }

    #[pyo3(signature = (data, metadata=None, id=None))]
    fn send_json(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> PyResult<()> {
        let message = json_input_to_canonical(data, metadata, id.as_deref())?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        py.detach(move || {
            run_sync_task(&runtime, async move {
                match publisher.send(message).await? {
                    Sent::Ack | Sent::Response(_) => Ok(()),
                }
            })
        })
        .map_err(to_py_runtime_error)
    }

    #[pyo3(signature = (data, metadata=None, id=None))]
    fn request_json(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        metadata: Option<HashMap<String, String>>,
        id: Option<String>,
    ) -> PyResult<Message> {
        let message = json_input_to_canonical(data, metadata, id.as_deref())?;
        let publisher = self.publisher.clone();
        let runtime = Arc::clone(&self.runtime);
        py.detach(move || {
            run_sync_task(&runtime, async move {
                let response = publisher.request(message).await?;
                Ok(Message::from_canonical(&response))
            })
        })
        .map_err(to_py_runtime_error)
    }
}

#[pyclass(module = "mq_bridge")]
struct MemoryDrainer {
    runtime: Arc<Runtime>,
    consumer: Arc<tokio::sync::Mutex<MemoryConsumer>>,
}

#[pymethods]
impl MemoryDrainer {
    #[staticmethod]
    #[pyo3(signature = (topic, capacity=65536))]
    fn from_topic(py: Python<'_>, topic: &str, capacity: usize) -> PyResult<Self> {
        let topic = topic.to_string();
        py.detach(move || -> anyhow::Result<Self> {
            Ok(Self {
                runtime: Arc::new(build_runtime()?),
                consumer: Arc::new(tokio::sync::Mutex::new(MemoryConsumer::new_local(
                    &topic, capacity,
                ))),
            })
        })
        .map_err(to_py_runtime_error)
    }

    #[pyo3(signature = (count, timeout=None, batch_size=256))]
    fn drain(
        &self,
        py: Python<'_>,
        count: usize,
        timeout: Option<f64>,
        batch_size: usize,
    ) -> PyResult<usize> {
        let runtime = Arc::clone(&self.runtime);
        let consumer = Arc::clone(&self.consumer);
        let batch_size = batch_size.max(1);
        py.detach(move || {
            run_sync_task(&runtime, async move {
                let drain_future = async move {
                    let mut drained = 0usize;
                    while drained < count {
                        let received_batch = {
                            let mut consumer = consumer.lock().await;
                            consumer.receive_batch(batch_size).await?
                        };
                        let batch_len = received_batch.messages.len();
                        if batch_len == 0 {
                            continue;
                        }
                        (received_batch.commit)(vec![MessageDisposition::Ack; batch_len]).await?;
                        drained += batch_len;
                    }
                    Ok(drained)
                };

                if let Some(timeout_secs) = timeout {
                    tokio::time::timeout(Duration::from_secs_f64(timeout_secs), drain_future)
                        .await
                        .map_err(|_| {
                            anyhow!(
                                "timed out after {:.3}s while draining {} message(s)",
                                timeout_secs,
                                count
                            )
                        })?
                } else {
                    drain_future.await
                }
            })
        })
        .map_err(to_py_runtime_error)
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
        .with_context(|| format!("failed to read YAML config from {}", path.display()))?;
    let value = serde_yaml_ng::from_str(&raw).context("failed to parse YAML config")?;
    Ok(unwrap_config_root(value))
}

fn load_named_route(path: &Path, name: &str) -> anyhow::Result<CoreRoute> {
    let value = load_config_value(path)?;
    if let Ok(document) = load_document_from_value(value.clone()) {
        if let Some(route) = document.routes.get(name).cloned() {
            return Ok(route);
        }
    }

    serde_yaml_ng::from_value(value).with_context(|| {
        format!("No route named '{name}' found, and the file could not be parsed as a single route")
    })
}

fn load_named_publisher(path: &Path, name: &str) -> anyhow::Result<Endpoint> {
    let value = load_config_value(path)?;
    if let Ok(document) = load_document_from_value(value.clone()) {
        if let Some(endpoint) = document.publishers.get(name).cloned() {
            return Ok(endpoint);
        }
    }

    serde_yaml_ng::from_value(value).with_context(|| {
        format!(
            "No publisher named '{name}' found, and the file could not be parsed as a single publisher endpoint"
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

fn unwrap_config_root(value: serde_yaml_ng::Value) -> serde_yaml_ng::Value {
    if let Some(map) = value.as_mapping() {
        let config_key = serde_yaml_ng::Value::String("config".to_string());
        if let Some(config) = map.get(&config_key) {
            return config.clone();
        }
    }
    value
}

fn parse_publishers_section(value: serde_yaml_ng::Value) -> anyhow::Result<PublisherConfig> {
    match value {
        serde_yaml_ng::Value::Mapping(_) => {
            serde_yaml_ng::from_value(value).context("failed to parse 'publishers' section")
        }
        serde_yaml_ng::Value::Sequence(_) => {
            let entries: Vec<NamedPublisher> = serde_yaml_ng::from_value(value)
                .context("failed to parse 'publishers' array section")?;
            Ok(entries
                .into_iter()
                .map(|entry| (entry.name, entry.endpoint))
                .collect())
        }
        other => Err(anyhow!(
            "failed to parse 'publishers' section: expected a map or array, got {other:?}"
        )),
    }
}

fn validate_message_id(id: Option<&str>) -> PyResult<()> {
    if let Some(id) = id {
        let _ = parse_message_id(id)?;
    }
    Ok(())
}

fn parse_message_id(id: &str) -> PyResult<u128> {
    core::canonical_message::deserialize_u128(JsonValue::String(id.to_string()).into_deserializer())
        .map_err(|err| PyValueError::new_err(format!("invalid message id '{id}': {err}")))
}

fn build_message(
    payload: Vec<u8>,
    metadata: Option<HashMap<String, String>>,
    id: Option<&str>,
) -> PyResult<CanonicalMessage> {
    let message_id = id.map(parse_message_id).transpose()?;
    let mut message = CanonicalMessage::new(payload, message_id);
    if let Some(metadata) = metadata {
        message.metadata.extend(metadata);
    }
    Ok(message)
}

fn message_input_to_canonical(
    message: &Bound<'_, PyAny>,
    metadata: Option<HashMap<String, String>>,
) -> PyResult<CanonicalMessage> {
    if let Ok(py_message) = message.cast::<Message>() {
        if metadata.is_some() {
            return Err(PyTypeError::new_err(
                "metadata must be None when message is a Message instance",
            ));
        }
        return py_message.borrow().to_canonical();
    }

    if let Ok(payload) = message.extract::<Vec<u8>>() {
        return build_message(payload, metadata, None);
    }

    Err(PyTypeError::new_err(
        "message must be a Message instance or bytes-like payload",
    ))
}

fn json_input_to_canonical(
    data: &Bound<'_, PyAny>,
    metadata: Option<HashMap<String, String>>,
    id: Option<&str>,
) -> PyResult<CanonicalMessage> {
    validate_message_id(id)?;
    build_message(python_to_json_bytes(data)?, metadata, id)
}

fn format_message_id(message_id: u128) -> String {
    fast_uuid_v7::format_uuid(message_id).to_string()
}

fn invoke_python_handler(
    callable: Arc<Py<PyAny>>,
    mode: PythonHandlerMode,
    label: String,
    message: CanonicalMessage,
) -> Result<Handled, HandlerError> {
    let message_id = message.message_id;
    Python::attach(|py| -> PyResult<Handled> {
        let arg = match mode {
            PythonHandlerMode::Message => {
                Py::new(py, Message::from_canonical(&message))?.into_any()
            }
            PythonHandlerMode::Json => json_bytes_to_python(py, message.payload.as_ref())?,
        };

        let result = callable.bind(py).call1((arg,))?;
        python_result_to_handled(&result, message_id, &message.metadata)
    })
    .map_err(|err| python_error_to_handler_error(py_err_context(&label, message_id), err))
}

fn invoke_python_handler_many(
    callable: Arc<Py<PyAny>>,
    mode: PythonHandlerMode,
    label: String,
    messages: Vec<CanonicalMessage>,
) -> Vec<Result<Handled, HandlerError>> {
    Python::attach(|py| {
        messages
            .into_iter()
            .map(|message| {
                let message_id = message.message_id;
                let arg = match mode {
                    PythonHandlerMode::Message => {
                        Py::new(py, Message::from_canonical(&message)).map(|msg| msg.into_any())
                    }
                    PythonHandlerMode::Json => json_bytes_to_python(py, message.payload.as_ref()),
                };

                match arg {
                    Ok(arg) => match callable.bind(py).call1((arg,)) {
                        Ok(result) => {
                            python_result_to_handled(&result, message_id, &message.metadata)
                                .map_err(|err| {
                                    python_error_to_handler_error(
                                        py_err_context(&label, message_id),
                                        err,
                                    )
                                })
                        }
                        Err(err) => Err(python_error_to_handler_error(
                            py_err_context(&label, message_id),
                            err,
                        )),
                    },
                    Err(err) => Err(python_error_to_handler_error(
                        py_err_context(&label, message_id),
                        err,
                    )),
                }
            })
            .collect()
    })
}

struct PyErrorContext<'a> {
    label: &'a str,
    message_id: u128,
}

fn py_err_context(label: &str, message_id: u128) -> PyErrorContext<'_> {
    PyErrorContext { label, message_id }
}

fn python_error_to_handler_error(ctx: PyErrorContext<'_>, err: PyErr) -> HandlerError {
    Python::attach(|py| {
        error!(
            handler = %ctx.label,
            message_id = %format_message_id(ctx.message_id),
            "Python handler raised an exception: {err}"
        );
        let message = anyhow!("Python handler failed for '{}': {}", ctx.label, err);
        if err.is_instance_of::<RetryableError>(py) {
            HandlerError::Retryable(message)
        } else if err.is_instance_of::<NonRetryableError>(py) {
            HandlerError::NonRetryable(message)
        } else {
            HandlerError::NonRetryable(message)
        }
    })
}

fn python_result_to_handled(
    obj: &Bound<'_, PyAny>,
    message_id: u128,
    inherited_metadata: &HashMap<String, String>,
) -> PyResult<Handled> {
    if obj.is_none() {
        return Ok(Handled::Ack);
    }

    if let Ok(message) = obj.cast::<Message>() {
        let mut message = message.borrow().to_canonical()?;
        message.set_id(message_id);
        return Ok(Handled::Publish(message));
    }

    if obj.is_instance_of::<PyBytes>() || obj.is_instance_of::<PyByteArray>() {
        let mut message = CanonicalMessage::from_vec(obj.extract::<Vec<u8>>()?);
        message.metadata.extend(inherited_metadata.clone());
        message.set_id(message_id);
        return Ok(Handled::Publish(message));
    }

    if let Ok(text) = obj.extract::<String>() {
        let mut message = CanonicalMessage::from(text);
        message.metadata.extend(inherited_metadata.clone());
        message.set_id(message_id);
        return Ok(Handled::Publish(message));
    }

    let mut message = CanonicalMessage::from_vec(python_to_json_bytes(obj)?);
    message.metadata.extend(inherited_metadata.clone());
    message.set_id(message_id);
    Ok(Handled::Publish(message))
}

struct PyJsonDecodeSeed<'py> {
    py: Python<'py>,
}

struct PyJsonDecodeVisitor<'py> {
    py: Python<'py>,
}

impl<'de, 'py> DeserializeSeed<'de> for PyJsonDecodeSeed<'py> {
    type Value = Py<PyAny>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(PyJsonDecodeVisitor { py: self.py })
    }
}

impl<'de, 'py> Visitor<'de> for PyJsonDecodeVisitor<'py> {
    type Value = Py<PyAny>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(self.py.None())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(self.py.None())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        PyJsonDecodeSeed { py: self.py }.deserialize(deserializer)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value.into_py_any(self.py).map_err(E::custom)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value.into_py_any(self.py).map_err(E::custom)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value.into_py_any(self.py).map_err(E::custom)
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value.into_py_any(self.py).map_err(E::custom)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value.into_py_any(self.py).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value.into_py_any(self.py).map_err(E::custom)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let list = PyList::empty(self.py);
        while let Some(value) = seq.next_element_seed(PyJsonDecodeSeed { py: self.py })? {
            list.append(value.bind(self.py)).map_err(A::Error::custom)?;
        }
        Ok(list.into_any().unbind())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let dict = PyDict::new(self.py);
        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value_seed(PyJsonDecodeSeed { py: self.py })?;
            dict.set_item(key, value.bind(self.py))
                .map_err(A::Error::custom)?;
        }
        Ok(dict.into_any().unbind())
    }
}

struct PyJsonEncodeContext {
    seen: RefCell<HashSet<usize>>,
}

struct PyJsonEncodeValue<'a, 'py> {
    obj: &'a Bound<'py, PyAny>,
    ctx: &'a PyJsonEncodeContext,
    depth: usize,
}

struct PyJsonContainerGuard<'a> {
    ctx: &'a PyJsonEncodeContext,
    ptr: usize,
}

impl Drop for PyJsonContainerGuard<'_> {
    fn drop(&mut self) {
        self.ctx.seen.borrow_mut().remove(&self.ptr);
    }
}

impl<'a, 'py> PyJsonEncodeValue<'a, 'py> {
    fn enter_container<E>(&self, ptr: usize) -> Result<PyJsonContainerGuard<'a>, E>
    where
        E: SerError,
    {
        if self.depth >= MAX_JSON_DEPTH {
            return Err(E::custom(format!(
                "Python value exceeds the maximum JSON nesting depth of {MAX_JSON_DEPTH}"
            )));
        }

        let mut seen = self.ctx.seen.borrow_mut();
        if !seen.insert(ptr) {
            return Err(E::custom(
                "Cyclic Python container values are not supported",
            ));
        }
        Ok(PyJsonContainerGuard { ctx: self.ctx, ptr })
    }
}

impl Serialize for PyJsonEncodeValue<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.obj.is_none() {
            return serializer.serialize_none();
        }

        if let Ok(value) = self.obj.extract::<bool>() {
            return serializer.serialize_bool(value);
        }

        if let Ok(dict) = self.obj.cast::<PyDict>() {
            let _guard = self.enter_container::<S::Error>(dict.as_ptr() as usize)?;
            let mut map = serializer.serialize_map(Some(dict.len()))?;
            for (key, value) in dict.iter() {
                let key = key.extract::<String>().map_err(S::Error::custom)?;
                map.serialize_entry(
                    &key,
                    &PyJsonEncodeValue {
                        obj: &value,
                        ctx: self.ctx,
                        depth: self.depth + 1,
                    },
                )?;
            }
            return map.end();
        }

        if let Ok(list) = self.obj.cast::<PyList>() {
            let _guard = self.enter_container::<S::Error>(list.as_ptr() as usize)?;
            let mut seq = serializer.serialize_seq(Some(list.len()))?;
            for value in list.iter() {
                seq.serialize_element(&PyJsonEncodeValue {
                    obj: &value,
                    ctx: self.ctx,
                    depth: self.depth + 1,
                })?;
            }
            return seq.end();
        }

        if let Ok(tuple) = self.obj.cast::<PyTuple>() {
            let _guard = self.enter_container::<S::Error>(tuple.as_ptr() as usize)?;
            let mut seq = serializer.serialize_seq(Some(tuple.len()))?;
            for value in tuple.iter() {
                seq.serialize_element(&PyJsonEncodeValue {
                    obj: &value,
                    ctx: self.ctx,
                    depth: self.depth + 1,
                })?;
            }
            return seq.end();
        }

        if let Ok(value) = self.obj.extract::<i64>() {
            return serializer.serialize_i64(value);
        }

        if let Ok(value) = self.obj.extract::<u64>() {
            return serializer.serialize_u64(value);
        }

        if let Ok(value) = self.obj.extract::<f64>() {
            if !value.is_finite() {
                return Err(S::Error::custom("NaN and infinity are not valid JSON"));
            }
            return serializer.serialize_f64(value);
        }

        if let Ok(value) = self.obj.extract::<String>() {
            return serializer.serialize_str(&value);
        }

        Err(S::Error::custom(
            "Python value must be a JSON scalar, list, tuple, or dict",
        ))
    }
}

fn json_bytes_to_python(py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyAny>> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let value = PyJsonDecodeSeed { py }
        .deserialize(&mut deserializer)
        .map_err(|err| PyValueError::new_err(format!("failed to decode JSON payload: {err}")))?;
    deserializer
        .end()
        .map_err(|err| PyValueError::new_err(format!("failed to decode JSON payload: {err}")))?;
    Ok(value)
}

fn python_to_json_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let ctx = PyJsonEncodeContext {
        seen: RefCell::new(HashSet::new()),
    };
    let mut payload = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut payload);
    PyJsonEncodeValue {
        obj,
        ctx: &ctx,
        depth: 0,
    }
    .serialize(&mut serializer)
    .map_err(|err| PyValueError::new_err(format!("failed to serialize JSON payload: {err}")))?;
    Ok(payload)
}

#[cfg(test)]
fn json_to_python(py: Python<'_>, value: &JsonValue) -> PyResult<Py<PyAny>> {
    let payload = serde_json::to_vec(value)
        .map_err(|err| PyValueError::new_err(format!("failed to serialize JSON payload: {err}")))?;
    json_bytes_to_python(py, &payload)
}

#[cfg(test)]
fn python_to_json(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    let payload = python_to_json_bytes(obj)?;
    serde_json::from_slice(&payload)
        .map_err(|err| PyValueError::new_err(format!("failed to serialize JSON payload: {err}")))
}

fn to_py_runtime_error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn run_sync_task<F, T>(runtime: &Runtime, future: F) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    runtime.block_on(future)
}

fn python_handler_semaphore() -> Arc<Semaphore> {
    Arc::clone(PYTHON_HANDLER_CONCURRENCY.get_or_init(|| {
        let limit = std::thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(4)
            .max(1);
        Arc::new(Semaphore::new(limit))
    }))
}

fn active_route_names() -> &'static Mutex<HashSet<String>> {
    ACTIVE_ROUTE_NAMES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn lock_active_route_names() -> PyResult<std::sync::MutexGuard<'static, HashSet<String>>> {
    active_route_names()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("Active route name lock poisoned"))
}

#[pymodule(gil_used = true)]
#[pyo3(name = "_mq_bridge")]
fn _mq_bridge(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Message>()?;
    module.add_class::<Route>()?;
    module.add_class::<Publisher>()?;
    module.add_class::<MemoryDrainer>()?;
    module.add("RetryableError", module.py().get_type::<RetryableError>())?;
    module.add(
        "NonRetryableError",
        module.py().get_type::<NonRetryableError>(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn write_yaml(contents: &str) -> String {
        let path =
            std::env::temp_dir().join(format!("mq-bridge-py-{}.yaml", fast_uuid_v7::gen_id()));
        fs::write(&path, contents).expect("failed to write test config");
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_route_from_yaml_supports_top_level_map() {
        let path = write_yaml(
            r#"
my_route:
  input:
    memory: { topic: "inbox", capacity: 8 }
  output:
    memory: { topic: "outbox", capacity: 8 }
"#,
        );

        let route = Python::attach(|py| Route::from_yaml(py, &path, "my_route")).unwrap();
        assert_eq!(route.name, "my_route");
    }

    #[test]
    fn test_route_from_yaml_supports_routes_section() {
        let path = write_yaml(
            r#"
routes:
  section_route:
    input:
      memory: { topic: "inbox", capacity: 8 }
    output:
      memory: { topic: "outbox", capacity: 8 }
"#,
        );

        let route = Python::attach(|py| Route::from_yaml(py, &path, "section_route")).unwrap();
        assert_eq!(route.name, "section_route");
    }

    #[test]
    fn test_route_from_yaml_supports_single_route_document() {
        let path = write_yaml(
            r#"
input:
  memory: { topic: "single-in", capacity: 8 }
output:
  memory: { topic: "single-out", capacity: 8 }
"#,
        );

        let route = Python::attach(|py| Route::from_yaml(py, &path, "orders_route")).unwrap();
        assert_eq!(route.name, "orders_route");
    }

    #[test]
    fn test_publisher_from_yaml_loads_publishers_section() {
        let path = write_yaml(
            r#"
publishers:
  echo:
    response: {}
"#,
        );

        let _publisher = Python::attach(|py| Publisher::from_yaml(py, &path, "echo")).unwrap();
    }

    #[test]
    fn test_publisher_from_yaml_supports_single_endpoint_document() {
        let path = write_yaml(
            r#"
memory:
  topic: "single-publisher"
  capacity: 8
"#,
        );

        let _publisher =
            Python::attach(|py| Publisher::from_yaml(py, &path, "orders_publisher")).unwrap();
    }

    #[test]
    fn test_load_document_supports_mqb_export_json() {
        let path = write_yaml(
            r#"
{
  "type": "mqb-export",
  "version": 1,
  "config": {
    "publishers": [
      {
        "name": "incoming",
        "endpoint": {
          "memory": {
            "topic": "orders",
            "capacity": 16
          }
        }
      }
    ],
    "routes": {
      "orders_route": {
        "input": {
          "memory": {
            "topic": "orders",
            "capacity": 16
          }
        },
        "output": {
          "response": {}
        }
      }
    }
  }
}
"#,
        );

        let document =
            load_document_from_value(load_config_value(Path::new(&path)).unwrap()).unwrap();
        assert!(document.routes.contains_key("orders_route"));
        assert!(document.publishers.contains_key("incoming"));
    }

    #[test]
    fn test_json_conversion_round_trip() {
        Python::attach(|py| {
            let value = json!({
                "name": "mq-bridge",
                "count": 3,
                "enabled": true,
                "items": [1, 2, null, {"nested": "ok"}]
            });

            let py_value = json_to_python(py, &value).unwrap();
            let round_trip = python_to_json(py_value.bind(py)).unwrap();
            assert_eq!(round_trip, value);
        });
    }

    #[test]
    fn test_python_result_mapping() {
        Python::attach(|py| {
            let none_value = py.None();
            assert!(matches!(
                python_result_to_handled(none_value.bind(py), 7, &HashMap::new()).unwrap(),
                Handled::Ack
            ));

            let py_message = Py::new(
                py,
                Message::new(
                    b"hello".to_vec(),
                    Some(HashMap::from([("kind".to_string(), "demo".to_string())])),
                    Some(format_message_id(9)),
                )
                .unwrap(),
            )
            .unwrap();
            match python_result_to_handled(
                py_message.bind(py).as_any(),
                11,
                &HashMap::from([("source".to_string(), "input".to_string())]),
            )
            .unwrap()
            {
                Handled::Publish(message) => {
                    assert_eq!(message.payload.as_ref(), b"hello");
                    assert_eq!(
                        message.metadata.get("kind").map(String::as_str),
                        Some("demo")
                    );
                    assert_eq!(message.message_id, 11);
                }
                Handled::Ack => panic!("expected publish"),
            }

            let py_dict = PyDict::new(py);
            py_dict.set_item("seen", 42).unwrap();
            match python_result_to_handled(
                py_dict.as_any(),
                13,
                &HashMap::from([
                    ("kind".to_string(), "demo.kind".to_string()),
                    ("reply_to".to_string(), "memory://reply".to_string()),
                ]),
            )
            .unwrap()
            {
                Handled::Publish(message) => {
                    let parsed: JsonValue = message.parse().unwrap();
                    assert_eq!(parsed, json!({ "seen": 42 }));
                    assert_eq!(
                        message.metadata.get("kind").map(String::as_str),
                        Some("demo.kind")
                    );
                    assert_eq!(
                        message.metadata.get("reply_to").map(String::as_str),
                        Some("memory://reply")
                    );
                    assert_eq!(message.message_id, 13);
                }
                Handled::Ack => panic!("expected publish"),
            }
        });
    }

    #[test]
    fn test_python_to_json_rejects_cycles() {
        Python::attach(|py| {
            let list = PyList::empty(py);
            list.append(list.clone()).unwrap();

            let err = python_to_json(list.as_any()).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(
                err.to_string()
                    .contains("Cyclic Python container values are not supported"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn test_python_error_mapping_respects_retryable_exceptions() {
        Python::attach(|py| {
            let retryable = PyErr::new::<RetryableError, _>("temporary");
            let non_retryable = PyErr::new::<NonRetryableError, _>("permanent");
            let generic = PyRuntimeError::new_err("boom");

            assert!(matches!(
                python_error_to_handler_error(py_err_context("demo", 1), retryable),
                HandlerError::Retryable(_)
            ));
            assert!(matches!(
                python_error_to_handler_error(py_err_context("demo", 2), non_retryable),
                HandlerError::NonRetryable(_)
            ));
            assert!(matches!(
                python_error_to_handler_error(py_err_context("demo", 3), generic),
                HandlerError::NonRetryable(_)
            ));

            let retryable_type = py.get_type::<RetryableError>();
            let non_retryable_type = py.get_type::<NonRetryableError>();
            assert_eq!(retryable_type.name().unwrap(), "RetryableError");
            assert_eq!(non_retryable_type.name().unwrap(), "NonRetryableError");
        });
    }

    #[test]
    fn test_with_handler_receives_message() {
        let path = write_yaml(
            r#"
routes:
  raw_route:
    input:
      memory: { topic: "inbox", capacity: 8 }
    output:
      memory: { topic: "outbox", capacity: 8 }
"#,
        );

        let route = Python::attach(|py| {
            Py::new(py, Route::from_yaml(py, &path, "raw_route").unwrap()).unwrap()
        });
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                pyo3::ffi::c_str!("def handle(msg):\n    return msg\n"),
                pyo3::ffi::c_str!("raw_route_handler.py"),
                pyo3::ffi::c_str!("raw_route_handler"),
            )
            .unwrap();
            let route_ref = route.bind(py).borrow_mut();
            Route::with_handler(route_ref, py, module.getattr("handle").unwrap().unbind()).unwrap();
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_handler_dispatches_json() {
        let path = write_yaml(
            r#"
routes:
  typed_route:
    input:
      memory: { topic: "typed-in", capacity: 8 }
    output:
      response: {}
"#,
        );

        let route = Python::attach(|py| {
            Py::new(py, Route::from_yaml(py, &path, "typed_route").unwrap()).unwrap()
        });
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                pyo3::ffi::c_str!("def handle(data):\n    return {'seen': data['value']}\n"),
                pyo3::ffi::c_str!("typed_route_handler.py"),
                pyo3::ffi::c_str!("typed_route_handler"),
            )
            .unwrap();
            let route_ref = route.bind(py).borrow_mut();
            Route::add_handler(
                route_ref,
                py,
                "demo.kind",
                module.getattr("handle").unwrap().unbind(),
            )
            .unwrap();
        });

        let handler = Python::attach(|py| {
            route
                .bind(py)
                .borrow()
                .route
                .lock()
                .unwrap()
                .output
                .handler
                .clone()
                .expect("handler should be attached")
        });

        let message = CanonicalMessage::from_json(json!({ "value": 21 }))
            .unwrap()
            .with_type_key("demo.kind");
        match handler.handle(message).await.unwrap() {
            Handled::Publish(message) => {
                let parsed: JsonValue = message.parse().unwrap();
                assert_eq!(parsed, json!({ "seen": 21 }));
                assert_eq!(
                    message.metadata.get("kind").map(String::as_str),
                    Some("demo.kind")
                );
            }
            Handled::Ack => panic!("expected publish"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_message_handler_dispatches_raw_message() {
        let path = write_yaml(
            r#"
routes:
  raw_typed_route:
    input:
      memory: { topic: "raw-typed-in", capacity: 8 }
    output:
      response: {}
"#,
        );

        let route = Python::attach(|py| {
            Py::new(py, Route::from_yaml(py, &path, "raw_typed_route").unwrap()).unwrap()
        });
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                pyo3::ffi::c_str!("def handle(msg):\n    return msg.payload\n"),
                pyo3::ffi::c_str!("raw_typed_route_handler.py"),
                pyo3::ffi::c_str!("raw_typed_route_handler"),
            )
            .unwrap();
            let route_ref = route.bind(py).borrow_mut();
            Route::add_message_handler(
                route_ref,
                py,
                "demo.raw",
                module.getattr("handle").unwrap().unbind(),
            )
            .unwrap();
        });

        let handler = Python::attach(|py| {
            route
                .bind(py)
                .borrow()
                .route
                .lock()
                .unwrap()
                .output
                .handler
                .clone()
                .expect("handler should be attached")
        });

        let message = CanonicalMessage::from_vec(b"hello".to_vec()).with_type_key("demo.raw");
        match handler.handle(message).await.unwrap() {
            Handled::Publish(message) => {
                assert_eq!(message.payload.as_ref(), b"hello");
                assert_eq!(
                    message.metadata.get("kind").map(String::as_str),
                    Some("demo.raw")
                );
            }
            Handled::Ack => panic!("expected publish"),
        }
    }

    #[test]
    fn test_publisher_send_and_request_support_message_and_bytes() {
        let path = write_yaml(
            r#"
publishers:
  echo:
    response: {}
"#,
        );

        let publisher = Python::attach(|py| Publisher::from_yaml(py, &path, "echo")).unwrap();
        Python::attach(|py| {
            let bytes_arg = PyBytes::new(py, b"hello");
            publisher
                .send(
                    py,
                    bytes_arg.as_any(),
                    Some(HashMap::from([("kind".to_string(), "demo".to_string())])),
                )
                .unwrap();

            let message = Py::new(
                py,
                Message::new(
                    b"world".to_vec(),
                    Some(HashMap::from([("kind".to_string(), "reply".to_string())])),
                    None,
                )
                .unwrap(),
            )
            .unwrap();
            let response = publisher
                .request(py, message.bind(py).as_any(), None)
                .unwrap();

            assert_eq!(response.payload, b"world");
            assert_eq!(
                response.metadata.get("kind").map(String::as_str),
                Some("reply")
            );
            assert!(response.id.is_some());
        });
    }

    #[test]
    fn test_publisher_send_json_and_request_json_serialize_in_rust() {
        let path = write_yaml(
            r#"
publishers:
  echo:
    response: {}
"#,
        );

        let publisher = Python::attach(|py| Publisher::from_yaml(py, &path, "echo")).unwrap();
        Python::attach(|py| {
            let data = PyDict::new(py);
            data.set_item("order_id", 42).unwrap();
            data.set_item("status", "created").unwrap();

            publisher
                .send_json(
                    py,
                    data.as_any(),
                    Some(HashMap::from([(
                        "kind".to_string(),
                        "order.created".to_string(),
                    )])),
                    None,
                )
                .unwrap();

            let response = publisher
                .request_json(py, data.as_any(), None, None)
                .unwrap();
            let parsed: JsonValue = serde_json::from_slice(&response.payload).unwrap();

            assert_eq!(parsed, json!({ "order_id": 42, "status": "created" }));
            assert!(response.id.is_some());
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_python_exceptions_become_handler_errors() {
        let handler = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                pyo3::ffi::c_str!("def fail(msg):\n    raise RuntimeError('boom')\n"),
                pyo3::ffi::c_str!("failing_handler.py"),
                pyo3::ffi::c_str!("failing_handler"),
            )
            .unwrap();
            PythonHandler::message("failing", module.getattr("fail").unwrap().unbind())
        });

        let message = CanonicalMessage::from_vec(b"hello".to_vec());
        let result = handler.handle(message).await;
        assert!(matches!(result, Err(HandlerError::NonRetryable(_))));
    }

    #[test]
    fn test_route_stop_unblocks_run() {
        let path = write_yaml(
            r#"
routes:
  stoppable_route:
    input:
      memory: { topic: "stop-in", capacity: 8 }
    output:
      memory: { topic: "stop-out", capacity: 8 }
"#,
        );

        let route =
            Arc::new(Python::attach(|py| Route::from_yaml(py, &path, "stoppable_route")).unwrap());
        let run_route = Arc::clone(&route);
        let thread = std::thread::spawn(move || {
            Python::attach(|py| run_route.run(py)).unwrap();
        });

        std::thread::sleep(Duration::from_millis(200));
        route.stop().unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn test_route_run_rejects_duplicate_name() {
        let path = write_yaml(
            r#"
routes:
  shared_route:
    input:
      memory: { topic: "shared-in", capacity: 8 }
    output:
      memory: { topic: "shared-out", capacity: 8 }
"#,
        );

        let first =
            Arc::new(Python::attach(|py| Route::from_yaml(py, &path, "shared_route")).unwrap());
        let second = Python::attach(|py| Route::from_yaml(py, &path, "shared_route")).unwrap();

        let run_route = Arc::clone(&first);
        let thread = std::thread::spawn(move || {
            Python::attach(|py| run_route.run(py)).unwrap();
        });

        std::thread::sleep(Duration::from_millis(200));

        let err = Python::attach(|py| second.run(py)).unwrap_err();
        assert!(
            err.to_string()
                .contains("A route named 'shared_route' is already running"),
            "unexpected error: {err}"
        );

        first.stop().unwrap();
        thread.join().unwrap();
    }
}
