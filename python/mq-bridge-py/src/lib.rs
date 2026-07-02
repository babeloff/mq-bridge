use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
#[cfg(test)]
use std::fs;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use ::mq_bridge as core;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use bytes::Bytes;
use core::canonical_message::format_message_id;
use core::endpoints::memory::MemoryConsumer;
use core::models::Endpoint;
use core::traits::{Handler, MessageConsumer, MessageDisposition};
use core::type_handler::TypeHandler;
use core::{
    CanonicalMessage, Handled, HandlerError, Publisher as CorePublisher, Route as CoreRoute, Sent,
};
use mq_bridge_bindings_common as common;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyList, PyModule, PyTuple, PyType};
use serde::de::{DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde::ser::{Error as SerError, SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
#[cfg(test)]
use serde_json::Value as JsonValue;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::error;

const MAX_JSON_DEPTH: usize = 64;

static PYTHON_HANDLER_CONCURRENCY: OnceLock<Option<Arc<Semaphore>>> = OnceLock::new();
static ACTIVE_ROUTE_NAMES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

create_exception!(mq_bridge, RetryableError, PyException);
create_exception!(mq_bridge, NonRetryableError, PyException);

#[derive(Default)]
struct RouteRunState {
    running: bool,
    stop_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<thread::JoinHandle<()>>,
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
    executor: PythonHandlerExecutor,
}

#[derive(Clone)]
enum PythonHandlerExecutor {
    Worker(Arc<PythonWorker>),
    Direct(Arc<Py<PyAny>>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PythonHandlerExecutorMode {
    Worker,
    Direct,
}

struct PythonWorker {
    tx: std::sync::mpsc::Sender<PythonWorkerRequest>,
}

struct PythonWorkerRequest {
    messages: Vec<CanonicalMessage>,
    reply_tx: oneshot::Sender<Vec<Result<Handled, HandlerError>>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl PythonHandler {
    fn message(label: impl Into<String>, callable: Py<PyAny>) -> Self {
        Self::new(label, PythonHandlerMode::Message, callable)
    }

    fn json(label: impl Into<String>, callable: Py<PyAny>) -> Self {
        Self::new(label, PythonHandlerMode::Json, callable)
    }

    fn new(label: impl Into<String>, mode: PythonHandlerMode, callable: Py<PyAny>) -> Self {
        Self::with_executor_mode(label, mode, callable, python_handler_executor_mode())
    }

    fn with_executor_mode(
        label: impl Into<String>,
        mode: PythonHandlerMode,
        callable: Py<PyAny>,
        executor_mode: PythonHandlerExecutorMode,
    ) -> Self {
        let label = label.into();
        let executor = match executor_mode {
            PythonHandlerExecutorMode::Worker => PythonHandlerExecutor::Worker(Arc::new(
                PythonWorker::spawn(label.clone(), mode, callable),
            )),
            PythonHandlerExecutorMode::Direct => PythonHandlerExecutor::Direct(Arc::new(callable)),
        };
        Self {
            label,
            mode,
            executor,
        }
    }

    async fn invoke_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Vec<Result<Handled, HandlerError>> {
        let len = messages.len();
        let permit = if let Some(semaphore) = python_handler_semaphore() {
            match semaphore.acquire_owned().await {
                Ok(permit) => Some(permit),
                Err(err) => {
                    let error =
                        HandlerError::NonRetryable(anyhow!("Python handler limit failed: {err}"));
                    return std::iter::repeat_with(|| {
                        Err(HandlerError::NonRetryable(anyhow!(error.to_string())))
                    })
                    .take(len)
                    .collect();
                }
            }
        } else {
            None
        };

        match &self.executor {
            PythonHandlerExecutor::Worker(worker) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if let Err(err) = worker.tx.send(PythonWorkerRequest {
                    messages,
                    reply_tx,
                    permit,
                }) {
                    let error = HandlerError::NonRetryable(anyhow!(
                        "Python handler worker unavailable for '{}': {}",
                        self.label,
                        err
                    ));
                    return std::iter::repeat_with(|| {
                        Err(HandlerError::NonRetryable(anyhow!(error.to_string())))
                    })
                    .take(len)
                    .collect();
                }

                match reply_rx.await {
                    Ok(results) => results,
                    Err(err) => std::iter::repeat_with(|| {
                        Err(HandlerError::NonRetryable(anyhow!(
                            "Python handler worker failed for '{}': {err}",
                            self.label
                        )))
                    })
                    .take(len)
                    .collect(),
                }
            }
            PythonHandlerExecutor::Direct(callable) => {
                let _permit = permit;
                invoke_python_handler_many(callable.as_ref(), self.mode, &self.label, messages)
            }
        }
    }
}

fn python_handler_executor_mode() -> PythonHandlerExecutorMode {
    match std::env::var("MQ_BRIDGE_PY_HANDLER_EXECUTOR") {
        Ok(value) => parse_python_handler_executor_mode(Some(value.as_str())),
        Err(_) => PythonHandlerExecutorMode::Worker,
    }
}

fn parse_python_handler_executor_mode(value: Option<&str>) -> PythonHandlerExecutorMode {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) if value.eq_ignore_ascii_case("direct") => PythonHandlerExecutorMode::Direct,
        Some(value) if value.eq_ignore_ascii_case("worker") => PythonHandlerExecutorMode::Worker,
        Some(value) => {
            tracing::warn!(
                value,
                "Unknown MQ_BRIDGE_PY_HANDLER_EXECUTOR value; falling back to worker"
            );
            PythonHandlerExecutorMode::Worker
        }
        None => PythonHandlerExecutorMode::Worker,
    }
}

impl PythonWorker {
    fn spawn(label: String, mode: PythonHandlerMode, callable: Py<PyAny>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<PythonWorkerRequest>();
        thread::Builder::new()
            .name(format!("mqb-py-{}", label))
            .spawn(move || {
                while let Ok(first) = rx.recv() {
                    // Drain any requests that piled up while we were busy so a
                    // single GIL acquisition serves all of them. Under load this
                    // amortizes the Python interpreter entry/exit cost across many
                    // requests (the dominant per-request cost at high concurrency);
                    // at low load the channel is empty and this stays a single call.
                    let mut requests = vec![first];
                    while let Ok(next) = rx.try_recv() {
                        requests.push(next);
                    }

                    if requests.len() == 1 {
                        let request = requests.pop().unwrap();
                        let _permit = request.permit;
                        let results =
                            invoke_python_handler_many(&callable, mode, &label, request.messages);
                        let _ = request.reply_tx.send(results);
                        continue;
                    }

                    // Coalesce: flatten every queued request's messages into one
                    // batch, invoke Python once, then split results back to each
                    // caller in order (invoke_python_handler_many preserves order).
                    let mut all_messages = Vec::new();
                    let mut replies = Vec::with_capacity(requests.len());
                    let mut permits = Vec::with_capacity(requests.len());
                    for request in requests {
                        let count = request.messages.len();
                        all_messages.extend(request.messages);
                        replies.push((request.reply_tx, count));
                        permits.push(request.permit);
                    }

                    let mut results =
                        invoke_python_handler_many(&callable, mode, &label, all_messages)
                            .into_iter();
                    for (reply_tx, count) in replies {
                        let chunk: Vec<_> = results.by_ref().take(count).collect();
                        let _ = reply_tx.send(chunk);
                    }
                    drop(permits);
                }
            })
            .expect("failed to spawn Python handler worker");
        Self { tx }
    }
}

#[async_trait]
impl Handler for PythonHandler {
    async fn handle(&self, msg: CanonicalMessage) -> Result<Handled, HandlerError> {
        match self.invoke_batch(vec![msg]).await.into_iter().next() {
            Some(result) => result,
            None => Err(HandlerError::NonRetryable(anyhow!(
                "Python handler worker returned no result for '{}'",
                self.label
            ))),
        }
    }

    async fn handle_many(&self, msgs: Vec<CanonicalMessage>) -> Vec<Result<Handled, HandlerError>> {
        self.invoke_batch(msgs).await
    }
}

#[pyclass(module = "mq_bridge")]
#[derive(Debug)]
struct Message {
    payload: Bytes,
    metadata: HashMap<String, String>,
    id: Option<u128>,
}

impl Message {
    fn from_canonical(message: &CanonicalMessage) -> Self {
        Self {
            payload: message.payload.clone(),
            metadata: message.metadata.clone(),
            id: Some(message.message_id),
        }
    }

    fn to_canonical(&self) -> PyResult<CanonicalMessage> {
        let mut message = CanonicalMessage::new_bytes(self.payload.clone(), self.id);
        message.metadata = self.metadata.clone();
        Ok(message)
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
        let id = id.as_deref().map(parse_message_id).transpose()?;
        Ok(Self {
            payload: Bytes::from(payload),
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
        let id = id.as_deref().map(parse_message_id).transpose()?;
        let payload = python_to_json_bytes(data)?;
        Ok(Self {
            payload: Bytes::from(payload),
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
        self.id.map(format_message_id)
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
            payload: Bytes::from(python_to_json_bytes(data)?),
            metadata: self.metadata.clone(),
            id: self.id,
        })
    }

    fn with_payload(&self, payload: Vec<u8>) -> Self {
        Self {
            payload: Bytes::from(payload),
            metadata: self.metadata.clone(),
            id: self.id,
        }
    }

    fn __repr__(&self) -> String {
        let id = self.id.map(format_message_id);
        format!(
            "Message(id={:?}, metadata={:?}, payload_len={})",
            id,
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
    /// Build a route from a YAML or JSON config file. Accepts a `routes:`
    /// document, a bare `{name: route}` map, or a single route body. Omit
    /// `name` (or pass `""`) when the file is a single bare route body.
    #[staticmethod]
    #[pyo3(signature = (path, name=None))]
    fn from_file(py: Python<'_>, path: &str, name: Option<&str>) -> PyResult<Self> {
        let path = path.to_string();
        let name = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let route = common::load_named_route(Path::new(&path), name.as_deref())?;
            Self::build(route, name.unwrap_or_else(common::default_route_name))
        })
        .map_err(to_py_runtime_error)
    }

    /// Deprecated alias for `from_file`.
    #[staticmethod]
    #[pyo3(signature = (path, name=None))]
    fn from_yaml(py: Python<'_>, path: &str, name: Option<&str>) -> PyResult<Self> {
        warn_deprecated(py, "Route.from_yaml is deprecated; use Route.from_file")?;
        Self::from_file(py, path, name)
    }

    /// Build a route from an in-memory YAML or JSON string. Accepts the same
    /// shapes as `from_file`. Omit `name` (or pass `""`) when the string is a
    /// single bare route body.
    #[staticmethod]
    #[pyo3(signature = (text, name=None))]
    fn from_str(py: Python<'_>, text: &str, name: Option<&str>) -> PyResult<Self> {
        let text = text.to_string();
        let name = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let value = serde_yaml_ng::from_str(&text).context("failed to parse YAML config")?;
            let route = common::named_route_from_value(value, name.as_deref())?;
            Self::build(route, name.unwrap_or_else(common::default_route_name))
        })
        .map_err(to_py_runtime_error)
    }

    /// Deprecated alias for `from_str`.
    #[staticmethod]
    #[pyo3(signature = (text, name=None))]
    fn from_yaml_str(py: Python<'_>, text: &str, name: Option<&str>) -> PyResult<Self> {
        warn_deprecated(py, "Route.from_yaml_str is deprecated; use Route.from_str")?;
        Self::from_str(py, text, name)
    }

    /// Build a route from an in-memory mapping (e.g. a Python ``dict``).
    ///
    /// The mapping may be a ``{"routes": {...}, "publishers": {...}}`` document,
    /// a bare ``{name: route}`` map, or a single route body. ``mq_bridge.config``
    /// exposes ``TypedDict`` types (``ConfigDocument``, ``RouteConfig``,
    /// ``EndpointConfig``) for editor autocompletion, and ``config_schema()``
    /// returns the full JSON Schema. Example::
    ///
    ///     Route.from_config(
    ///         {"routes": {"orders": {
    ///             "input": {"memory": {"topic": "orders.in"}},
    ///             "output": {"response": {}},
    ///         }}},
    ///         "orders",
    ///     )
    ///
    /// Omit ``name`` (or pass ``""``) to treat the mapping as a single bare
    /// route body, in which case a name is generated automatically::
    ///
    ///     Route.from_config({
    ///         "input": {"memory": {"topic": "orders.in"}},
    ///         "output": {"response": {}},
    ///     })
    #[staticmethod]
    #[pyo3(signature = (config, name=None))]
    fn from_config(
        py: Python<'_>,
        config: &Bound<'_, PyAny>,
        name: Option<&str>,
    ) -> PyResult<Self> {
        let bytes = python_to_json_bytes(config)?;
        let name = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let text = std::str::from_utf8(&bytes).context("config is not valid UTF-8")?;
            let value = serde_yaml_ng::from_str(text).context("failed to parse config mapping")?;
            let route = common::named_route_from_value(value, name.as_deref())?;
            Self::build(route, name.unwrap_or_else(common::default_route_name))
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

    /// Deploy the route and block the calling thread until `stop()` is called
    /// (typically from another thread). Use `start()` instead if you want to
    /// keep running Python code after the route is up.
    fn run(&self, py: Python<'_>) -> PyResult<()> {
        let route = self.lock_route()?.clone();
        let stop_rx = self.begin_run()?;
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
            finish_run(&run_state, &name);
            result
        })
        .map_err(to_py_runtime_error)
    }

    /// Deploy the route and return immediately, running it on a background
    /// thread. Configuration and connection errors surface here. Call `stop()`
    /// (and optionally `join()`) to shut it down. The route is also usable as a
    /// context manager, which starts on enter and stops on exit.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let route = self.lock_route()?.clone();
        let stop_rx = self.begin_run()?;
        let name = self.name.clone();
        let runtime = Arc::clone(&self.runtime);
        let run_state = Arc::clone(&self.run_state);

        // Deploy synchronously so config/connection errors raise from start()
        // rather than disappearing on the background thread.
        let deploy_name = name.clone();
        let deploy_runtime = Arc::clone(&runtime);
        let deploy_result = py.detach(move || {
            deploy_runtime.block_on(async move { route.deploy(&deploy_name).await })
        });
        if let Err(err) = deploy_result {
            finish_run(&run_state, &name);
            return Err(to_py_runtime_error(err));
        }

        let wait_name = name.clone();
        let wait_run_state = Arc::clone(&run_state);
        let cleanup_runtime = Arc::clone(&runtime);
        let handle = match thread::Builder::new()
            .name(format!("mqb-route-{name}"))
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
                // The route is already deployed and run_state marked running; a failed
                // spawn would otherwise leak the deployed route and its name reservation.
                py.detach(|| cleanup_runtime.block_on(async { core::Route::stop(&name).await }));
                finish_run(&run_state, &name);
                return Err(to_py_runtime_error(err));
            }
        };

        self.lock_run_state()?.join_handle = Some(handle);
        Ok(())
    }

    /// Block until a route started with `start()` has fully stopped. No-op for
    /// routes that were never started or that ran via the blocking `run()`.
    fn join(&self, py: Python<'_>) -> PyResult<()> {
        let handle = self.lock_run_state()?.join_handle.take();
        if let Some(handle) = handle {
            py.detach(|| handle.join())
                .map_err(|_| PyRuntimeError::new_err("Route background thread panicked"))?;
        }
        Ok(())
    }

    fn stop(&self) -> PyResult<()> {
        let stop_tx = self.lock_run_state()?.stop_tx.take();
        if let Some(stop_tx) = stop_tx {
            let _ = stop_tx.send(());
        }
        Ok(())
    }

    fn __enter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<PyRef<'a, Self>> {
        slf.start(py)?;
        Ok(slf)
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) -> PyResult<bool> {
        self.stop()?;
        self.join(py)?;
        Ok(false)
    }
}

impl Route {
    fn build(route: CoreRoute, name: String) -> anyhow::Result<Self> {
        Ok(Self {
            runtime: Arc::new(common::build_runtime()?),
            route: Arc::new(Mutex::new(route)),
            name,
            run_state: Arc::new(Mutex::new(RouteRunState::default())),
        })
    }

    /// Mark the route as running, reserve its name, and return the stop
    /// receiver. Shared prologue for both `run()` and `start()`.
    fn begin_run(&self) -> PyResult<oneshot::Receiver<()>> {
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
        Ok(stop_rx)
    }

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

impl Publisher {
    fn build(endpoint: Endpoint) -> anyhow::Result<Self> {
        let runtime = Arc::new(common::build_runtime()?);
        let publisher = runtime.block_on(CorePublisher::new(endpoint))?;
        Ok(Self { runtime, publisher })
    }
}

#[pymethods]
impl Publisher {
    /// Build a publisher from a YAML or JSON config file. Accepts a
    /// `publishers:` document, a bare `{name: endpoint}` map, or a single
    /// endpoint body. Omit `name` (or pass `""`) for a single bare endpoint.
    #[staticmethod]
    #[pyo3(signature = (path, name=None))]
    fn from_file(py: Python<'_>, path: &str, name: Option<&str>) -> PyResult<Self> {
        let path = path.to_string();
        let name = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            Self::build(common::load_named_publisher(
                Path::new(&path),
                name.as_deref(),
            )?)
        })
        .map_err(to_py_runtime_error)
    }

    /// Deprecated alias for `from_file`.
    #[staticmethod]
    #[pyo3(signature = (path, name=None))]
    fn from_yaml(py: Python<'_>, path: &str, name: Option<&str>) -> PyResult<Self> {
        warn_deprecated(
            py,
            "Publisher.from_yaml is deprecated; use Publisher.from_file",
        )?;
        Self::from_file(py, path, name)
    }

    /// Build a publisher from an in-memory YAML or JSON string. Omit `name`
    /// (or pass `""`) when the string is a single bare endpoint body.
    #[staticmethod]
    #[pyo3(signature = (text, name=None))]
    fn from_str(py: Python<'_>, text: &str, name: Option<&str>) -> PyResult<Self> {
        let text = text.to_string();
        let name = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let value = serde_yaml_ng::from_str(&text).context("failed to parse YAML config")?;
            Self::build(common::named_publisher_from_value(value, name.as_deref())?)
        })
        .map_err(to_py_runtime_error)
    }

    /// Deprecated alias for `from_str`.
    #[staticmethod]
    #[pyo3(signature = (text, name=None))]
    fn from_yaml_str(py: Python<'_>, text: &str, name: Option<&str>) -> PyResult<Self> {
        warn_deprecated(
            py,
            "Publisher.from_yaml_str is deprecated; use Publisher.from_str",
        )?;
        Self::from_str(py, text, name)
    }

    /// Build a publisher from an in-memory mapping. Omit `name` (or pass `""`)
    /// to treat the mapping as a single bare endpoint body, e.g.
    /// ``Publisher.from_config({"response": {}})``.
    #[staticmethod]
    #[pyo3(signature = (config, name=None))]
    fn from_config(
        py: Python<'_>,
        config: &Bound<'_, PyAny>,
        name: Option<&str>,
    ) -> PyResult<Self> {
        let bytes = python_to_json_bytes(config)?;
        let name = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let text = std::str::from_utf8(&bytes).context("config is not valid UTF-8")?;
            let value = serde_yaml_ng::from_str(text).context("failed to parse config mapping")?;
            Self::build(common::named_publisher_from_value(value, name.as_deref())?)
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
                runtime: Arc::new(common::build_runtime()?),
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
                        let needed = (count - drained).min(batch_size);
                        let received_batch = {
                            let mut consumer = consumer.lock().await;
                            consumer.receive_batch(needed).await?
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
                    if !timeout_secs.is_finite() || timeout_secs < 0.0 {
                        return Err(anyhow!(
                            "timeout must be a finite, non-negative number of seconds"
                        ));
                    }
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

/// A pull-based consumer over any mq-bridge input endpoint.
///
/// `poll()` receives a batch of messages but does **not** acknowledge them;
/// call `commit()` once the messages have been durably handled to ack every
/// batch returned since the previous commit (advancing offsets / removing them
/// from the source). This manual-commit model gives at-least-once delivery
/// across a failed downstream load (e.g. feeding a `dlt` resource). The endpoint
/// config decides durability (consumer vs subscriber mode), exactly as it does
/// for a route input.
///
/// Relationship to the Rust core: this is a boundary-friendly projection of the
/// core `MessageConsumer::receive_batch`, which is the low-level primitive. Two
/// differences are deliberate, both because a Rust commit closure cannot be
/// handed across the FFI boundary for Python to call later:
///   - `receive_batch` returns the messages *and* their commit closure together;
///     `poll()` returns only the messages and keeps the closure on the Rust side,
///     so committing becomes the separate `commit()` call.
///   - that closure accepts a per-message disposition vector (ack/nack/reject);
///     `poll()` + `commit()` only ack the whole batch.
/// `poll()` additionally layers a `timeout_ms` over `receive_batch` (which has no
/// timeout of its own). Native Rust code should use `receive_batch` directly — it
/// is strictly more expressive and needs no deferred-commit state.
#[pyclass(module = "mq_bridge")]
struct Consumer {
    runtime: Arc<Runtime>,
    // `None` once `close()` has dropped the underlying consumer.
    consumer: Arc<tokio::sync::Mutex<Option<Box<dyn MessageConsumer>>>>,
    // Polled-but-uncommitted batches, keyed by a monotonic token. Ordered so
    // `commit()` acks them oldest-first; `ack(token)`/`nack(token)` address one.
    pending: Arc<Mutex<BTreeMap<u64, (core::traits::BatchCommitFunc, usize)>>>,
    next_token: Arc<AtomicU64>,
    exhausted: Arc<std::sync::atomic::AtomicBool>,
    // `true` for cumulative-ack transports (Kafka, …) where acking a later batch
    // implicitly acks earlier ones, so token acks must stay oldest-first.
    requires_order: bool,
}

impl Consumer {
    fn build(name: String, endpoint: Endpoint) -> anyhow::Result<Self> {
        let runtime = Arc::new(common::build_runtime()?);
        let consumer = runtime.block_on(core::endpoints::create_consumer_from_route(
            &name, &endpoint,
        ))?;
        let requires_order = consumer.commit_requires_order();
        Ok(Self {
            runtime,
            consumer: Arc::new(tokio::sync::Mutex::new(Some(consumer))),
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            next_token: Arc::new(AtomicU64::new(0)),
            exhausted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            requires_order,
        })
    }

    fn lock_pending(
        &self,
    ) -> PyResult<std::sync::MutexGuard<'_, BTreeMap<u64, (core::traits::BatchCommitFunc, usize)>>>
    {
        self.pending
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Consumer commit lock poisoned"))
    }

    /// Receive up to `max` messages, registering the batch's commit closure under
    /// a fresh token. Returns `None` on timeout or end-of-stream, otherwise the
    /// messages and their token. Shared by `poll()` and `poll_batch()`.
    fn receive(
        &self,
        py: Python<'_>,
        max: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<(Vec<Message>, u64)>> {
        let runtime = Arc::clone(&self.runtime);
        let consumer = Arc::clone(&self.consumer);
        let exhausted = Arc::clone(&self.exhausted);
        let pending = Arc::clone(&self.pending);
        let next_token = Arc::clone(&self.next_token);
        let max = max.max(1);
        // Returns the raw canonical messages and the token they were registered
        // under. The token is allocated and the batch registered while still
        // holding the consumer lock, so token order matches receive order even
        // when several threads poll concurrently.
        let outcome = py
            .detach(move || {
                run_sync_task(&runtime, async move {
                    let mut guard = consumer.lock().await;
                    let consumer = guard
                        .as_mut()
                        .ok_or_else(|| anyhow!("consumer is closed"))?;
                    let recv = consumer.receive_batch(max);
                    let batch = if let Some(timeout_ms) = timeout_ms {
                        match tokio::time::timeout(Duration::from_millis(timeout_ms), recv).await {
                            Ok(result) => result,
                            Err(_) => return Ok(None),
                        }
                    } else {
                        recv.await
                    };
                    match batch {
                        Ok(batch) => {
                            let count = batch.messages.len();
                            if count == 0 {
                                return Ok(None);
                            }
                            let token = next_token.fetch_add(1, Ordering::SeqCst);
                            let mut pending = pending
                                .lock()
                                .map_err(|_| anyhow!("Consumer commit lock poisoned"))?;
                            if pending.contains_key(&token) {
                                return Err(anyhow!(
                                    "batch token space exhausted (token counter wrapped with batches still outstanding)"
                                ));
                            }
                            pending.insert(token, (batch.commit, count));
                            drop(pending);
                            Ok(Some((batch.messages, token)))
                        }
                        Err(core::errors::ConsumerError::EndOfStream) => {
                            exhausted.store(true, Ordering::SeqCst);
                            Ok(None)
                        }
                        Err(err) => Err(err.into()),
                    }
                })
            })
            .map_err(to_py_runtime_error)?;

        let Some((messages, token)) = outcome else {
            return Ok(None);
        };
        let messages: Vec<Message> = messages.iter().map(Message::from_canonical).collect();
        Ok(Some((messages, token)))
    }

    /// Run one batch's commit closure with a uniform disposition, removing it from
    /// `pending`. Used by `ack(token)` and `nack(token)`.
    fn commit_one(
        &self,
        py: Python<'_>,
        token: u64,
        disposition: MessageDisposition,
    ) -> PyResult<()> {
        let entry = {
            let mut pending = self.lock_pending()?;
            // On cumulative-ack transports, acking a later batch implicitly acks
            // the earlier ones, so an out-of-order ack would silently drop them.
            // Reject it (the token stays outstanding) instead of committing.
            if matches!(disposition, MessageDisposition::Ack) && self.requires_order {
                if let Some((&oldest, _)) = pending.iter().next() {
                    if token != oldest && pending.contains_key(&token) {
                        return Err(PyValueError::new_err(format!(
                            "cannot ack batch token {token} before older outstanding token {oldest}: this transport commits cumulatively, so acks must follow receive order (ack older batches first, or use commit())"
                        )));
                    }
                }
            }
            pending.remove(&token)
        };
        let Some((commit, len)) = entry else {
            return Err(PyValueError::new_err(format!(
                "unknown batch token {token} (already committed, or never polled)"
            )));
        };
        let runtime = Arc::clone(&self.runtime);
        let result = py
            .detach(move || {
                run_sync_task(
                    &runtime,
                    async move { Ok(commit(vec![disposition; len]).await) },
                )
            })
            .map_err(to_py_runtime_error)?;
        // On failure the closure consumed the batch; it cannot be retried by token.
        result.map_err(to_py_runtime_error)
    }
}

#[pymethods]
impl Consumer {
    /// Build a consumer from a YAML or JSON config file. Accepts a
    /// `consumers:` document entry (with `name`) or a single bare endpoint body.
    #[staticmethod]
    #[pyo3(signature = (path, name=None))]
    fn from_file(py: Python<'_>, path: &str, name: Option<&str>) -> PyResult<Self> {
        let path = path.to_string();
        let resolved = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let endpoint = common::load_named_consumer(Path::new(&path), resolved.as_deref())?;
            Self::build(
                resolved.unwrap_or_else(common::default_route_name),
                endpoint,
            )
        })
        .map_err(to_py_runtime_error)
    }

    /// Build a consumer from an in-memory YAML or JSON string.
    #[staticmethod]
    #[pyo3(signature = (text, name=None))]
    fn from_str(py: Python<'_>, text: &str, name: Option<&str>) -> PyResult<Self> {
        let text = text.to_string();
        let resolved = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let value = serde_yaml_ng::from_str(&text).context("failed to parse YAML config")?;
            let endpoint = common::named_consumer_from_value(value, resolved.as_deref())?;
            Self::build(
                resolved.unwrap_or_else(common::default_route_name),
                endpoint,
            )
        })
        .map_err(to_py_runtime_error)
    }

    /// Build a consumer from an in-memory mapping (e.g. a Python ``dict``).
    /// Omit ``name`` to treat the mapping as a single bare endpoint body, e.g.
    /// ``Consumer.from_config({"nats": {"subject": "orders", "url": ...}})``.
    #[staticmethod]
    #[pyo3(signature = (config, name=None))]
    fn from_config(
        py: Python<'_>,
        config: &Bound<'_, PyAny>,
        name: Option<&str>,
    ) -> PyResult<Self> {
        let bytes = python_to_json_bytes(config)?;
        let resolved = common::normalize_name(name).map(str::to_string);
        py.detach(move || -> anyhow::Result<Self> {
            let text = std::str::from_utf8(&bytes).context("config is not valid UTF-8")?;
            let value = serde_yaml_ng::from_str(text).context("failed to parse config mapping")?;
            let endpoint = common::named_consumer_from_value(value, resolved.as_deref())?;
            Self::build(
                resolved.unwrap_or_else(common::default_route_name),
                endpoint,
            )
        })
        .map_err(to_py_runtime_error)
    }

    /// Receive up to `max` messages without acknowledging them. Returns an empty
    /// list if `timeout_ms` milliseconds elapse with nothing received, or if the
    /// source is exhausted (see `exhausted`). Omit `timeout_ms` to block until a
    /// message arrives. The returned messages are committed by the next
    /// `commit()` call.
    #[pyo3(signature = (max=256, timeout_ms=None))]
    fn poll(&self, py: Python<'_>, max: usize, timeout_ms: Option<u64>) -> PyResult<Vec<Message>> {
        match self.receive(py, max, timeout_ms)? {
            Some((messages, _token)) => Ok(messages),
            None => Ok(Vec::new()),
        }
    }

    /// Like `poll()`, but also return the batch's token so it can be acked or
    /// nacked individually with `ack(token)` / `nack(token)` — the shape a `dlt`
    /// resource wants (`poll → yield → commit load package → ack(token)`).
    /// Returns `(messages, token)`, or `([], None)` on timeout or end-of-stream.
    /// Tokens stay outstanding until acked/nacked; `commit()` still acks every
    /// outstanding batch at once, so don't mix the two styles on one consumer.
    #[pyo3(signature = (max=256, timeout_ms=None))]
    fn poll_batch(
        &self,
        py: Python<'_>,
        max: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<(Vec<Message>, Option<u64>)> {
        match self.receive(py, max, timeout_ms)? {
            Some((messages, token)) => Ok((messages, Some(token))),
            None => Ok((Vec::new(), None)),
        }
    }

    /// Acknowledge a single batch by the token from `poll_batch()`, advancing the
    /// consumer offset for just that batch. Raises if the token is unknown
    /// (already acked/nacked, or never polled).
    fn ack(&self, py: Python<'_>, token: u64) -> PyResult<()> {
        self.commit_one(py, token, MessageDisposition::Ack)
    }

    /// Negatively acknowledge so the broker can redeliver. With a `token`, nacks
    /// just that batch; without one, nacks every outstanding batch (oldest
    /// first). On Kafka there is no per-message nack — this leaves the offset
    /// unadvanced, so redelivery happens on the next run/rebalance, not at once.
    #[pyo3(signature = (token=None))]
    fn nack(&self, py: Python<'_>, token: Option<u64>) -> PyResult<()> {
        if let Some(token) = token {
            return self.commit_one(py, token, MessageDisposition::Nack);
        }
        // Nack all outstanding batches, oldest first.
        let pending: Vec<(u64, (core::traits::BatchCommitFunc, usize))> =
            std::mem::take(&mut *self.lock_pending()?)
                .into_iter()
                .collect();
        if pending.is_empty() {
            return Ok(());
        }
        let runtime = Arc::clone(&self.runtime);
        let (err, tail) = py
            .detach(move || {
                run_sync_task(&runtime, async move {
                    let mut iter = pending.into_iter();
                    while let Some((_token, (commit, len))) = iter.next() {
                        if let Err(err) = commit(vec![MessageDisposition::Nack; len]).await {
                            return Ok((Some(err), iter.collect::<Vec<_>>()));
                        }
                    }
                    Ok((None, Vec::new()))
                })
            })
            .map_err(to_py_runtime_error)?;
        if !tail.is_empty() {
            let mut pending = self.lock_pending()?;
            for (token, entry) in tail {
                pending.insert(token, entry);
            }
        }
        match err {
            Some(err) => Err(to_py_runtime_error(err)),
            None => Ok(()),
        }
    }

    /// Acknowledge every batch returned by `poll()` since the last `commit()`,
    /// advancing the consumer offset.
    ///
    /// Calling this is required, not optional. Without it the offset never
    /// advances (messages are re-delivered on the next run), most brokers stall
    /// once their unacknowledged/prefetch window fills, and uncommitted batches
    /// are held in memory so the process grows unbounded. To retry a failed
    /// batch, simply don't commit it — it will be redelivered.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        // Drain oldest-first; `BTreeMap` iterates in token order.
        let commits: Vec<(u64, (core::traits::BatchCommitFunc, usize))> =
            std::mem::take(&mut *self.lock_pending()?)
                .into_iter()
                .collect();
        if commits.is_empty() {
            return Ok(());
        }
        let runtime = Arc::clone(&self.runtime);
        let (err, tail) = py
            .detach(move || {
                run_sync_task(&runtime, async move {
                    let mut iter = commits.into_iter();
                    while let Some((_token, (commit, len))) = iter.next() {
                        if let Err(err) = commit(vec![MessageDisposition::Ack; len]).await {
                            // Hand back the batches we never attempted so they can be retried.
                            return Ok((Some(err), iter.collect::<Vec<_>>()));
                        }
                    }
                    Ok((None, Vec::new()))
                })
            })
            .map_err(to_py_runtime_error)?;
        if !tail.is_empty() {
            // Re-insert the un-attempted batches under their original tokens.
            let mut pending = self.lock_pending()?;
            for (token, entry) in tail {
                pending.insert(token, entry);
            }
        }
        match err {
            Some(err) => Err(to_py_runtime_error(err)),
            None => Ok(()),
        }
    }

    /// `True` once the source has signalled end-of-stream (e.g. a fully drained
    /// file). Streaming brokers never set this.
    #[getter]
    fn exhausted(&self) -> bool {
        self.exhausted.load(Ordering::SeqCst)
    }

    /// Return a status snapshot for the underlying endpoint as a ``dict``:
    /// ``healthy``, ``target``, optional ``pending`` (broker backlog/lag where
    /// the transport reports it — e.g. Kafka offset lag, AMQP queue depth, NATS
    /// JetStream ``num_pending``), optional ``capacity``/``error``, and
    /// ``details``. ``pending == 0`` is a precise "caught up" signal on those
    /// transports; it is `null` where the broker exposes no backlog (e.g. core
    /// NATS, MQTT). The value is a point-in-time snapshot, not a guarantee.
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let runtime = Arc::clone(&self.runtime);
        let consumer = Arc::clone(&self.consumer);
        let bytes = py
            .detach(move || -> anyhow::Result<Vec<u8>> {
                run_sync_task(&runtime, async move {
                    let guard = consumer.lock().await;
                    let consumer = guard
                        .as_ref()
                        .ok_or_else(|| anyhow!("consumer is closed"))?;
                    Ok(serde_json::to_vec(&consumer.status().await)?)
                })
            })
            .map_err(to_py_runtime_error)?;
        json_bytes_to_python(py, &bytes)
    }

    /// Release the underlying consumer connection. Idempotent. After this,
    /// `poll()` and `status()` raise. Prefer the context-manager form, which
    /// calls this on exit. GC'd Python has no deterministic drop, so closing
    /// explicitly is how the broker connection is freed promptly.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = Arc::clone(&self.runtime);
        let consumer = Arc::clone(&self.consumer);
        py.detach(move || {
            run_sync_task(&runtime, async move {
                let taken = consumer.lock().await.take();
                if let Some(mut consumer) = taken {
                    consumer.close().await?;
                }
                Ok(())
            })
        })
        .map_err(to_py_runtime_error)
    }

    fn __enter__<'a>(slf: PyRef<'a, Self>) -> PyRef<'a, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

/// Emit a Python `DeprecationWarning` from a deprecated constructor alias.
fn warn_deprecated(py: Python<'_>, message: &str) -> PyResult<()> {
    // Build the C string at runtime rather than with a `c"..."` literal so the
    // crate keeps compiling on its MSRV (c-string literals stabilized in 1.77).
    let message =
        std::ffi::CString::new(message).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    PyErr::warn(
        py,
        &py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
        &message,
        1,
    )
}

fn validate_message_id(id: Option<&str>) -> PyResult<()> {
    if let Some(id) = id {
        let _ = parse_message_id(id)?;
    }
    Ok(())
}

fn parse_message_id(id: &str) -> PyResult<u128> {
    core::canonical_message::message_id_from_str(id).map_err(PyValueError::new_err)
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

/// Cyclic-GC strategy for the Python handler interpreter.
///
/// CPython's cyclic collector runs a stop-the-world sweep roughly every 700
/// net container allocations. At high request rates that fires hundreds of
/// times per second, each pause landing in a random request's tail latency.
/// Because mq-bridge runs Python handlers on a single worker interpreter we can
/// take the collector over: disable it and run `gc.collect()` off the hot path
/// on a fixed request cadence instead. Reference counting still frees the vast
/// majority of objects immediately, so only genuine reference cycles wait for
/// the periodic sweep. Inspired by pyronova's GC-takeover approach.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GcMode {
    /// Leave CPython's cyclic GC untouched (default; fully backward compatible).
    Default,
    /// Disable the cyclic GC; run `gc.collect()` every `threshold` messages.
    Count,
    /// Disable the cyclic GC entirely; never auto-collect (pure refcount).
    Off,
}

struct GcConfig {
    mode: GcMode,
    threshold: u64,
}

fn gc_config() -> &'static GcConfig {
    static GC_CONFIG: OnceLock<GcConfig> = OnceLock::new();
    GC_CONFIG.get_or_init(|| {
        let mode = match std::env::var("MQ_BRIDGE_PY_GC_MODE") {
            Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
                "count" => GcMode::Count,
                "off" => GcMode::Off,
                "" | "default" => GcMode::Default,
                other => {
                    tracing::warn!(
                        value = other,
                        "Unknown MQ_BRIDGE_PY_GC_MODE; leaving CPython GC untouched"
                    );
                    GcMode::Default
                }
            },
            Err(_) => GcMode::Default,
        };
        let threshold = std::env::var("MQ_BRIDGE_PY_GC_THRESHOLD")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|count| *count > 0)
            .unwrap_or(100_000);
        GcConfig { mode, threshold }
    })
}

/// Disable CPython's cyclic GC the first time a worker holds the GIL. Cheap
/// (`Once` fast path) on every subsequent call.
fn ensure_gc_configured(py: Python<'_>) {
    static CONFIGURED: std::sync::Once = std::sync::Once::new();
    let cfg = gc_config();
    if cfg.mode == GcMode::Default {
        return;
    }
    CONFIGURED.call_once(
        || match py.import("gc").and_then(|gc| gc.call_method0("disable")) {
            Ok(_) => tracing::info!(
                mode = if cfg.mode == GcMode::Count {
                    "count"
                } else {
                    "off"
                },
                threshold = cfg.threshold,
                "Python cyclic GC disabled; mq-bridge driving collection off the hot path"
            ),
            Err(err) => tracing::warn!("Failed to disable Python GC: {err}"),
        },
    );
}

/// Account for `processed` handled messages and, in `count` mode, trigger
/// `gc.collect()` when a threshold boundary is crossed. Runs under the GIL.
fn gc_tick(py: Python<'_>, processed: u64) {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let cfg = gc_config();
    if cfg.mode != GcMode::Count || processed == 0 {
        return;
    }
    let prev = COUNTER.fetch_add(processed, Ordering::Relaxed);
    // Only collect when this batch pushes the running total past a multiple of
    // the threshold, so the sweep is amortized across `threshold` requests.
    if prev / cfg.threshold != (prev + processed) / cfg.threshold {
        if let Ok(gc) = py.import("gc") {
            let _ = gc.call_method0("collect");
        }
    }
}

fn invoke_python_handler_many(
    callable: &Py<PyAny>,
    mode: PythonHandlerMode,
    label: &str,
    messages: Vec<CanonicalMessage>,
) -> Vec<Result<Handled, HandlerError>> {
    let processed = messages.len() as u64;
    Python::attach(|py| {
        ensure_gc_configured(py);
        let results: Vec<Result<Handled, HandlerError>> = messages
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
                        Ok(result) => python_result_to_handled(
                            &result,
                            message_id,
                            message.metadata,
                        )
                        .map_err(|err| {
                            python_error_to_handler_error(py_err_context(label, message_id), err)
                        }),
                        Err(err) => Err(python_error_to_handler_error(
                            py_err_context(label, message_id),
                            err,
                        )),
                    },
                    Err(err) => Err(python_error_to_handler_error(
                        py_err_context(label, message_id),
                        err,
                    )),
                }
            })
            .collect();
        gc_tick(py, processed);
        results
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
        // RetryableError requests a retry; every other exception (including
        // NonRetryableError) is non-retryable.
        if err.is_instance_of::<RetryableError>(py) {
            HandlerError::Retryable(message)
        } else {
            HandlerError::NonRetryable(message)
        }
    })
}

fn python_result_to_handled(
    obj: &Bound<'_, PyAny>,
    message_id: u128,
    inherited_metadata: HashMap<String, String>,
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
        message.metadata = inherited_metadata;
        message.set_id(message_id);
        return Ok(Handled::Publish(message));
    }

    if let Ok(text) = obj.extract::<String>() {
        let mut message = CanonicalMessage::from(text);
        message.metadata = inherited_metadata;
        message.set_id(message_id);
        return Ok(Handled::Publish(message));
    }

    let mut message = CanonicalMessage::from_vec(python_to_json_bytes(obj)?);
    message.metadata = inherited_metadata;
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

fn install_default_crypto_provider() {
    #[cfg(feature = "rustls-aws-lc")]
    {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
    #[cfg(all(feature = "rustls-ring", not(feature = "rustls-aws-lc")))]
    {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

fn run_sync_task<F, T>(runtime: &Runtime, future: F) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    runtime.block_on(future)
}

fn python_handler_semaphore() -> Option<Arc<Semaphore>> {
    PYTHON_HANDLER_CONCURRENCY
        .get_or_init(|| {
            python_handler_concurrency_limit().map(|limit| Arc::new(Semaphore::new(limit)))
        })
        .clone()
}

fn python_handler_concurrency_limit() -> Option<usize> {
    if let Ok(value) = std::env::var("MQ_BRIDGE_PY_HANDLER_CONCURRENCY") {
        if value.trim() == "0" {
            return None;
        }
        if let Ok(limit) = value.parse::<usize>() {
            return Some(limit.max(1));
        }
    }

    Some(
        std::thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(4)
            .max(1),
    )
}

fn active_route_names() -> &'static Mutex<HashSet<String>> {
    ACTIVE_ROUTE_NAMES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn lock_active_route_names() -> PyResult<std::sync::MutexGuard<'static, HashSet<String>>> {
    active_route_names()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("Active route name lock poisoned"))
}

/// Clear a route's running state and release its reserved name once it has
/// stopped. Called from whichever thread drives the route to completion.
fn finish_run(run_state: &Arc<Mutex<RouteRunState>>, name: &str) {
    if let Ok(mut state) = run_state.lock() {
        state.running = false;
        state.stop_tx = None;
    }
    if let Ok(mut active_route_names) = active_route_names().lock() {
        active_route_names.remove(name);
    }
}

/// Return the JSON Schema for the route/config mapping, generated on demand
/// from the compiled Rust models (no checked-in copy, so it cannot drift).
#[pyfunction]
fn config_schema(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let schema = schemars::schema_for!(core::models::Config);
    let json = serde_json::to_vec(&schema)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to serialize schema: {err}")))?;
    json_bytes_to_python(py, &json)
}

#[pymodule(gil_used = true)]
#[pyo3(name = "_mq_bridge")]
fn _mq_bridge(module: &Bound<'_, PyModule>) -> PyResult<()> {
    install_default_crypto_provider();
    module.add_class::<Message>()?;
    module.add_class::<Route>()?;
    module.add_class::<Publisher>()?;
    module.add_class::<Consumer>()?;
    module.add_class::<MemoryDrainer>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_function(wrap_pyfunction!(config_schema, module)?)?;
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

        let route = Python::attach(|py| Route::from_file(py, &path, Some("my_route"))).unwrap();
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

        let route =
            Python::attach(|py| Route::from_file(py, &path, Some("section_route"))).unwrap();
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

        let route = Python::attach(|py| Route::from_file(py, &path, Some("orders_route"))).unwrap();
        assert_eq!(route.name, "orders_route");
    }

    #[test]
    fn test_route_from_yaml_without_name_parses_single_route() {
        let path = write_yaml(
            r#"
input:
  memory: { topic: "nameless-in", capacity: 8 }
output:
  memory: { topic: "nameless-out", capacity: 8 }
"#,
        );

        let route = Python::attach(|py| Route::from_file(py, &path, None)).unwrap();
        assert!(route.name.starts_with("route-"));
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

        let _publisher =
            Python::attach(|py| Publisher::from_file(py, &path, Some("echo"))).unwrap();
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
            Python::attach(|py| Publisher::from_file(py, &path, Some("orders_publisher"))).unwrap();
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
            common::load_document_from_value(common::load_config_value(Path::new(&path)).unwrap())
                .unwrap();
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
                python_result_to_handled(none_value.bind(py), 7, HashMap::new()).unwrap(),
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
                HashMap::from([("source".to_string(), "input".to_string())]),
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
                HashMap::from([
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
    fn test_config_schema_is_always_available() {
        Python::attach(|py| {
            let schema = config_schema(py).unwrap();
            let schema = schema.bind(py).cast::<PyDict>().unwrap();

            assert_eq!(
                schema
                    .get_item("type")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "object"
            );
        });
    }

    #[cfg(any(feature = "rustls-aws-lc", feature = "rustls-ring"))]
    #[test]
    fn test_module_init_installs_rustls_provider() {
        Python::attach(|py| {
            let module = PyModule::new(py, "_mq_bridge").unwrap();
            _mq_bridge(&module).unwrap();
        });

        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "Python module init should install the selected rustls CryptoProvider"
        );
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
            Py::new(py, Route::from_file(py, &path, Some("raw_route")).unwrap()).unwrap()
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
            Py::new(
                py,
                Route::from_file(py, &path, Some("typed_route")).unwrap(),
            )
            .unwrap()
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
    async fn test_direct_python_handler_dispatches_json() {
        let callable = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                pyo3::ffi::c_str!("def handle(data):\n    return {'seen': data['value']}\n"),
                pyo3::ffi::c_str!("direct_handler.py"),
                pyo3::ffi::c_str!("direct_handler"),
            )
            .unwrap();
            module.getattr("handle").unwrap().unbind()
        });
        let handler = PythonHandler::with_executor_mode(
            "direct:test",
            PythonHandlerMode::Json,
            callable,
            PythonHandlerExecutorMode::Direct,
        );

        let message = CanonicalMessage::from_json(json!({ "value": 34 })).unwrap();
        match handler.handle(message).await.unwrap() {
            Handled::Publish(message) => {
                let parsed: JsonValue = message.parse().unwrap();
                assert_eq!(parsed, json!({ "seen": 34 }));
            }
            Handled::Ack => panic!("expected publish"),
        }
    }

    #[test]
    fn test_python_handler_executor_mode_parser() {
        assert_eq!(
            parse_python_handler_executor_mode(None),
            PythonHandlerExecutorMode::Worker
        );
        assert_eq!(
            parse_python_handler_executor_mode(Some("")),
            PythonHandlerExecutorMode::Worker
        );
        assert_eq!(
            parse_python_handler_executor_mode(Some("worker")),
            PythonHandlerExecutorMode::Worker
        );
        assert_eq!(
            parse_python_handler_executor_mode(Some("DIRECT")),
            PythonHandlerExecutorMode::Direct
        );
        assert_eq!(
            parse_python_handler_executor_mode(Some("unknown")),
            PythonHandlerExecutorMode::Worker
        );
    }

    #[test]
    fn test_python_handler_executor_mode_constructs_direct() {
        let callable = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                pyo3::ffi::c_str!("def handle(data):\n    return data\n"),
                pyo3::ffi::c_str!("executor_mode_handler.py"),
                pyo3::ffi::c_str!("executor_mode_handler"),
            )
            .unwrap();
            module.getattr("handle").unwrap().unbind()
        });
        let handler = PythonHandler::with_executor_mode(
            "executor-mode:test",
            PythonHandlerMode::Json,
            callable,
            PythonHandlerExecutorMode::Direct,
        );

        assert!(matches!(handler.executor, PythonHandlerExecutor::Direct(_)));
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

        let publisher = Python::attach(|py| Publisher::from_file(py, &path, Some("echo"))).unwrap();
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

            assert_eq!(response.payload.as_ref(), b"world");
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

        let publisher = Python::attach(|py| Publisher::from_file(py, &path, Some("echo"))).unwrap();
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

        let route = Arc::new(
            Python::attach(|py| Route::from_file(py, &path, Some("stoppable_route"))).unwrap(),
        );
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

        let first = Arc::new(
            Python::attach(|py| Route::from_file(py, &path, Some("shared_route"))).unwrap(),
        );
        let second =
            Python::attach(|py| Route::from_file(py, &path, Some("shared_route"))).unwrap();

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
