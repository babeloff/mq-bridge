from types import TracebackType
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Type, Union

__version__: str

def config_schema() -> Dict[str, Any]:
    """Return the route-config JSON Schema, generated from the Rust models."""
    ...

JsonValue = Any
HandlerResult = Optional[Union["Message", bytes, str, Dict[str, JsonValue], List[JsonValue], int, float, bool]]


class RetryableError(Exception): ...


class NonRetryableError(Exception): ...


class Message:
    def __init__(
        self,
        payload: bytes,
        metadata: Optional[Mapping[str, str]] = ...,
        id: Optional[str] = ...,
    ) -> None: ...

    @classmethod
    def from_json(
        cls,
        data: JsonValue,
        metadata: Optional[Mapping[str, str]] = ...,
        id: Optional[str] = ...,
    ) -> "Message": ...

    @property
    def payload(self) -> bytes: ...

    @property
    def metadata(self) -> Dict[str, str]: ...

    @property
    def id(self) -> Optional[str]: ...

    def json(self) -> JsonValue: ...

    def text(self) -> str: ...

    def with_json(self, data: JsonValue) -> "Message": ...

    def with_payload(self, payload: bytes) -> "Message": ...


class Route:
    @classmethod
    def from_file(cls, path: str, name: Optional[str] = ...) -> "Route": ...

    @classmethod
    def from_str(cls, text: str, name: Optional[str] = ...) -> "Route": ...

    @classmethod
    def from_config(
        cls, config: Mapping[str, Any], name: Optional[str] = ...
    ) -> "Route": ...

    # Deprecated: use from_file (emits DeprecationWarning at runtime).
    @classmethod
    def from_yaml(cls, path: str, name: Optional[str] = ...) -> "Route": ...

    # Deprecated: use from_str (emits DeprecationWarning at runtime).
    @classmethod
    def from_yaml_str(cls, text: str, name: Optional[str] = ...) -> "Route": ...

    def with_handler(self, handler: Callable[[Message], HandlerResult]) -> "Route": ...

    def add_handler(
        self,
        kind: str,
        handler: Callable[[JsonValue], HandlerResult],
    ) -> "Route": ...

    def run(self) -> None:
        """Deploy and block the calling thread until ``stop()`` is called."""
        ...

    def start(self) -> None:
        """Deploy on a background thread and return immediately."""
        ...

    def join(self) -> None:
        """Block until a route started with ``start()`` has stopped."""
        ...

    def stop(self) -> None: ...

    def __enter__(self) -> "Route": ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool: ...


class Consumer:
    @classmethod
    def from_file(cls, path: str, name: Optional[str] = ...) -> "Consumer": ...

    @classmethod
    def from_str(cls, text: str, name: Optional[str] = ...) -> "Consumer": ...

    @classmethod
    def from_config(
        cls, config: Mapping[str, Any], name: Optional[str] = ...
    ) -> "Consumer": ...

    def poll(
        self,
        max: int = ...,
        timeout_ms: Optional[int] = ...,
    ) -> List["Message"]:
        """Receive up to ``max`` messages without acking. Empty list once
        ``timeout_ms`` milliseconds elapse with nothing received, or when the
        source is exhausted; omit ``timeout_ms`` to block until a message
        arrives. The returned messages are acked by the next ``commit()`` call —
        you must call it (see ``commit``)."""
        ...

    def poll_batch(
        self,
        max: int = ...,
        timeout_ms: Optional[int] = ...,
    ) -> Tuple[List["Message"], Optional[int]]:
        """Like ``poll()``, but also return the batch's token so it can be acked
        or nacked individually with ``ack(token)`` / ``nack(token)`` — the shape a
        ``dlt`` resource wants (``poll → yield → commit load package →
        ack(token)``). Returns ``(messages, token)``, or ``([], None)`` on timeout
        or end-of-stream. Tokens stay outstanding until acked/nacked; ``commit()``
        still acks every outstanding batch at once, so don't mix the two styles on
        one consumer."""
        ...

    def ack(self, token: int) -> None:
        """Ack a single batch by the token from ``poll_batch()``, advancing the
        consumer offset for just that batch. Raises if the token is unknown
        (already acked/nacked, or never polled)."""
        ...

    def nack(self, token: Optional[int] = ...) -> None:
        """Negatively acknowledge so the broker can redeliver. With a ``token``,
        nacks just that batch; without one, nacks every outstanding batch (oldest
        first). On Kafka there is no per-message nack — this leaves the offset
        unadvanced, so redelivery happens on the next run/rebalance, not at
        once."""
        ...

    def commit(self) -> None:
        """Ack every batch returned by ``poll()`` since the last ``commit()``,
        advancing the consumer offset.

        Calling this is required, not optional. Without it the offset never
        advances (messages are re-delivered on the next run), most brokers stall
        once their unacknowledged/prefetch window fills, and uncommitted batches
        are held in memory so the process grows unbounded. To retry a failed
        batch, simply don't commit it — it will be redelivered."""
        ...

    def status(self) -> Dict[str, Any]:
        """Status snapshot for the underlying endpoint: ``healthy``, ``target``,
        optional ``pending`` (broker backlog/lag where reported — Kafka offset
        lag, AMQP queue depth, NATS JetStream ``num_pending``), optional
        ``capacity``/``error``, and ``details``. ``pending == 0`` is a precise
        "caught up" signal; ``None`` where the broker exposes no backlog."""
        ...

    def close(self) -> None:
        """Release the broker connection. Idempotent; ``poll()``/``status()``
        raise afterwards. The context-manager form calls this on exit."""
        ...

    @property
    def exhausted(self) -> bool: ...

    def __enter__(self) -> "Consumer": ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool: ...


class MemoryDrainer:
    @classmethod
    def from_topic(
        cls,
        topic: str,
        capacity: int = ...,
    ) -> "MemoryDrainer": ...

    def drain(
        self,
        count: int,
        timeout: Optional[float] = ...,
        batch_size: int = ...,
    ) -> int: ...


class Publisher:
    @classmethod
    def from_file(cls, path: str, name: Optional[str] = ...) -> "Publisher": ...

    @classmethod
    def from_str(cls, text: str, name: Optional[str] = ...) -> "Publisher": ...

    @classmethod
    def from_config(
        cls, config: Mapping[str, Any], name: Optional[str] = ...
    ) -> "Publisher": ...

    # Deprecated: use from_file (emits DeprecationWarning at runtime).
    @classmethod
    def from_yaml(cls, path: str, name: Optional[str] = ...) -> "Publisher": ...

    # Deprecated: use from_str (emits DeprecationWarning at runtime).
    @classmethod
    def from_yaml_str(cls, text: str, name: Optional[str] = ...) -> "Publisher": ...

    def send(
        self,
        message: Union[Message, bytes],
        metadata: Optional[Mapping[str, str]] = ...,
    ) -> None: ...

    def request(
        self,
        message: Union[Message, bytes],
        metadata: Optional[Mapping[str, str]] = ...,
    ) -> Message: ...

    def send_json(
        self,
        data: JsonValue,
        metadata: Optional[Mapping[str, str]] = ...,
        id: Optional[str] = ...,
    ) -> None: ...

    def request_json(
        self,
        data: JsonValue,
        metadata: Optional[Mapping[str, str]] = ...,
        id: Optional[str] = ...,
    ) -> Message: ...
