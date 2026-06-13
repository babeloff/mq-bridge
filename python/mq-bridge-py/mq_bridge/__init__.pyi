from typing import Any, Callable, Dict, List, Mapping, Optional, Union

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
    def from_yaml(cls, path: str, name: str) -> "Route": ...

    def with_handler(self, handler: Callable[[Message], HandlerResult]) -> "Route": ...

    def add_handler(
        self,
        kind: str,
        handler: Callable[[JsonValue], HandlerResult],
    ) -> "Route": ...

    def run(self) -> None: ...

    def stop(self) -> None: ...


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
    def from_yaml(cls, path: str, name: str) -> "Publisher": ...

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
