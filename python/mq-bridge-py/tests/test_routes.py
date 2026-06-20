"""Brokerless end-to-end tests for the route/publisher runtime surface.

These exercise the actual compiled extension (not just internal Rust units)
over in-memory endpoints, so no Docker services are required.
"""

import threading
import time
import uuid

import pytest

from mq_bridge import MemoryDrainer, Publisher, Route


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex}"


def _transform_config(in_topic: str, out_topic: str) -> dict:
    return {
        "routes": {
            "transform_route": {
                "batch_size": 64,
                "input": {"memory": {"topic": in_topic, "capacity": 4096}},
                "output": {"memory": {"topic": out_topic, "capacity": 4096}},
            }
        },
        "publishers": {
            "pub": {"memory": {"topic": in_topic, "capacity": 4096}}
        },
    }


@pytest.mark.parametrize("executor", ["worker", "direct"])
def test_from_config_round_trip(monkeypatch: pytest.MonkeyPatch, executor: str) -> None:
    monkeypatch.setenv("MQ_BRIDGE_PY_HANDLER_EXECUTOR", executor)

    in_topic = _unique("pytest.in")
    out_topic = _unique("pytest.out")
    config = _transform_config(in_topic, out_topic)

    route = Route.from_config(config, "transform_route")
    publisher = Publisher.from_config(config, "pub")
    drainer = MemoryDrainer.from_topic(out_topic, 4096)

    def transform(data):
        data["value"] += 1
        return data

    route.add_handler("bench.tick", transform)
    route.start()
    try:
        for value in range(50):
            publisher.send_json({"value": value}, {"kind": "bench.tick"})
        drained = drainer.drain(50, timeout=10.0)
    finally:
        route.stop()
        route.join()

    assert drained == 50


def test_context_manager_starts_and_stops() -> None:
    in_topic = _unique("pytest.cm.in")
    out_topic = _unique("pytest.cm.out")
    config = _transform_config(in_topic, out_topic)

    route = Route.from_config(config, "transform_route").add_handler(
        "bench.tick", lambda data: data
    )
    publisher = Publisher.from_config(config, "pub")
    drainer = MemoryDrainer.from_topic(out_topic, 4096)

    with route:
        for value in range(10):
            publisher.send_json({"value": value}, {"kind": "bench.tick"})
        assert drainer.drain(10, timeout=10.0) == 10


def test_start_rejects_double_start() -> None:
    in_topic = _unique("pytest.dup.in")
    out_topic = _unique("pytest.dup.out")
    route = Route.from_config(_transform_config(in_topic, out_topic), "transform_route")
    route.add_handler("bench.tick", lambda data: data)

    route.start()
    try:
        with pytest.raises(RuntimeError):
            route.start()
    finally:
        route.stop()
        route.join()


def test_run_blocks_until_stop() -> None:
    """run() must not return until another thread calls stop() — this is the
    blocking contract that start() exists to avoid."""
    in_topic = _unique("pytest.run.in")
    out_topic = _unique("pytest.run.out")
    route = Route.from_config(_transform_config(in_topic, out_topic), "transform_route")
    route.add_handler("bench.tick", lambda data: data)

    returned = threading.Event()

    def run() -> None:
        route.run()
        returned.set()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    # Give run() a moment; it should still be blocking.
    time.sleep(0.3)
    assert not returned.is_set()

    route.stop()
    assert returned.wait(timeout=5.0)
    thread.join(timeout=5.0)


def test_publisher_request_json_echoes_via_config() -> None:
    publisher = Publisher.from_config({"response": {}}, "echo")

    reply = publisher.request_json({"order_id": 7, "status": "ok"})

    assert reply.json() == {"order_id": 7, "status": "ok"}
    assert reply.id is not None


def test_publisher_from_yaml_str_echoes() -> None:
    publisher = Publisher.from_yaml_str(
        """
        publishers:
          echo:
            response: {}
        """,
        "echo",
    )

    reply = publisher.request(b"ping")

    assert reply.payload == b"ping"
