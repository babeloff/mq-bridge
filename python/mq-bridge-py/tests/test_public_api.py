from mq_bridge import (
    MemoryDrainer,
    Message,
    NonRetryableError,
    Publisher,
    RetryableError,
    Route,
)


def test_public_exports_are_available() -> None:
    assert MemoryDrainer is not None
    assert Message is not None
    assert NonRetryableError is not None
    assert Route is not None
    assert Publisher is not None
    assert RetryableError is not None


def test_message_from_json_round_trip_shape() -> None:
    message = Message.from_json(
        {"hello": "world"},
        {"kind": "demo.created"},
        "018f4aa3-95b5-7cc2-b6d4-a4d82b8cf7a1",
    )

    assert message.payload.startswith(b"{")
    assert message.json() == {"hello": "world"}
    assert message.metadata == {"kind": "demo.created"}
    assert message.id == "018f4aa3-95b5-7cc2-b6d4-a4d82b8cf7a1"


def test_message_text_decodes_utf8_payload() -> None:
    message = Message(b"hello", {"kind": "demo.text"})

    assert message.text() == "hello"


def test_message_response_helpers_preserve_metadata_and_id() -> None:
    message = Message(
        b"old",
        {"kind": "demo.reply"},
        "018f4aa3-95b5-7cc2-b6d4-a4d82b8cf7a1",
    )

    json_reply = message.with_json({"ok": True})
    payload_reply = message.with_payload(b"new")

    assert json_reply.json() == {"ok": True}
    assert json_reply.metadata == message.metadata
    assert json_reply.id == message.id
    assert payload_reply.payload == b"new"
    assert payload_reply.metadata == message.metadata
    assert payload_reply.id == message.id
