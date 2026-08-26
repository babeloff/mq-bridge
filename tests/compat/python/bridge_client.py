"""Generated-client compatibility probe for mqbridge.Bridge."""

import sys
import threading

import grpc

from mqbridge import bridge_pb2, bridge_pb2_grpc


def main() -> None:
    address = sys.argv[1]
    channel = grpc.insecure_channel(address)
    stub = bridge_pb2_grpc.BridgeStub(channel)
    received = []
    errors = []
    subscribed = threading.Event()

    def subscribe_and_ack() -> None:
        try:
            stream = stub.Subscribe(
                bridge_pb2.SubscribeRequest(topic="compat", consumer_id="python-compat")
            )
            # Response headers arrive only once the server handler has returned, which is
            # after it registered the subscription. Publishing before that would race.
            stream.initial_metadata()
            subscribed.set()
            message = next(stream)
            received.append(message)
            ack = stub.Acknowledge(
                bridge_pb2.Ack(
                    id=message.id,
                    status=bridge_pb2.Ack.ACK,
                    metadata={"mq_bridge.consumer_id": "python-compat"},
                )
            )
            assert ack.success, ack.error
            stream.cancel()
        except BaseException as error:  # Propagate thread failures through main.
            errors.append(error)
        finally:
            subscribed.set()  # Never let a failed subscribe wedge main.

    subscriber = threading.Thread(target=subscribe_and_ack, daemon=True)
    subscriber.start()
    assert subscribed.wait(timeout=10), "Subscribe did not become ready"
    assert not errors, errors

    response = stub.Publish(
        bridge_pb2.BridgeMessage(
            payload=b"python-generated-client",
            id="018f0b4d-2f36-7c20-8000-000000000001",
            metadata={"mq_bridge.topic": "compat"},
        )
    )
    assert response.WhichOneof("result") == "ack"
    assert response.ack.status == bridge_pb2.Ack.ACK
    subscriber.join(timeout=5)
    assert not errors, errors
    assert received and received[0].payload == b"python-generated-client"


if __name__ == "__main__":
    main()
