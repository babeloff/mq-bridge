import threading
import time

from mq_bridge import Message, Publisher, Route


def handle_order(message: Message) -> dict:
    print("id:", message.id)
    print("metadata:", message.metadata)
    print("payload:", message.payload)
    return {"handled": True}


route = Route.from_yaml("examples/memory.yaml", "orders_route").with_handler(handle_order)
publisher = Publisher.from_yaml("examples/memory.yaml", "orders_publisher")


def drive() -> None:
    time.sleep(0.2)
    publisher.send_json(
        {"order_id": 42, "status": "created"},
        {"kind": "order.created"},
    )
    time.sleep(0.2)
    route.stop()


threading.Thread(target=drive, daemon=True).start()
route.run()
