import os
import threading
import time

from mq_bridge import Message, Publisher, Route


def handle_order(message: Message) -> dict:
    print("id:", message.id)
    print("metadata:", message.metadata)
    print("payload:", message.payload)
    return {"handled": True}


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "examples",
    "memory.yaml",
)

def drive() -> None:
    global publisher, route
    time.sleep(0.2)
    publisher.send_json(
        {"order_id": 42, "status": "created"},
        {"kind": "order.created"},
    )
    time.sleep(0.2)
    route.stop()

def main() -> None:
    global publisher, route
    route = Route.from_yaml(CONFIG_PATH, "orders_route").with_handler(handle_order)
    publisher = Publisher.from_yaml(CONFIG_PATH, "orders_publisher")
    threading.Thread(target=drive, daemon=True).start()
    route.run()


if __name__ == "__main__":
    main()
