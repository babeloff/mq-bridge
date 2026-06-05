import threading
import time

from mq_bridge import Publisher, Route


def handle_order(data):
    print("decoded data:", data)
    return {"accepted": True, "order_id": data["order_id"]}


route = Route.from_yaml("examples/memory.yaml", "orders_route").add_handler(
    "order.created",
    handle_order,
)
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
