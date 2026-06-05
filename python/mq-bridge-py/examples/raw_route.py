import threading
import time

from mq_bridge import Publisher, Route


def handle_raw(message):
    print("raw payload:", message.payload)
    print("raw metadata:", message.metadata)
    return b"ok"


route = Route.from_yaml("examples/memory.yaml", "orders_route").with_handler(handle_raw)
publisher = Publisher.from_yaml("examples/memory.yaml", "orders_publisher")


def drive() -> None:
    time.sleep(0.2)
    publisher.send(b'{"hello":"world"}', {"kind": "raw.demo"})
    time.sleep(0.2)
    route.stop()


threading.Thread(target=drive, daemon=True).start()
route.run()
