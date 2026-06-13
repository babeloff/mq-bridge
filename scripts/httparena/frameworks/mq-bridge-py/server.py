"""HttpArena core server for mq-bridge-py (Python).

Serves the cleartext HTTP/1.1 + HTTP/2 (h2c) profiles on ``0.0.0.0:8080`` via a
single catch-all ``http -> response`` route. mq-bridge keeps all HTTP framing in
Rust (hyper-util's auto connection builder negotiates HTTP/1.1 and h2 prior
knowledge on the plaintext port), and the inline-response fast path keeps the
response on the Rust side; the Python handler runs only the per-request dispatch.

Endpoints (HttpArena reference contract)
----------------------------------------
* ``GET  /pipeline``                    -> ``ok``             (baseline/pipelined/limited-conn)
* ``GET  /baseline11?a=&b=``            -> ``a+b``            (baseline)
* ``POST /baseline11?a=&b=`` + body int -> ``a+b+body``
* ``GET  /baseline2?a=&b=``             -> ``a+b``
* ``GET  /json/{count}?m=``             -> processed dataset JSON  (json/json-comp)
* ``POST /upload`` + body               -> received byte count     (upload)
* ``GET  /async-db?min=&max=&limit=``   -> Postgres ``items`` rows  (async-db)
* ``GET  /static/{file}``               -> file from /data/static   (static)

Harness inputs: dataset from ``/data/dataset.json`` (``DATASET_PATH`` overrides),
static assets from ``/data/static`` (``STATIC_DIR``), Postgres from
``DATABASE_URL``. A missing DB / driver is non-fatal: ``/async-db`` then returns
an empty result so the cleartext profiles still run.

``json-comp`` is handled by mq-bridge's response compression
(``compression_enabled``): bodies over the threshold are gzip-encoded when the
client advertises ``Accept-Encoding: gzip``, identity otherwise — so the same
``/json`` handler serves both ``json`` and ``json-comp``.
"""

from __future__ import annotations

import json as _json
import os
import tempfile
from pathlib import Path
from urllib.parse import parse_qs

from mq_bridge import Message, Route

LISTEN = os.environ.get("MQB_LISTEN", "0.0.0.0:8080")
DATASET_PATH = os.environ.get("DATASET_PATH", "/data/dataset.json")
STATIC_DIR = Path(os.environ.get("STATIC_DIR", "/data/static")).resolve()

SERVER = "mq-bridge-py"
JSON_META = {"content-type": "application/json", "Server": SERVER}
TEXT_META = {"content-type": "text/plain", "Server": SERVER}
NOT_FOUND_META = {"content-type": "text/plain", "Server": SERVER, "http_status_code": "404"}

CONFIG = f"""
routes:
  httparena:
    concurrency: 1
    batch_size: 512
    input:
      http:
        url: "{LISTEN}"
        concurrency_limit: 65536
        internal_buffer_size: 16384
        inline_response_fast_path: true
        compression_enabled: true
        compression_threshold_bytes: 256
    output:
      response: {{}}
"""

CONTENT_TYPES = {
    "js": "application/javascript",
    "css": "text/css",
    "html": "text/html",
    "json": "application/json",
    "woff2": "font/woff2",
    "png": "image/png",
    "svg": "image/svg+xml",
}


def _load_dataset() -> list[dict]:
    try:
        with open(DATASET_PATH, "rb") as f:
            data = _json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []


DATASET = _load_dataset()


# ---------- optional Postgres (async-db) ----------

_POOL = None


def _init_pool():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        return None
    try:
        from psycopg_pool import ConnectionPool
    except ImportError:
        return None
    max_conn = int(os.environ.get("DATABASE_MAX_CONN", "256"))
    try:
        pool = ConnectionPool(url, min_size=1, max_size=max_conn, open=True)
        return pool
    except Exception as exc:  # noqa: BLE001 - non-fatal, /async-db degrades to empty
        print(f"Postgres connection failed ({exc}); /async-db returns empty")
        return None


def _query_int(qs: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int(qs[key][0])
    except (KeyError, IndexError, ValueError):
        return default


# ---------- handlers ----------

def _build_json(count: int, m: int) -> bytes:
    count = min(count, len(DATASET))
    items = []
    for d in DATASET[:count]:
        items.append(
            {
                "id": d["id"],
                "name": d["name"],
                "category": d["category"],
                "price": d["price"],
                "quantity": d["quantity"],
                "active": d["active"],
                "tags": d["tags"],
                "rating": {"score": d["rating"]["score"], "count": d["rating"]["count"]},
                "total": d["price"] * d["quantity"] * m,
            }
        )
    return _json.dumps({"items": items, "count": count}, separators=(",", ":")).encode()


def _async_db(qs: dict[str, list[str]]) -> bytes:
    if _POOL is None:
        return b'{"items":[],"count":0}'
    min_p = _query_int(qs, "min", 10)
    max_p = _query_int(qs, "max", 50)
    limit = max(1, min(_query_int(qs, "limit", 50), 50))
    try:
        with _POOL.connection() as conn:
            cur = conn.execute(
                "SELECT id, name, category, price, quantity, active, tags, "
                "rating_score, rating_count FROM items WHERE price BETWEEN %s AND %s LIMIT %s",
                (min_p, max_p, limit),
            )
            rows = cur.fetchall()
    except Exception:  # noqa: BLE001 - degrade to empty result
        return b'{"items":[],"count":0}'
    items = [
        {
            "id": r[0],
            "name": r[1],
            "category": r[2],
            "price": r[3],
            "quantity": r[4],
            "active": r[5],
            "tags": r[6],
            "rating": {"score": r[7], "count": r[8]},
        }
        for r in rows
    ]
    return _json.dumps({"count": len(items), "items": items}, separators=(",", ":")).encode()


def _content_type_for(name: str) -> str:
    ext = name.rsplit(".", 1)[-1] if "." in name else ""
    return CONTENT_TYPES.get(ext, "application/octet-stream")


def _serve_static(name: str) -> Message:
    # Reject path traversal: the name must be a single normal path component.
    if not name or "/" in name or name in (".", ".."):
        return Message(b"Not Found", NOT_FOUND_META)
    target = (STATIC_DIR / name).resolve()
    if STATIC_DIR not in target.parents and target != STATIC_DIR:
        return Message(b"Not Found", NOT_FOUND_META)
    try:
        body = target.read_bytes()
    except OSError:
        return Message(b"Not Found", NOT_FOUND_META)
    return Message(body, {"content-type": _content_type_for(name), "Server": SERVER})


def handle(message: Message) -> Message:
    method = message.metadata.get("http_method", "")
    path = message.metadata.get("http_path", "")
    qs = parse_qs(message.metadata.get("http_query", ""))

    if method == "GET" and path == "/pipeline":
        return Message(b"ok", TEXT_META)
    if method == "GET" and path in ("/baseline11", "/baseline2"):
        total = _query_int(qs, "a", 0) + _query_int(qs, "b", 0)
        return Message(str(total).encode(), TEXT_META)
    if method == "POST" and path == "/baseline11":
        total = _query_int(qs, "a", 0) + _query_int(qs, "b", 0)
        try:
            total += int(bytes(message.payload).decode().strip())
        except (ValueError, UnicodeDecodeError):
            pass
        return Message(str(total).encode(), TEXT_META)
    if method == "POST" and path == "/upload":
        return Message(str(len(message.payload)).encode(), TEXT_META)
    if method == "GET" and path == "/async-db":
        return Message(_async_db(qs), JSON_META)
    if method == "GET" and path.startswith("/json/"):
        try:
            count = int(path[len("/json/"):])
        except ValueError:
            count = 0
        return Message(_build_json(count, _query_int(qs, "m", 1)), JSON_META)
    if method == "GET" and path.startswith("/static/"):
        return _serve_static(path[len("/static/"):])
    return Message(b"Not Found", NOT_FOUND_META)


def main() -> None:
    global _POOL
    _POOL = _init_pool()
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(CONFIG)
        config_path = f.name
    route = Route.from_yaml(config_path, "httparena").with_handler(handle)
    route.run()


if __name__ == "__main__":
    main()
