window.BENCHMARK_DATA = {
  "lastUpdate": 1788530581691,
  "repoUrl": "https://github.com/babeloff/mq-bridge",
  "entries": {
    "Rust Benchmark": [
      {
        "commit": {
          "author": {
            "email": "62469331+marcomq@users.noreply.github.com",
            "name": "Marco Mengelkoch",
            "username": "marcomq"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "59dda56abd07c9771b498a6f85bd6fc65e6cba7d",
          "message": "Merge pull request #105 from marcomq/dev\n\nAdd 3rd party license files",
          "timestamp": "2026-09-04T09:47:02+02:00",
          "tree_id": "edfeb96289964572122d343063af8a1f171213f5",
          "url": "https://github.com/babeloff/mq-bridge/commit/59dda56abd07c9771b498a6f85bd6fc65e6cba7d"
        },
        "date": 1788530580922,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 136153182,
            "range": "± 22246916",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 405205510,
            "range": "± 18557286",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 7614817,
            "range": "± 1083501",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 39163451,
            "range": "± 1845606",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 85882288,
            "range": "± 19451577",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 947492967,
            "range": "± 282438374",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 13578789,
            "range": "± 8633389",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 24440591,
            "range": "± 5056371",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3655681,
            "range": "± 48330",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1823422,
            "range": "± 43781",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 276175,
            "range": "± 24714",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 617184,
            "range": "± 24921",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 405386,
            "range": "± 12101",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1479814,
            "range": "± 22020",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 153859,
            "range": "± 4060",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 33757,
            "range": "± 1412",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 490138,
            "range": "± 23732",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1817409,
            "range": "± 27288",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 224960,
            "range": "± 9096",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 165712,
            "range": "± 7972",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 132783850,
            "range": "± 4056463",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1563399,
            "range": "± 182088",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1668164,
            "range": "± 60195",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 145011,
            "range": "± 18472",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 131988493,
            "range": "± 3721156",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2213710,
            "range": "± 171689",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1731297,
            "range": "± 56139",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 51501,
            "range": "± 7280",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 14749719,
            "range": "± 453898",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14388295,
            "range": "± 289460",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5761256,
            "range": "± 127564",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7445285,
            "range": "± 1376313",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 615569,
            "range": "± 85136",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5655006,
            "range": "± 2225794",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 89039776,
            "range": "± 9015087",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3232291,
            "range": "± 331748",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43366542,
            "range": "± 2993110",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1326972,
            "range": "± 240488",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 88677373,
            "range": "± 13029164",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1059227247,
            "range": "± 170311452",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1228662801,
            "range": "± 41557550",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2760841101,
            "range": "± 26264308",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 179827930,
            "range": "± 8981438",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 312349613,
            "range": "± 10613212",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 65666961,
            "range": "± 70509356",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5917656,
            "range": "± 19203189",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 6843033,
            "range": "± 1737962",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4552240,
            "range": "± 1070146",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 23348263,
            "range": "± 1646445",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 18663719,
            "range": "± 3772678",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 19321707,
            "range": "± 1039911",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 18052046,
            "range": "± 2289312",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 17548682,
            "range": "± 6269422",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 9001593,
            "range": "± 730251",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 14321005,
            "range": "± 31760149",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 8094194,
            "range": "± 272206",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 71327986,
            "range": "± 2991432",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2192340,
            "range": "± 179319",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 70118858,
            "range": "± 5213410",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 326344,
            "range": "± 21985",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}