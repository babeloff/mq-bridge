window.BENCHMARK_DATA = {
  "lastUpdate": 1789030212652,
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
      },
      {
        "commit": {
          "author": {
            "name": "Marco Mengelkoch",
            "username": "marcomq",
            "email": "62469331+marcomq@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "59dda56abd07c9771b498a6f85bd6fc65e6cba7d",
          "message": "Merge pull request #105 from marcomq/dev\n\nAdd 3rd party license files",
          "timestamp": "2026-09-04T07:47:02Z",
          "url": "https://github.com/babeloff/mq-bridge/commit/59dda56abd07c9771b498a6f85bd6fc65e6cba7d"
        },
        "date": 1788595978171,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 180413048,
            "range": "± 27822463",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 502148024,
            "range": "± 18503768",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10838719,
            "range": "± 1571105",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 56724174,
            "range": "± 3338714",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 72748204,
            "range": "± 664979",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 779466245,
            "range": "± 14379071",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8786975,
            "range": "± 3805138",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29299940,
            "range": "± 4773282",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3839625,
            "range": "± 51142",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1753879,
            "range": "± 74164",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 243101,
            "range": "± 286965",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 534873,
            "range": "± 10152",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 363620,
            "range": "± 7910",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1404949,
            "range": "± 56595",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 139693,
            "range": "± 5964",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 41467,
            "range": "± 1871",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 432497,
            "range": "± 10978",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1726981,
            "range": "± 48373",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 180445,
            "range": "± 14957",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 151543,
            "range": "± 4986",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 103574546,
            "range": "± 1125156",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1436102,
            "range": "± 66103",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1367140,
            "range": "± 69306",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 163782,
            "range": "± 18701",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 102498026,
            "range": "± 956400",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1837743,
            "range": "± 30621",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1357622,
            "range": "± 31538",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 60476,
            "range": "± 11379",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 13229960,
            "range": "± 413941",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 12774715,
            "range": "± 239972",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5873105,
            "range": "± 253867",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 5243578,
            "range": "± 2067784",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 482146,
            "range": "± 30444",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5238458,
            "range": "± 3323832",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 80977299,
            "range": "± 11887095",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 2904107,
            "range": "± 293227",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43468650,
            "range": "± 3828384",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1185327,
            "range": "± 149976",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 89286536,
            "range": "± 32148271",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1228784000,
            "range": "± 193821499",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1215950549,
            "range": "± 35864757",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2764484434,
            "range": "± 24596131",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 181021008,
            "range": "± 11492360",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 311302576,
            "range": "± 2909820",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 132368986,
            "range": "± 85320967",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 4838589,
            "range": "± 1699808",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 7980193,
            "range": "± 2102292",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 3851266,
            "range": "± 973600",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 23415790,
            "range": "± 1799164",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 18775694,
            "range": "± 4276604",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 19294115,
            "range": "± 840247",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 17980763,
            "range": "± 2127024",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 16123823,
            "range": "± 10290821",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 9219482,
            "range": "± 307881",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 13847021,
            "range": "± 3676425",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 8288217,
            "range": "± 401649",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 67682146,
            "range": "± 5954058",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2171571,
            "range": "± 110868",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 72090548,
            "range": "± 2966849",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 336918,
            "range": "± 14746",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marco Mengelkoch",
            "username": "marcomq",
            "email": "62469331+marcomq@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "59dda56abd07c9771b498a6f85bd6fc65e6cba7d",
          "message": "Merge pull request #105 from marcomq/dev\n\nAdd 3rd party license files",
          "timestamp": "2026-09-04T07:47:02Z",
          "url": "https://github.com/babeloff/mq-bridge/commit/59dda56abd07c9771b498a6f85bd6fc65e6cba7d"
        },
        "date": 1788683511763,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 178220207,
            "range": "± 9731858",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 532697341,
            "range": "± 31812686",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 9783255,
            "range": "± 1714145",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 53235419,
            "range": "± 4595663",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 95029727,
            "range": "± 1288485",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 887793604,
            "range": "± 13061117",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8422344,
            "range": "± 4022716",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 44270416,
            "range": "± 3734117",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3714945,
            "range": "± 67834",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1840007,
            "range": "± 74325",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 317881,
            "range": "± 10790",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 674010,
            "range": "± 18669",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 407748,
            "range": "± 9370",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1483976,
            "range": "± 21038",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 160133,
            "range": "± 5227",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 32042,
            "range": "± 2448",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 501316,
            "range": "± 13498",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1835535,
            "range": "± 30637",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 231419,
            "range": "± 18694",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 178302,
            "range": "± 6527",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 131660988,
            "range": "± 3662978",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1703847,
            "range": "± 256374",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1692861,
            "range": "± 83700",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 162745,
            "range": "± 21890",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 131890448,
            "range": "± 4331232",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2409371,
            "range": "± 143639",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1769351,
            "range": "± 104060",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 57032,
            "range": "± 8018",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 15124040,
            "range": "± 552977",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14742812,
            "range": "± 524911",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5754883,
            "range": "± 273300",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6220016,
            "range": "± 1694779",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 563553,
            "range": "± 189466",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6191838,
            "range": "± 1557213",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 85876805,
            "range": "± 12225543",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3625927,
            "range": "± 338105",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 41458186,
            "range": "± 6101186",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1480736,
            "range": "± 118600",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 91258676,
            "range": "± 664130136",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1130566460,
            "range": "± 225188834",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1066171341,
            "range": "± 43052720",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2176306651,
            "range": "± 14815859",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 139907551,
            "range": "± 12646695",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 246748733,
            "range": "± 7058071",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 65512521,
            "range": "± 71774053",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 6295151,
            "range": "± 318030258",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 6313330,
            "range": "± 16372148",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 2378145,
            "range": "± 553693",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 18738416,
            "range": "± 3108669",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 15204201,
            "range": "± 2631981",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 15603757,
            "range": "± 2379733",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 14548442,
            "range": "± 954242",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 15009312,
            "range": "± 14517355",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 7205429,
            "range": "± 467506",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 11684022,
            "range": "± 1504955",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 6376207,
            "range": "± 328481",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 61000637,
            "range": "± 12932733",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 1750507,
            "range": "± 104162",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 62835905,
            "range": "± 12682881",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 301744,
            "range": "± 21562",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marco Mengelkoch",
            "username": "marcomq",
            "email": "62469331+marcomq@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "59dda56abd07c9771b498a6f85bd6fc65e6cba7d",
          "message": "Merge pull request #105 from marcomq/dev\n\nAdd 3rd party license files",
          "timestamp": "2026-09-04T07:47:02Z",
          "url": "https://github.com/babeloff/mq-bridge/commit/59dda56abd07c9771b498a6f85bd6fc65e6cba7d"
        },
        "date": 1788771917603,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 138270278,
            "range": "± 23406038",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 445142054,
            "range": "± 68125232",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 8024159,
            "range": "± 551548",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 43695962,
            "range": "± 4408219",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 170515154,
            "range": "± 54228385",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 968928542,
            "range": "± 442658682",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 10575170,
            "range": "± 21558968",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 33631110,
            "range": "± 22223527",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 2991912,
            "range": "± 40740",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1332381,
            "range": "± 105755",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 199433,
            "range": "± 11622",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 424941,
            "range": "± 5645",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 285480,
            "range": "± 4916",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1100163,
            "range": "± 19244",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 113888,
            "range": "± 5151",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 33132,
            "range": "± 4443",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 342957,
            "range": "± 12489",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1361373,
            "range": "± 25255",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 159637,
            "range": "± 23861",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 124654,
            "range": "± 4640",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 83807992,
            "range": "± 911245",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1147707,
            "range": "± 157105",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1113156,
            "range": "± 52628",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 143277,
            "range": "± 14083",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 82928341,
            "range": "± 3218745",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1507409,
            "range": "± 35335",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1141505,
            "range": "± 48713",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 47555,
            "range": "± 7380",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 10182402,
            "range": "± 432809",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 9812170,
            "range": "± 151915",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 4555959,
            "range": "± 190201",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6260018,
            "range": "± 1806082",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 412301,
            "range": "± 35423",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5524374,
            "range": "± 2585371",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 78531739,
            "range": "± 13722648",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 2441788,
            "range": "± 248465",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 42474508,
            "range": "± 12683126",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 968051,
            "range": "± 98740",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 79379001,
            "range": "± 11737952",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 897762857,
            "range": "± 214431029",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1429781751,
            "range": "± 53071323",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2996315050,
            "range": "± 26906246",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 206688662,
            "range": "± 15116029",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 331958690,
            "range": "± 4432402",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 67776609,
            "range": "± 109029768",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5494679,
            "range": "± 2240700",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 8295052,
            "range": "± 3173524",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4127758,
            "range": "± 853120",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 25953714,
            "range": "± 1731052",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 21541622,
            "range": "± 3408908",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 23052534,
            "range": "± 1625751",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 21103369,
            "range": "± 2548076",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 18951896,
            "range": "± 15098715",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10391601,
            "range": "± 485049",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 14220850,
            "range": "± 1323721",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 8934487,
            "range": "± 753817",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 68090312,
            "range": "± 14133639",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2140305,
            "range": "± 188900",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 64010627,
            "range": "± 6922799",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 291941,
            "range": "± 34355",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marco Mengelkoch",
            "username": "marcomq",
            "email": "62469331+marcomq@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "59dda56abd07c9771b498a6f85bd6fc65e6cba7d",
          "message": "Merge pull request #105 from marcomq/dev\n\nAdd 3rd party license files",
          "timestamp": "2026-09-04T07:47:02Z",
          "url": "https://github.com/babeloff/mq-bridge/commit/59dda56abd07c9771b498a6f85bd6fc65e6cba7d"
        },
        "date": 1788857152317,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 186401790,
            "range": "± 40849429",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 523422001,
            "range": "± 18189232",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 9762231,
            "range": "± 1890280",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 50164775,
            "range": "± 4503965",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 96511115,
            "range": "± 2748576",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 910018045,
            "range": "± 9633069",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8644253,
            "range": "± 3705741",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 44442080,
            "range": "± 3702334",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 2795192,
            "range": "± 27116",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 2241656,
            "range": "± 188454",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 290332,
            "range": "± 13411",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 508839,
            "range": "± 12183",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 551043,
            "range": "± 8606",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 2018101,
            "range": "± 33832",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 173201,
            "range": "± 5360",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 41994,
            "range": "± 2346",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 583569,
            "range": "± 23631",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 2456824,
            "range": "± 35021",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 199169,
            "range": "± 18108",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 162924,
            "range": "± 4851",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 117047026,
            "range": "± 3051087",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1783986,
            "range": "± 87880",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1457783,
            "range": "± 74083",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 193185,
            "range": "± 10841",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 118751198,
            "range": "± 3647307",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2490891,
            "range": "± 110988",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1518904,
            "range": "± 86215",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 66360,
            "range": "± 3213",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 11811642,
            "range": "± 349469",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 10849835,
            "range": "± 134343",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5045329,
            "range": "± 210094",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 8409292,
            "range": "± 2282026",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 549751,
            "range": "± 67714",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 4228140,
            "range": "± 2556071",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 89469187,
            "range": "± 12741506",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3699271,
            "range": "± 494311",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43469895,
            "range": "± 7608816",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1237821,
            "range": "± 130845",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 108219777,
            "range": "± 18027835",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1096252868,
            "range": "± 130061060",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1351429300,
            "range": "± 31222744",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2924518642,
            "range": "± 32294925",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 203678214,
            "range": "± 14960163",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 327756962,
            "range": "± 1805751",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 66910386,
            "range": "± 23325994",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5587745,
            "range": "± 28093246",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 8843776,
            "range": "± 16116067",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4474271,
            "range": "± 1266318",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 25389574,
            "range": "± 1261827",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 21281405,
            "range": "± 4594895",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 21850261,
            "range": "± 1561883",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 21495348,
            "range": "± 2615279",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 17906526,
            "range": "± 8955852",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10281347,
            "range": "± 692731",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 13902798,
            "range": "± 1527797",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 8678976,
            "range": "± 404150",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 56824558,
            "range": "± 8722704",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2260343,
            "range": "± 143637",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 65619011,
            "range": "± 13278438",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 334991,
            "range": "± 44320",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "cd177bcacd192a566dbff5089909871b509d272a",
          "message": "Merge pull request #107 from marcomq/dev\n\nImprove performance, add mcp inter connection",
          "timestamp": "2026-09-06T14:15:00+02:00",
          "tree_id": "60898444f48d5a3beae59944f08df9e2dc0ab86f",
          "url": "https://github.com/babeloff/mq-bridge/commit/cd177bcacd192a566dbff5089909871b509d272a"
        },
        "date": 1788892605394,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 218402705,
            "range": "± 42167491",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 569853323,
            "range": "± 21549750",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 11140049,
            "range": "± 1094816",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 60299069,
            "range": "± 7755141",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 108200783,
            "range": "± 832227",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 925902214,
            "range": "± 22323990",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9263302,
            "range": "± 6557617",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29203988,
            "range": "± 3502763",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3953421,
            "range": "± 88053",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1771121,
            "range": "± 34719",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 245222,
            "range": "± 10929",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 563238,
            "range": "± 21148",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 365522,
            "range": "± 12125",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1394226,
            "range": "± 23395",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 142832,
            "range": "± 7018",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 40193,
            "range": "± 11345",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 440515,
            "range": "± 25350",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1738223,
            "range": "± 22540",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 193226,
            "range": "± 46811",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 154662,
            "range": "± 5668",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 106459893,
            "range": "± 1348511",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1470279,
            "range": "± 213703",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1394714,
            "range": "± 55178",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 162585,
            "range": "± 16392",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 105032926,
            "range": "± 7034497",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1945049,
            "range": "± 25453",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1402317,
            "range": "± 38219",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 55732,
            "range": "± 4586",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 13444844,
            "range": "± 432152",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 12816421,
            "range": "± 251710",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 6004076,
            "range": "± 163997",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 8196108,
            "range": "± 1878597",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 516657,
            "range": "± 52505",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6860206,
            "range": "± 2101511",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 87932781,
            "range": "± 11791413",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3046338,
            "range": "± 357404",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 41841255,
            "range": "± 5754576",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1297978,
            "range": "± 91178",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 93215415,
            "range": "± 12537073",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1046122642,
            "range": "± 173734200",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1358940364,
            "range": "± 38365735",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2913995856,
            "range": "± 37152466",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 203051309,
            "range": "± 11119323",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 316606740,
            "range": "± 3708722",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 68345239,
            "range": "± 46469279",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 8423980,
            "range": "± 2914422",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 8154616,
            "range": "± 16576886",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4144420,
            "range": "± 1224503",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 25561705,
            "range": "± 1441192",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 20860260,
            "range": "± 3549011",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 22214409,
            "range": "± 1865277",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 20061331,
            "range": "± 2921011",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 17755980,
            "range": "± 2818518",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10264335,
            "range": "± 703368",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 13500309,
            "range": "± 1192035",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9127876,
            "range": "± 714501",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 57740738,
            "range": "± 13890860",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2272653,
            "range": "± 174146",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 63784292,
            "range": "± 6493948",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 346005,
            "range": "± 36005",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marco Mengelkoch",
            "username": "marcomq",
            "email": "62469331+marcomq@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cd177bcacd192a566dbff5089909871b509d272a",
          "message": "Merge pull request #107 from marcomq/dev\n\nImprove performance, add mcp inter connection",
          "timestamp": "2026-09-06T12:15:00Z",
          "url": "https://github.com/babeloff/mq-bridge/commit/cd177bcacd192a566dbff5089909871b509d272a"
        },
        "date": 1788943625920,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 201828941,
            "range": "± 30560583",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 526011330,
            "range": "± 23277152",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10004728,
            "range": "± 1779389",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 51354206,
            "range": "± 3061595",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 95078623,
            "range": "± 1253413",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 892229593,
            "range": "± 10669121",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8676068,
            "range": "± 3738098",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 44150494,
            "range": "± 7451824",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3598233,
            "range": "± 49842",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1801673,
            "range": "± 51503",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 299753,
            "range": "± 10197",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 651096,
            "range": "± 23414",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 403588,
            "range": "± 13418",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1493397,
            "range": "± 30236",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 154102,
            "range": "± 4573",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 30681,
            "range": "± 2372",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 477564,
            "range": "± 15609",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1767285,
            "range": "± 15936",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 217693,
            "range": "± 12758",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 166544,
            "range": "± 6468",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 117510845,
            "range": "± 7006018",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1709908,
            "range": "± 170078",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1611766,
            "range": "± 94097",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 155463,
            "range": "± 14921",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 119941711,
            "range": "± 4420647",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2397316,
            "range": "± 151602",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1656239,
            "range": "± 81646",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 55734,
            "range": "± 6464",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 14994211,
            "range": "± 530860",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14680625,
            "range": "± 252157",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5869400,
            "range": "± 208412",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6859864,
            "range": "± 2738686",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 589483,
            "range": "± 68252",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6077811,
            "range": "± 2962067",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 89654663,
            "range": "± 10771113",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3493853,
            "range": "± 655415",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43782960,
            "range": "± 5910047",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1466326,
            "range": "± 136837",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 90170211,
            "range": "± 18629168",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1121317272,
            "range": "± 185332584",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1388743718,
            "range": "± 50999706",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2964941402,
            "range": "± 30204262",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 206738048,
            "range": "± 2349665",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 329826448,
            "range": "± 2980290",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 72383131,
            "range": "± 33393780",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 8319938,
            "range": "± 2644501",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 7718433,
            "range": "± 21723890",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4138921,
            "range": "± 919387",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 25343628,
            "range": "± 1637191",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 21227036,
            "range": "± 4161399",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 22805019,
            "range": "± 2024889",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 20264123,
            "range": "± 2402441",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 18955902,
            "range": "± 9369019",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10420730,
            "range": "± 505457",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 13871010,
            "range": "± 1988183",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9372403,
            "range": "± 673297",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 68893006,
            "range": "± 8308719",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2356944,
            "range": "± 199749",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 67498791,
            "range": "± 12537945",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 346558,
            "range": "± 20961",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marco Mengelkoch",
            "username": "marcomq",
            "email": "62469331+marcomq@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cd177bcacd192a566dbff5089909871b509d272a",
          "message": "Merge pull request #107 from marcomq/dev\n\nImprove performance, add mcp inter connection",
          "timestamp": "2026-09-06T12:15:00Z",
          "url": "https://github.com/babeloff/mq-bridge/commit/cd177bcacd192a566dbff5089909871b509d272a"
        },
        "date": 1789030211875,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 202254323,
            "range": "± 29487779",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 579494299,
            "range": "± 49973922",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 11170153,
            "range": "± 1044844",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 63000616,
            "range": "± 3511760",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 115558088,
            "range": "± 3126038",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 975214820,
            "range": "± 65456763",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 10541584,
            "range": "± 3965091",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 47493726,
            "range": "± 4753776",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3685113,
            "range": "± 96834",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1785536,
            "range": "± 25529",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 277037,
            "range": "± 19014",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 630359,
            "range": "± 22795",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 393210,
            "range": "± 15639",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1479563,
            "range": "± 15541",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 147758,
            "range": "± 7618",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 32703,
            "range": "± 1774",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 484961,
            "range": "± 33072",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1779840,
            "range": "± 48640",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 221584,
            "range": "± 22185",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 168100,
            "range": "± 3916",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 125936427,
            "range": "± 2940927",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1628569,
            "range": "± 129043",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1677205,
            "range": "± 106100",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 151971,
            "range": "± 24885",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 126384747,
            "range": "± 3022674",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2286730,
            "range": "± 136738",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1696886,
            "range": "± 109644",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 51737,
            "range": "± 9262",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 15063796,
            "range": "± 564782",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14843241,
            "range": "± 362568",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5740891,
            "range": "± 242971",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6765420,
            "range": "± 2749950",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 606191,
            "range": "± 91201",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 4314024,
            "range": "± 2361568",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 86387986,
            "range": "± 8942701",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3331208,
            "range": "± 285051",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43549939,
            "range": "± 7130809",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1466211,
            "range": "± 73798",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 91202470,
            "range": "± 14083554",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1106218968,
            "range": "± 130944617",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1093816967,
            "range": "± 35104923",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2633910968,
            "range": "± 33408405",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 168032956,
            "range": "± 7194781",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 285774807,
            "range": "± 4156494",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 64540841,
            "range": "± 57564551",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 9057655,
            "range": "± 3655058",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 35464397,
            "range": "± 26792906",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4368018,
            "range": "± 127043716",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 20021745,
            "range": "± 1441681",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 14891406,
            "range": "± 2220082",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 15092881,
            "range": "± 1193898",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 14230671,
            "range": "± 1338845",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 13263397,
            "range": "± 11981715",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 8117969,
            "range": "± 664860",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 12146083,
            "range": "± 4116193",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 6723154,
            "range": "± 465281",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 50466779,
            "range": "± 8391357",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2131577,
            "range": "± 254913",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 55537453,
            "range": "± 5268212",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 378552,
            "range": "± 35819",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}