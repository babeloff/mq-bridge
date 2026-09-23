window.BENCHMARK_DATA = {
  "lastUpdate": 1790154941842,
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
        "date": 1789116154868,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 206106384,
            "range": "± 33962307",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 562909741,
            "range": "± 18604886",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10537572,
            "range": "± 1205454",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 56346349,
            "range": "± 4766011",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 107649639,
            "range": "± 2368572",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 917047531,
            "range": "± 20121206",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9126543,
            "range": "± 3712701",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 27623111,
            "range": "± 3742149",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3735147,
            "range": "± 73247",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1834714,
            "range": "± 49243",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 291417,
            "range": "± 19506",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 643251,
            "range": "± 25133",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 395934,
            "range": "± 9498",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1495789,
            "range": "± 19300",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 151236,
            "range": "± 3026",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 34475,
            "range": "± 2652",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 494529,
            "range": "± 15733",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1845581,
            "range": "± 29659",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 224262,
            "range": "± 25195",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 169245,
            "range": "± 8113",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 133207989,
            "range": "± 2659814",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1643498,
            "range": "± 169957",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1699457,
            "range": "± 79412",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 152795,
            "range": "± 30039",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 131954018,
            "range": "± 2606656",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2196661,
            "range": "± 130794",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1799027,
            "range": "± 45052",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 58925,
            "range": "± 6867",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 15093441,
            "range": "± 590695",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14758597,
            "range": "± 317974",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5683310,
            "range": "± 281426",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7126279,
            "range": "± 2783461",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 571679,
            "range": "± 93592",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6798271,
            "range": "± 2084197",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 88120505,
            "range": "± 9416986",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3353317,
            "range": "± 383825",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43540642,
            "range": "± 13209610",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1445898,
            "range": "± 179287",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 90609505,
            "range": "± 32290720",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1033781439,
            "range": "± 127281089",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1074574076,
            "range": "± 162902737",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 1700436413,
            "range": "± 36295479",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 107688993,
            "range": "± 12072002",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 192478335,
            "range": "± 2445870",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 60812949,
            "range": "± 64540652",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 4787060,
            "range": "± 2113292",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 7510824,
            "range": "± 15581598",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 2082236,
            "range": "± 538399",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 14328982,
            "range": "± 1181758",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 10878648,
            "range": "± 890452",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 11615599,
            "range": "± 798480",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 10523811,
            "range": "± 1166992",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 10644786,
            "range": "± 1790894",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 5663980,
            "range": "± 300328",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 8969874,
            "range": "± 1262635",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 4594909,
            "range": "± 366699",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 56042085,
            "range": "± 4807829",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 1432736,
            "range": "± 106693",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 50802833,
            "range": "± 12540760",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 272835,
            "range": "± 59379",
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
        "date": 1789201992769,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 200272947,
            "range": "± 33954562",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 612526451,
            "range": "± 46123129",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10797730,
            "range": "± 910885",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 59498876,
            "range": "± 3514004",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 112391913,
            "range": "± 3228996",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 942811041,
            "range": "± 18839164",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9472384,
            "range": "± 4138266",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29368379,
            "range": "± 4867226",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3911555,
            "range": "± 126223",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1742315,
            "range": "± 71023",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 242337,
            "range": "± 16460",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 541864,
            "range": "± 7888",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 381714,
            "range": "± 10483",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1406926,
            "range": "± 19286",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 143334,
            "range": "± 7904",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 41134,
            "range": "± 3689",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 443737,
            "range": "± 35144",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1743541,
            "range": "± 12188",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 181149,
            "range": "± 27318",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 154149,
            "range": "± 6700",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 104839172,
            "range": "± 1109756",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1379666,
            "range": "± 69394",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1386652,
            "range": "± 41703",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 142055,
            "range": "± 14117",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 102925837,
            "range": "± 1268450",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1887072,
            "range": "± 41200",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1410723,
            "range": "± 23081",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 53013,
            "range": "± 5492",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 13406052,
            "range": "± 714461",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 12821721,
            "range": "± 386655",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5809059,
            "range": "± 210384",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6891931,
            "range": "± 1509732",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 489636,
            "range": "± 50620",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5076433,
            "range": "± 1790852",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 84614832,
            "range": "± 9577689",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3194638,
            "range": "± 322598",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43229255,
            "range": "± 3397872",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1203577,
            "range": "± 114683",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 90239994,
            "range": "± 12626108",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1036444412,
            "range": "± 203943196",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1205219522,
            "range": "± 33918642",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2736205940,
            "range": "± 23638771",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 177936834,
            "range": "± 9183605",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 307274795,
            "range": "± 1139739",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 67912214,
            "range": "± 61847060",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 7964459,
            "range": "± 3643214",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 6801281,
            "range": "± 16521651",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 2840975,
            "range": "± 541791",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 24263428,
            "range": "± 1244914",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 18911020,
            "range": "± 3099463",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 19336917,
            "range": "± 1241889",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 18354692,
            "range": "± 2030264",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 17775156,
            "range": "± 10562928",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 9024441,
            "range": "± 424057",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 13416943,
            "range": "± 1593778",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 8222499,
            "range": "± 467909",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 70058949,
            "range": "± 4831234",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2217057,
            "range": "± 185712",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 72115910,
            "range": "± 6487598",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 346813,
            "range": "± 34852",
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
        "date": 1789289851198,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 132377168,
            "range": "± 19512872",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 580186827,
            "range": "± 12298251",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10975042,
            "range": "± 723225",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 53615692,
            "range": "± 2328426",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 83997231,
            "range": "± 1851063",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 1023223380,
            "range": "± 98576419",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8430957,
            "range": "± 3066846",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 26019982,
            "range": "± 4376388",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3680981,
            "range": "± 68938",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1819662,
            "range": "± 51673",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 290309,
            "range": "± 25622",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 630011,
            "range": "± 24926",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 393802,
            "range": "± 6218",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1473553,
            "range": "± 11765",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 153634,
            "range": "± 3491",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 35147,
            "range": "± 2264",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 488133,
            "range": "± 15462",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1819591,
            "range": "± 24484",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 224285,
            "range": "± 27175",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 167977,
            "range": "± 5934",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 129576877,
            "range": "± 4373216",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1629246,
            "range": "± 157652",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1650900,
            "range": "± 77603",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 156134,
            "range": "± 19455",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 131193363,
            "range": "± 2275016",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2194771,
            "range": "± 82249",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1704910,
            "range": "± 106761",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 51640,
            "range": "± 7065",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 15189326,
            "range": "± 871353",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14991375,
            "range": "± 415558",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5634821,
            "range": "± 203739",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7032406,
            "range": "± 1972928",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 575610,
            "range": "± 115048",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6936058,
            "range": "± 1718972",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 94053478,
            "range": "± 13501430",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3289116,
            "range": "± 785171",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43087349,
            "range": "± 12751809",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1400478,
            "range": "± 143361",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 91236043,
            "range": "± 17156356",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1133158210,
            "range": "± 140859353",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1044544199,
            "range": "± 24243626",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2123746307,
            "range": "± 20951999",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 138519197,
            "range": "± 7816881",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 239976602,
            "range": "± 9933130",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 66295545,
            "range": "± 27227294",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 3674443,
            "range": "± 1339180",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 7402215,
            "range": "± 15072547",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 3195587,
            "range": "± 1050695",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 18312206,
            "range": "± 1391607",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 14767829,
            "range": "± 2199612",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 15214663,
            "range": "± 1942883",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 13927384,
            "range": "± 1571381",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 13017663,
            "range": "± 7864404",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 7788167,
            "range": "± 669964",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 10967025,
            "range": "± 1497505",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 6449227,
            "range": "± 291910",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 61962517,
            "range": "± 13910754",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 1772299,
            "range": "± 95583",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 65003479,
            "range": "± 3537460",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 302048,
            "range": "± 22963",
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
        "date": 1789379070188,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 186625026,
            "range": "± 32881646",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 524391845,
            "range": "± 18383497",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 9856624,
            "range": "± 2083389",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 51444996,
            "range": "± 6518076",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 96172457,
            "range": "± 1076346",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 904635212,
            "range": "± 12372821",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8688113,
            "range": "± 3962091",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 44991683,
            "range": "± 3817105",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 2998818,
            "range": "± 46345",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1343635,
            "range": "± 57103",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 215424,
            "range": "± 16304",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 428119,
            "range": "± 10641",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 294359,
            "range": "± 5262",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1084926,
            "range": "± 10586",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 117614,
            "range": "± 6500",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 32691,
            "range": "± 2810",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 345036,
            "range": "± 20131",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1318429,
            "range": "± 13299",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 158892,
            "range": "± 12147",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 126316,
            "range": "± 4801",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 90847431,
            "range": "± 1576276",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1123769,
            "range": "± 84668",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1231548,
            "range": "± 46692",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 146461,
            "range": "± 13283",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 88500478,
            "range": "± 1110795",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1537935,
            "range": "± 28579",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1234382,
            "range": "± 34995",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 47652,
            "range": "± 3818",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 10371381,
            "range": "± 315213",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 9929828,
            "range": "± 81258",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 4576943,
            "range": "± 209505",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6566649,
            "range": "± 1764641",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 412162,
            "range": "± 33503",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5853367,
            "range": "± 2213698",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 68705790,
            "range": "± 12136481",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 2632153,
            "range": "± 343055",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 42693332,
            "range": "± 2905969",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1004996,
            "range": "± 136480",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 80098876,
            "range": "± 11160507",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 956736425,
            "range": "± 158264408",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1530479747,
            "range": "± 43911789",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 3382394264,
            "range": "± 49549318",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 245007612,
            "range": "± 5453075",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 390663492,
            "range": "± 17896761",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 67875964,
            "range": "± 43054831",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5060361,
            "range": "± 23246877",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 6892049,
            "range": "± 1964864",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 5002585,
            "range": "± 1289333",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 27597231,
            "range": "± 2269599",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 21719082,
            "range": "± 4611697",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 23829301,
            "range": "± 1804260",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 21637838,
            "range": "± 4005461",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 20416737,
            "range": "± 13403354",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 11065037,
            "range": "± 775099",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 15776461,
            "range": "± 29474852",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9455134,
            "range": "± 733844",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 68645987,
            "range": "± 5283289",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2340799,
            "range": "± 131586",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 68675392,
            "range": "± 9707953",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 358062,
            "range": "± 29395",
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
        "date": 1789464220705,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 134187025,
            "range": "± 14184123",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 429254211,
            "range": "± 37842447",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 7394607,
            "range": "± 754527",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 40288311,
            "range": "± 2204714",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 91589461,
            "range": "± 5850342",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 878978619,
            "range": "± 35844593",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 6763593,
            "range": "± 3133560",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 24960735,
            "range": "± 2150650",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 2798402,
            "range": "± 56930",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 2241630,
            "range": "± 94476",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 337878,
            "range": "± 12654",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 607468,
            "range": "± 8816",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 516623,
            "range": "± 25213",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1783655,
            "range": "± 28363",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 197882,
            "range": "± 7807",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 49622,
            "range": "± 8152",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 585111,
            "range": "± 12046",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 2178599,
            "range": "± 52903",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 203547,
            "range": "± 14196",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 155194,
            "range": "± 4371",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 98362983,
            "range": "± 1909485",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1879733,
            "range": "± 313505",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1595664,
            "range": "± 137424",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 184658,
            "range": "± 19171",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 96630749,
            "range": "± 1597023",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2386325,
            "range": "± 125755",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1534069,
            "range": "± 81536",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 61979,
            "range": "± 6216",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 11983220,
            "range": "± 499160",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 11286805,
            "range": "± 220180",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 4979866,
            "range": "± 365798",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7208899,
            "range": "± 1832988",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 617984,
            "range": "± 78786",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 4992980,
            "range": "± 1433288",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 83144185,
            "range": "± 8307564",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3952274,
            "range": "± 541840",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43826889,
            "range": "± 3090305",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1644840,
            "range": "± 312343",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 104344727,
            "range": "± 6192418",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 989494428,
            "range": "± 190906956",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1417066356,
            "range": "± 50197540",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 3038831263,
            "range": "± 66068812",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 212630961,
            "range": "± 13188834",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 340667884,
            "range": "± 10951056",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 64945059,
            "range": "± 39088144",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5364180,
            "range": "± 16717966",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 5618686,
            "range": "± 3081497",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4443507,
            "range": "± 989993",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 26874364,
            "range": "± 2508856",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 21752641,
            "range": "± 4311402",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 23199603,
            "range": "± 1830895",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 21102897,
            "range": "± 1807509",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 17531038,
            "range": "± 3022648",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10551685,
            "range": "± 244431",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 14043758,
            "range": "± 54548321",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9714459,
            "range": "± 490144",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 62048904,
            "range": "± 7379759",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2403911,
            "range": "± 191431",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 65697244,
            "range": "± 11791588",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 380406,
            "range": "± 37872",
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
        "date": 1789550156677,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 203583733,
            "range": "± 12120562",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 571766220,
            "range": "± 41205271",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10136331,
            "range": "± 934438",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 62161980,
            "range": "± 6402431",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 111741903,
            "range": "± 3318719",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 962164311,
            "range": "± 39687097",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9559349,
            "range": "± 3683843",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 30033370,
            "range": "± 4612413",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3041325,
            "range": "± 82010",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1359751,
            "range": "± 25490",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 214991,
            "range": "± 22447",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 433694,
            "range": "± 14649",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 305004,
            "range": "± 5085",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1106805,
            "range": "± 24590",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 117071,
            "range": "± 6649",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 38433,
            "range": "± 4018",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 351701,
            "range": "± 16270",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1361378,
            "range": "± 21315",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 152744,
            "range": "± 20230",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 126925,
            "range": "± 6279",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 82248115,
            "range": "± 1264492",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1139902,
            "range": "± 55911",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1124654,
            "range": "± 41722",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 129054,
            "range": "± 18817",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 82051422,
            "range": "± 1101412",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1566451,
            "range": "± 27969",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1140960,
            "range": "± 40203",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 45809,
            "range": "± 6844",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 10474970,
            "range": "± 172259",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 10006465,
            "range": "± 79790",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 4502162,
            "range": "± 256419",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6396191,
            "range": "± 1960095",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 397318,
            "range": "± 65981",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6076241,
            "range": "± 1948269",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 75966797,
            "range": "± 11548522",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 2612419,
            "range": "± 294092",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 42764236,
            "range": "± 4500506",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1004324,
            "range": "± 64148",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 83428560,
            "range": "± 2928605",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1011088579,
            "range": "± 101049396",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1324736482,
            "range": "± 33429359",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2897398785,
            "range": "± 32554513",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 201649133,
            "range": "± 9975594",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 316302915,
            "range": "± 4482058",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 67964907,
            "range": "± 82700182",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5341548,
            "range": "± 960990",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 8340056,
            "range": "± 2439337",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4119665,
            "range": "± 754676",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 26483048,
            "range": "± 1562256",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 20725302,
            "range": "± 3062433",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 22593321,
            "range": "± 1711598",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 19692582,
            "range": "± 2620979",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 18080493,
            "range": "± 15412142",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 9966653,
            "range": "± 450034",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 14697478,
            "range": "± 1845459",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9344092,
            "range": "± 942760",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 61530733,
            "range": "± 12709508",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2300409,
            "range": "± 174722",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 70482884,
            "range": "± 5991389",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 326994,
            "range": "± 42778",
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
        "date": 1789636879440,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 225849473,
            "range": "± 28790687",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 562294490,
            "range": "± 19444385",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10099272,
            "range": "± 2194720",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 58035068,
            "range": "± 10010975",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 106767709,
            "range": "± 2215709",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 908848012,
            "range": "± 8998108",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 8888406,
            "range": "± 3446649",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 27894926,
            "range": "± 3317927",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3005851,
            "range": "± 54602",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1355318,
            "range": "± 62433",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 204582,
            "range": "± 106706",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 453854,
            "range": "± 14515",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 291178,
            "range": "± 6558",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1092913,
            "range": "± 15848",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 119691,
            "range": "± 4157",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 35610,
            "range": "± 2429",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 347697,
            "range": "± 11994",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1354514,
            "range": "± 19365",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 152135,
            "range": "± 17054",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 128512,
            "range": "± 5011",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 84753574,
            "range": "± 902749",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1128027,
            "range": "± 139694",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1206931,
            "range": "± 40610",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 129560,
            "range": "± 15396",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 84585161,
            "range": "± 684624",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1516165,
            "range": "± 34239",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1215594,
            "range": "± 26119",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 50415,
            "range": "± 7110",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 10470769,
            "range": "± 204525",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 9887929,
            "range": "± 234583",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 4338596,
            "range": "± 263915",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7246376,
            "range": "± 1964675",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 417422,
            "range": "± 37562",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 7141140,
            "range": "± 2429591",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 76744384,
            "range": "± 8103396",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 2749900,
            "range": "± 470080",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43196962,
            "range": "± 417601",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1021042,
            "range": "± 480391",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 79556526,
            "range": "± 245919319",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 979123259,
            "range": "± 184686317",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1329367891,
            "range": "± 33523172",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2896000347,
            "range": "± 28290030",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 200916780,
            "range": "± 9126657",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 326147394,
            "range": "± 3070138",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 66186321,
            "range": "± 91313207",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 6157366,
            "range": "± 35428876",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 5976478,
            "range": "± 1575177",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4877609,
            "range": "± 1009996",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 26413102,
            "range": "± 1449470",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 21547217,
            "range": "± 3938564",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 22647355,
            "range": "± 1509499",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 21208474,
            "range": "± 2374577",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 19314076,
            "range": "± 6370995",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10166296,
            "range": "± 343578",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 14039663,
            "range": "± 1498521",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9216982,
            "range": "± 598003",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 60426123,
            "range": "± 4391592",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2257359,
            "range": "± 179714",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 67498360,
            "range": "± 12453278",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 373874,
            "range": "± 17753",
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
        "date": 1789721881083,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 84520667,
            "range": "± 9359739",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 342479994,
            "range": "± 16322858",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 5577902,
            "range": "± 1350798",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 30467171,
            "range": "± 3055821",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 76705323,
            "range": "± 6225474",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 731630296,
            "range": "± 56769666",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 6376110,
            "range": "± 7118006",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 19098077,
            "range": "± 8451637",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3653295,
            "range": "± 59287",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1821477,
            "range": "± 38053",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 312848,
            "range": "± 17903",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 650180,
            "range": "± 23668",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 410374,
            "range": "± 16624",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1505040,
            "range": "± 16524",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 153609,
            "range": "± 7250",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 33111,
            "range": "± 2216",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 501118,
            "range": "± 19483",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1797995,
            "range": "± 23704",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 223270,
            "range": "± 18985",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 177432,
            "range": "± 5538",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 125218015,
            "range": "± 3692880",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1616880,
            "range": "± 143216",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1702497,
            "range": "± 74470",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 147265,
            "range": "± 21699",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 122846138,
            "range": "± 3634095",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2342948,
            "range": "± 181986",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1775310,
            "range": "± 78919",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 56019,
            "range": "± 8100",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 14968028,
            "range": "± 437824",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14823183,
            "range": "± 220301",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5816890,
            "range": "± 215702",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7649325,
            "range": "± 1815729",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 574694,
            "range": "± 63268",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5722018,
            "range": "± 2295267",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 86144089,
            "range": "± 10101654",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3264277,
            "range": "± 309960",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43627842,
            "range": "± 1413778",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1502111,
            "range": "± 153046",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 91498744,
            "range": "± 15715287",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1143295445,
            "range": "± 89355975",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1083043424,
            "range": "± 61118866",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2104889555,
            "range": "± 24519067",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 136625729,
            "range": "± 2662519",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 239739949,
            "range": "± 2380110",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 60296226,
            "range": "± 21302955",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 7498193,
            "range": "± 317045269",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 4569050,
            "range": "± 2004217",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 2154133,
            "range": "± 234299",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 18506797,
            "range": "± 1595985",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 14332899,
            "range": "± 1943552",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 15030579,
            "range": "± 767558",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 14068305,
            "range": "± 1086602",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 13112855,
            "range": "± 16577598",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 7550579,
            "range": "± 500639",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 11052875,
            "range": "± 28890010",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 6422132,
            "range": "± 338359",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 64732085,
            "range": "± 3747925",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 1757511,
            "range": "± 79428",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 62330153,
            "range": "± 6512893",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 312324,
            "range": "± 26907",
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
        "date": 1789807336460,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 202740218,
            "range": "± 16495843",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 611557326,
            "range": "± 36987342",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10302729,
            "range": "± 888270",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 60469762,
            "range": "± 5122004",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 111424111,
            "range": "± 3646516",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 955875413,
            "range": "± 49297109",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9630218,
            "range": "± 3837578",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29580192,
            "range": "± 4146498",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3663259,
            "range": "± 66017",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1793033,
            "range": "± 59548",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 291607,
            "range": "± 10204",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 642789,
            "range": "± 17597",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 397513,
            "range": "± 6786",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1469612,
            "range": "± 25076",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 150895,
            "range": "± 6953",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 31844,
            "range": "± 2961",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 480707,
            "range": "± 18200",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1799545,
            "range": "± 36765",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 221169,
            "range": "± 19420",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 167862,
            "range": "± 4151",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 126989005,
            "range": "± 4808129",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1598360,
            "range": "± 136292",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1716224,
            "range": "± 83333",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 146819,
            "range": "± 19226",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 127709641,
            "range": "± 4834153",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2279530,
            "range": "± 112455",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1775839,
            "range": "± 107011",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 48195,
            "range": "± 9384",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 14968983,
            "range": "± 533444",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14621456,
            "range": "± 394153",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5924209,
            "range": "± 204961",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7461919,
            "range": "± 1772800",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 596664,
            "range": "± 163176",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 6906795,
            "range": "± 1979853",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 81429848,
            "range": "± 12339546",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3450755,
            "range": "± 413216",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43487080,
            "range": "± 1467353",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1475798,
            "range": "± 101185",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 90173106,
            "range": "± 18473985",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1068365715,
            "range": "± 119251123",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1081710295,
            "range": "± 53095624",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2166788848,
            "range": "± 26029238",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 140268006,
            "range": "± 8546494",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 245138817,
            "range": "± 3540933",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 61156197,
            "range": "± 67729338",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5995877,
            "range": "± 2736186",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 7962294,
            "range": "± 2651350",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 2216149,
            "range": "± 507366",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 18142632,
            "range": "± 1335487",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 14811245,
            "range": "± 2306471",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 14899601,
            "range": "± 2359266",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 14061162,
            "range": "± 2849168",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 14016473,
            "range": "± 10420106",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 7437400,
            "range": "± 574087",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 10491467,
            "range": "± 1431282",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 6163984,
            "range": "± 326974",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 63053225,
            "range": "± 7637260",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 1795883,
            "range": "± 153992",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 65251954,
            "range": "± 5690126",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 334023,
            "range": "± 32180",
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
        "date": 1789895687993,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 199851517,
            "range": "± 15717315",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 614934413,
            "range": "± 47902708",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10784291,
            "range": "± 870297",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 60753071,
            "range": "± 8288023",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 112275392,
            "range": "± 2767480",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 964883388,
            "range": "± 47623061",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9723735,
            "range": "± 3144173",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 30318030,
            "range": "± 4823701",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 2356348,
            "range": "± 48203",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1602043,
            "range": "± 271360",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 275724,
            "range": "± 55929",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 391752,
            "range": "± 25666",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 534370,
            "range": "± 10790",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1666715,
            "range": "± 122227",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 164858,
            "range": "± 53144",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 47699,
            "range": "± 2594",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 534563,
            "range": "± 22852",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1900787,
            "range": "± 98237",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 184401,
            "range": "± 42169",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 126056,
            "range": "± 7171",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 118845187,
            "range": "± 14390801",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1288168,
            "range": "± 161864",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 4061863,
            "range": "± 922487",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 172565,
            "range": "± 30824",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 110056559,
            "range": "± 4980392",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 1856432,
            "range": "± 166586",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 2650780,
            "range": "± 1050920",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 63444,
            "range": "± 14348",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 10112474,
            "range": "± 356855",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 9342160,
            "range": "± 602489",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 3858910,
            "range": "± 245998",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6975517,
            "range": "± 3020920",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 426742,
            "range": "± 44185",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 4751718,
            "range": "± 1922339",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 83028197,
            "range": "± 10468007",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 2758100,
            "range": "± 310048",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43259829,
            "range": "± 2232217",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 891315,
            "range": "± 186694",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 98972176,
            "range": "± 11758037",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 970778607,
            "range": "± 155724971",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1236081515,
            "range": "± 32993513",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2694248440,
            "range": "± 25901864",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 180469287,
            "range": "± 12402002",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 305215456,
            "range": "± 3830908",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 62182951,
            "range": "± 59453406",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 10248978,
            "range": "± 316484281",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 8671338,
            "range": "± 15122424",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 3188359,
            "range": "± 185013",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 23156357,
            "range": "± 835304",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 18885406,
            "range": "± 1900332",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 18305884,
            "range": "± 688383",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 17829538,
            "range": "± 2190004",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 15274843,
            "range": "± 9724304",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 9815013,
            "range": "± 382963",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 12755290,
            "range": "± 1145250",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 8745378,
            "range": "± 1621680",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 48613450,
            "range": "± 9690522",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2777523,
            "range": "± 352417",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 54657037,
            "range": "± 5382184",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 440011,
            "range": "± 64130",
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
        "date": 1789983958128,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 205538582,
            "range": "± 44639345",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 571187733,
            "range": "± 40307404",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10643729,
            "range": "± 1196392",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 59068991,
            "range": "± 2727576",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 109785534,
            "range": "± 2021101",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 962219417,
            "range": "± 48580964",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9785761,
            "range": "± 3349201",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29390317,
            "range": "± 4075575",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3668432,
            "range": "± 53893",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1842782,
            "range": "± 96806",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 292989,
            "range": "± 15475",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 631912,
            "range": "± 19210",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 410773,
            "range": "± 9759",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1474626,
            "range": "± 28680",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 152328,
            "range": "± 7223",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 32556,
            "range": "± 2048",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 485220,
            "range": "± 26140",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1773745,
            "range": "± 35628",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 223084,
            "range": "± 22073",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 170703,
            "range": "± 7862",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 130791827,
            "range": "± 3572912",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1554308,
            "range": "± 119133",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1748903,
            "range": "± 63826",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 163608,
            "range": "± 30244",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 130586523,
            "range": "± 3642006",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2277065,
            "range": "± 182112",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1721792,
            "range": "± 71009",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 58515,
            "range": "± 10344",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 14911925,
            "range": "± 446481",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14555720,
            "range": "± 671365",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5744992,
            "range": "± 222668",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7754578,
            "range": "± 1793711",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 623438,
            "range": "± 86866",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5852822,
            "range": "± 3195524",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 86374272,
            "range": "± 8191125",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3295875,
            "range": "± 737980",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43651147,
            "range": "± 1575081",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1424015,
            "range": "± 160738",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 89113800,
            "range": "± 32497789",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1088033161,
            "range": "± 161474137",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1334953289,
            "range": "± 30605624",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2892302988,
            "range": "± 25548189",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 202234623,
            "range": "± 8662432",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 327563866,
            "range": "± 13280101",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 68714120,
            "range": "± 58471540",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5847286,
            "range": "± 24228446",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 7954381,
            "range": "± 2030512",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4202480,
            "range": "± 1112441",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 26484763,
            "range": "± 2242306",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 20712877,
            "range": "± 4686832",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 22498696,
            "range": "± 1407347",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 19957688,
            "range": "± 2438904",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 17723806,
            "range": "± 3091077",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10156435,
            "range": "± 452674",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 14451157,
            "range": "± 40811740",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9090305,
            "range": "± 802026",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 62205117,
            "range": "± 14915226",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2224722,
            "range": "± 204970",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 65133904,
            "range": "± 12588092",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 314130,
            "range": "± 32462",
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
        "date": 1790068412905,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 201337016,
            "range": "± 15233905",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 597838984,
            "range": "± 37119677",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10346353,
            "range": "± 1115053",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 63090593,
            "range": "± 9909264",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 110687557,
            "range": "± 3223981",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 957168591,
            "range": "± 46567231",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9389969,
            "range": "± 3999213",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29796204,
            "range": "± 4506122",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3572663,
            "range": "± 56460",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1842320,
            "range": "± 81173",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 291639,
            "range": "± 18789",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 630264,
            "range": "± 30405",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 393742,
            "range": "± 4221",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1418235,
            "range": "± 18014",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 148145,
            "range": "± 7361",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 29653,
            "range": "± 1385",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 473973,
            "range": "± 9552",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1749718,
            "range": "± 23112",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 210339,
            "range": "± 22888",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 157206,
            "range": "± 4106",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 116887465,
            "range": "± 5244809",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1594499,
            "range": "± 109300",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1614733,
            "range": "± 71761",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 141941,
            "range": "± 12573",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 118758664,
            "range": "± 3563565",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2150183,
            "range": "± 176491",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1633566,
            "range": "± 73320",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 44608,
            "range": "± 5004",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 14638199,
            "range": "± 490870",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14447397,
            "range": "± 258641",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5721454,
            "range": "± 99369",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 7058930,
            "range": "± 2965687",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 651768,
            "range": "± 91085",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5742636,
            "range": "± 1801035",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 87828973,
            "range": "± 13749336",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3061861,
            "range": "± 284556",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43364956,
            "range": "± 1939034",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1424193,
            "range": "± 211791",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 87453180,
            "range": "± 3961584",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1104547241,
            "range": "± 172846154",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1235595803,
            "range": "± 41814048",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 2959041827,
            "range": "± 32455900",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 192133297,
            "range": "± 7481683",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 325854504,
            "range": "± 7958526",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 69321681,
            "range": "± 99290579",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 9015059,
            "range": "± 32239647",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 9300450,
            "range": "± 25895096",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 3443721,
            "range": "± 159363568",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 22687850,
            "range": "± 1177337",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 17224833,
            "range": "± 2224757",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 16019750,
            "range": "± 843776",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 15997787,
            "range": "± 1213202",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 16380622,
            "range": "± 8775489",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 8785521,
            "range": "± 390943",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 12799439,
            "range": "± 1420858",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 7808954,
            "range": "± 1249250",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 44204734,
            "range": "± 13283904",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2475267,
            "range": "± 421083",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 57430629,
            "range": "± 3742640",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 410572,
            "range": "± 38235",
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
        "date": 1790154941451,
        "tool": "cargo",
        "benches": [
          {
            "name": "performance/mongodb_single_write",
            "value": 199899069,
            "range": "± 41407058",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_single_read",
            "value": 575688761,
            "range": "± 38406895",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_write",
            "value": 10605378,
            "range": "± 1522074",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mongodb_batch_read",
            "value": 60338489,
            "range": "± 6029779",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_write",
            "value": 109124398,
            "range": "± 1372935",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_single_read",
            "value": 929119897,
            "range": "± 25137951",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_write",
            "value": 9676950,
            "range": "± 3835955",
            "unit": "ns/iter"
          },
          {
            "name": "performance/postgres_batch_read",
            "value": 29614716,
            "range": "± 4024446",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_write",
            "value": 3678841,
            "range": "± 55205",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_single_read",
            "value": 1793023,
            "range": "± 93145",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_write",
            "value": 293003,
            "range": "± 60546",
            "unit": "ns/iter"
          },
          {
            "name": "performance/zeromq_batch_read",
            "value": 636653,
            "range": "± 21577",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_write",
            "value": 398592,
            "range": "± 14479",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_single_read",
            "value": 1462778,
            "range": "± 59765",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_write",
            "value": 152628,
            "range": "± 4478",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_batch_read",
            "value": 33973,
            "range": "± 3203",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_write",
            "value": 479586,
            "range": "± 34123",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_single_read",
            "value": 1801269,
            "range": "± 23739",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_write",
            "value": 222049,
            "range": "± 37948",
            "unit": "ns/iter"
          },
          {
            "name": "performance/memory_subscriber_batch_read",
            "value": 169310,
            "range": "± 4816",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_write",
            "value": 133580246,
            "range": "± 4195265",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_single_read",
            "value": 1673853,
            "range": "± 101450",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_write",
            "value": 1761083,
            "range": "± 92181",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_delete_batch_read",
            "value": 153437,
            "range": "± 19167",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_write",
            "value": 131320556,
            "range": "± 3150291",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_single_read",
            "value": 2222865,
            "range": "± 104602",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_write",
            "value": 1725304,
            "range": "± 80131",
            "unit": "ns/iter"
          },
          {
            "name": "performance/file_batch_read",
            "value": 48180,
            "range": "± 5930",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_batch",
            "value": 15100412,
            "range": "± 646359",
            "unit": "ns/iter"
          },
          {
            "name": "performance/http_single",
            "value": 14770903,
            "range": "± 284359",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_write",
            "value": 5867506,
            "range": "± 232985",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_single_read",
            "value": 6083310,
            "range": "± 2711189",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_write",
            "value": 559419,
            "range": "± 178728",
            "unit": "ns/iter"
          },
          {
            "name": "performance/websocket_batch_read",
            "value": 5679985,
            "range": "± 1881344",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_write",
            "value": 85552376,
            "range": "± 10467727",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_single_read",
            "value": 3189764,
            "range": "± 239992",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_write",
            "value": 43301362,
            "range": "± 3243829",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_batch_read",
            "value": 1489203,
            "range": "± 96419",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_batch",
            "value": 90117472,
            "range": "± 7899193",
            "unit": "ns/iter"
          },
          {
            "name": "performance/grpc_server_single",
            "value": 1221784403,
            "range": "± 186487850",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_write",
            "value": 1416138599,
            "range": "± 37885035",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_single_read",
            "value": 3033969012,
            "range": "± 48228449",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_write",
            "value": 207031215,
            "range": "± 4344307",
            "unit": "ns/iter"
          },
          {
            "name": "performance/aws_batch_read",
            "value": 349104331,
            "range": "± 11868614",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_write",
            "value": 68786721,
            "range": "± 49205194",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_single_read",
            "value": 5696854,
            "range": "± 13480567",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_write",
            "value": 6900122,
            "range": "± 15297057",
            "unit": "ns/iter"
          },
          {
            "name": "performance/kafka_batch_read",
            "value": 4340003,
            "range": "± 1462952",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_write",
            "value": 26968568,
            "range": "± 1819724",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_single_read",
            "value": 22744261,
            "range": "± 4251327",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_write",
            "value": 23642927,
            "range": "± 2003952",
            "unit": "ns/iter"
          },
          {
            "name": "performance/amqp_batch_read",
            "value": 21350007,
            "range": "± 1894115",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_write",
            "value": 18718532,
            "range": "± 2680803",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_single_read",
            "value": 10554894,
            "range": "± 726836",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_write",
            "value": 13920459,
            "range": "± 4598676",
            "unit": "ns/iter"
          },
          {
            "name": "performance/nats_batch_read",
            "value": 9531980,
            "range": "± 1194416",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_write",
            "value": 60591327,
            "range": "± 12580900",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_single_read",
            "value": 2248077,
            "range": "± 103172",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_write",
            "value": 68195411,
            "range": "± 4090783",
            "unit": "ns/iter"
          },
          {
            "name": "performance/mqtt_batch_read",
            "value": 366476,
            "range": "± 16942",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}