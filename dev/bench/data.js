window.BENCHMARK_DATA = {
  "lastUpdate": 1788683512513,
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
      }
    ]
  }
}