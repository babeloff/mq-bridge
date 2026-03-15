# Configuration Guide

`mq-bridge` uses a flexible configuration system supporting YAML, JSON, and environment variables.

## Configuration Reference

The best way to understand the configuration structure is through a comprehensive example. `mq-bridge` uses a YAML map where keys are route names.

```yaml
# mq-bridge.yaml

# Route 1: Kafka to NATS
kafka_to_nats:
  concurrency: 4
  input:
    kafka:
      url: "localhost:9092"
      topic: "orders"
      group_id: "bridge_group"
      # TLS Configuration (Optional)
      tls:
        required: true
        ca_file: "./certs/ca.pem"
  output:
    nats:
      url: "nats://localhost:4222"
      subject: "orders.processed"
      stream: "orders_stream"

# Route 2: HTTP Webhook to MongoDB with Middleware
webhook_to_mongo:
  input:
    http:
      url: "0.0.0.0:8080"
      # Optional: Send response back to HTTP caller via another endpoint
      response_out:
        static: "Accepted"
    middlewares:
      - retry:
          max_attempts: 3
          initial_interval_ms: 500
  output:
    mongodb:
      url: "mongodb://localhost:27017"
      database: "app_db"
      collection: "webhooks"
      format: "json" # a bit slower, but better readability

# Route 3: File to AMQP (RabbitMQ)
file_ingest:
  input:
    file:
      path: "./data/input.jsonl"
  output:
    amqp:
      url: "amqp://localhost:5672"
      exchange: "logs"
      queue: "file_logs"

# Route 4: AWS SQS to SNS
aws_sqs_to_sns:
  input:
    aws:
      # To consume from SNS, subscribe this SQS queue to the SNS topic in AWS Console/Terraform.
      queue_url: "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue"
      region: "us-east-1"
      # Credentials (optional if using env vars or IAM roles)
      access_key: "test"
      secret_key: "test"
  output:
    aws:
      topic_arn: "arn:aws:sns:us-east-1:000000000000:my-topic"
      region: "us-east-1"

# Route 5: IBM MQ Example
ibm_mq_route:
  input:
    ibmmq:
      queue_manager: "QM1"
      url: "localhost(1414)"
      channel: "DEV.APP.SVRCONN"
      queue: "DEV.QUEUE.1"
      username: "app"
      password: "admin"
  output:
    memory:
      topic: "received_from_mq"

# Route 6: MQTT to Switch (Content-based Routing)
iot_router:
  input:
    mqtt:
      url: "mqtt://localhost:1883"
      topic: "sensors/+"
      qos: 1
  output:
    switch:
      metadata_key: "sensor_type"
      cases:
        temp:
          kafka:
            url: "localhost:9092"
            topic: "temperature"
      default:
        memory:
          topic: "dropped_sensors"

# Route 7: ZeroMQ PUSH/PULL
zeromq_pipeline:
  input:
    zeromq:
      url: "tcp://0.0.0.0:5555"
      socket_type: "pull"
      bind: true
  output:
    zeromq:
      url: "tcp://localhost:5556"
      socket_type: "push"
      bind: false

# Route 8: PostgreSQL via SQLx
sqlx_postgres_route:
  input:
    sqlx:
      url: "postgres://user:pass@localhost:5432/mydb"
      table: "job_queue"
      delete_after_read: true
  output:
    memory:
      topic: "processed_jobs"
```

## Configuration Details

### Environment Variables
All YAML configuration can be overridden with environment variables. The mapping follows this pattern:
`MQB__{ROUTE_NAME}__{PATH_TO_SETTING}`

For example, to set the Kafka topic for the `kafka_to_nats` route:
```sh
export MQB__KAFKA_TO_NATS__INPUT__KAFKA__TOPIC="my-other-topic"
```

### Middleware Configuration
Middleware is defined as a list under an endpoint.

```yaml
input:
  middlewares:
    - retry:
        max_attempts: 5
        initial_interval_ms: 200
    - dlq:
        endpoint:
          nats:
            subject: "my-dlq-subject"
            url: "nats://localhost:4222"
    - deduplication:
        sled_path: "/var/data/mq-bridge/dedup_db"
        ttl_seconds: 3600 # 1 hour
  kafka:
    # ... kafka config
```

### Specialized Endpoints

#### Switch

The `switch` endpoint is a conditional publisher that routes messages to different outputs based on a metadata key.

It checks the specified `metadata_key` in each message. If the key's value matches one of the `cases`, the message is forwarded to that endpoint. If no case matches, it's sent to the `default` endpoint. If there is no default, the message is dropped.

This is useful for content-based routing.

**Example**: Route orders to different systems based on `country_code` metadata.

```yaml
output:
  switch:
    metadata_key: "country_code"
    cases:
      US:
        kafka:
          topic: "us_orders"
          url: "kafka-us:9092"
      EU:
        nats:
          subject: "eu_orders"
          url: "nats-eu:4222"
    default:
      file:
        path: "/var/data/unroutable_orders.log"
```

### IDE Support (Schema Validation) 
mq-bridge includes a JSON schema for configuration validation and auto-completion. 
1. Ensure you have a YAML plugin installed (e.g., YAML for VS Code). 
2. Configure your editor to reference the schema. For VS Code, add this to .vscode/settings.json: 
```json 
{ 
  "yaml.schemas": { 
    "https://raw.githubusercontent.com/marcomq/mq-bridge/main/mq-bridge.schema.json": ["mq-bridge.yaml", "config.yaml"]
  } 
} 
```
To regenerate the schema from this repo, run: `cargo test --features schema`