# Import/Export Samples

Use these JSON files for quick manual UI checks.

## Import samples

1. Open the UI and go to `App Config`.
2. Click one of:
   - `Import Postman` -> use `postman.collection.sample.json`
   - `Import OpenAPI` -> use `openapi.sample.json`
   - `Import AsyncAPI` -> use `asyncapi.sample.json`
3. Enter a target publisher name when prompted.
4. Verify presets appear in `Publishers -> Presets`.

## Native MQB samples

- `mqb-presets.sample.json` can be imported with native preset import logic (used in automated tests).
- `mqb-export.sample.json` demonstrates a full config export payload shape.
- `mqb-export.protocol-showcase.sample.json` provides a rich demo dataset with:
  - publishers: HTTP, NATS, AMQP (RabbitMQ), Kafka, ZeroMQ, MQTT, gRPC, Memory
  - consumers: HTTP, NATS, AMQP, Kafka, ZeroMQ, MQTT, gRPC, Memory
  - presets + env vars for screenshot-ready UI data
- `mqb-config.consumers-showcase.sample.json` is optimized for importing consumers only.

## Screenshot-oriented import flow

### Publisher-rich import
1. Open `Publishers`.
2. Click `Import mq-bridge`.
3. Select `mqb-export.protocol-showcase.sample.json`.

### Consumer-rich import
1. Open `Consumers`.
2. Click `Import mq-bridge`.
3. Select `mqb-config.consumers-showcase.sample.json`.

## Export checks

From `App Config`:
- `Export Config` -> validate output contains only `config`.
- `Export All` -> validate output contains `config`, `presets`, and `envVars`.

From `Publishers -> Presets`:
- `Export Presets` -> validate output contains only current publisher presets + `envVars`.
