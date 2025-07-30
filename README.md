# @platformatic/kafka-opentelemetry

OpenTelemetry instrumentation for [@platformatic/kafka](https://github.com/platformatic/kafka).

## Features

- **Automatic Tracing**: Comprehensive tracing for Kafka producers and consumers.
- **Semantic Conventions**: Follows OpenTelemetry semantic conventions for messaging systems.
- **Zero Configuration**: Works out of the box with minimal setup.
- **Performance Optimized**: Low-overhead instrumentation designed for production use.
- **Type Safety**: Full TypeScript support with strong typing.

## Installation

```bash
npm install @platformatic/kafka-opentelemetry
```

## Getting Started

### Basic Usage

```typescript
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { registerInstrumentations } from '@opentelemetry/instrumentation'
import { KafkaInstrumentation } from '@platformatic/kafka-opentelemetry'

// Initialize OpenTelemetry
const provider = new NodeTracerProvider()
provider.register()

// Register the Kafka instrumentation
registerInstrumentations({
  instrumentations: [new KafkaInstrumentation()]
})

// Now use @platformatic/kafka as normal - traces will be automatically generated
import { Producer, Consumer } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092']
})

const consumer = new Consumer({
  groupId: 'my-consumer-group',
  clientId: 'my-consumer',
  bootstrapBrokers: ['localhost:9092']
})
```

### Configuration Options

| Property                | Type       | Default     | Description                                                                                                        |
| ----------------------- | ---------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `enabled`               | `boolean`  | `true`      | If the instrumentation is enabled.                                                                                 |
| `producedKeySerializer` | `Function` | `undefined` | A function to serialized produced keys before tracing them. This function must be synchronous.                     |
| `consumedKeySerializer` | `Function` | `undefined` | A function to serialized consumed keys before tracing them. This function must be synchronous.                     |
| beforeProduce           | `Function` | `undefined` | A function to customize a message before sending it to Kafka.This function must be synchronous.                    |
| beforeProcess           | `Function` | `undefined` | A function to analyze a consumed message before pushing to the messages stream. This function must be synchronous. |

### Producer Trace

- **Span Kind**: `PRODUCER`
- **Attributes**:
  - `messaging.system`: `kafka`
  - `messaging.operation.name`: `send`
  - `messaging.operation.type`: `send`
  - `messaging.destination.name`: Topic name
  - `messaging.destination.partition.id`: Partition number (if specified)
  - `messaging.kafka.message.key`: Message key (if present)
  - `messaging.kafka.message.tombstone`: `true` (if no value is present)
  - `error.type`: Error code (if present)

### Consumer Traces

- **Span Kind**: `CONSUMER`
- **Attributes**:
  - `messaging.system`: `kafka`
  - `messaging.operation.name`: `process`
  - `messaging.operation.type`: `process`
  - `messaging.destination.name`: Topic name
  - `messaging.destination.partition.id`: Partition number (if specified)
  - `messaging.kafka.message.key`: Message key (if present)
  - `messaging.kafka.message.tombstone`: `true` (if no value is present)
  - `error.type`: Error code (if present)

## Metrics

This instrumentation automatically exports OpenTelemetry metrics to provide observability into Kafka operations.

#### `messaging.client.sent.messages`

- **Type**: Counter
- **Description**: Number of messages sent by Kafka producers
- **Attributes**:
  - `messaging.system`: `kafka`
  - `messaging.operation.name`: `send`
  - `messaging.destination.name`: Topic name
  - `messaging.destination.partition.id`: Partition ID (when specified)
  - `error.type`: Error code (only present when sending fails)

#### `messaging.client.consumed.messages`

- **Type**: Counter
- **Description**: Number of messages consumed by Kafka consumers
- **Attributes**:
  - `messaging.system`: `kafka`
  - `messaging.operation.name`: `process`
  - `messaging.destination.name`: Topic name
  - `messaging.destination.partition.id`: Partition ID
  - `error.type`: Error code (only present when processing fails)

### Duration Histograms

#### `messaging.client.operation.duration`

- **Type**: Histogram
- **Description**: Duration of Kafka client API operations (ApiVersions, Metadata, Produce, etc.)
- **Unit**: Seconds
- **Bucket Boundaries**: `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`
- **Attributes**:
  - `messaging.system`: `kafka`
  - `messaging.operation.name`: API operation name (e.g., `ApiVersions`, `Metadata`, `Produce`)
  - `server.address`: Kafka broker host
  - `server.port`: Kafka broker port

#### `messaging.process.duration`

- **Type**: Histogram
- **Description**: Duration of message processing operations in consumers
- **Unit**: Seconds
- **Bucket Boundaries**: `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`
- **Attributes**:
  - `messaging.system`: `kafka`
  - `messaging.operation.name`: `process`
  - `error.type`: Error code (only present when processing fails)

## Requirements

- Node.js >= 22.14.0

## License

Apache-2.0 - See [LICENSE](LICENSE) for more information.
