import { context, metrics, trace } from '@opentelemetry/api'
import { resourceFromAttributes } from '@opentelemetry/resources'
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  PeriodicExportingMetricReader
} from '@opentelemetry/sdk-metrics'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-node'
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions'
import { Admin, Consumer, Producer, sleep, type ConsumerOptions, type ProducerOptions } from '@platformatic/kafka'
import { randomUUID } from 'crypto'
import { type TestContext } from 'node:test'
import { KafkaInstrumentation } from '../src/instrumentation.ts'
import { type Config } from '../src/types.ts'
import { name, version } from '../src/version.ts'

const bootstrapBrokers = ['localhost:9092']

interface TracingContext {
  instrumentation: KafkaInstrumentation
}

export function createConsumer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ConsumerOptions<Key, Value, HeaderKey, HeaderValue>> = {}
) {
  const options: ConsumerOptions<Key, Value, HeaderKey, HeaderValue> = {
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers,
    groupId: `test-consumer-${randomUUID()}`,
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retries: 1,
    ...overrideOptions
  }

  const consumer = new Consumer<Key, Value, HeaderKey, HeaderValue>(options)
  t.after(() => consumer.close(true))

  return consumer
}

export function createProducer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ProducerOptions<Key, Value, HeaderKey, HeaderValue>> = {}
) {
  const options: ProducerOptions<Key, Value, HeaderKey, HeaderValue> = {
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers,
    ...overrideOptions
  }

  const producer = new Producer<Key, Value, HeaderKey, HeaderValue>(options)
  t.after(() => producer.close())

  return producer
}

export async function createTopic (t: TestContext, create: boolean = false) {
  const topic = `test-topic-${randomUUID()}`

  if (create) {
    const admin = new Admin({ clientId: `test-admin-${randomUUID()}`, bootstrapBrokers })
    t.after(() => admin.close())

    await admin.createTopics({ topics: [topic] })
  }

  return topic
}

export async function runWithTracing (
  t: TestContext,
  fn: () => Promise<unknown>,
  options?: Config,
  before?: (ctx: TracingContext) => void,
  after?: (ctx: TracingContext) => void
) {
  trace.disable()
  metrics.disable()

  const traceExporter = new InMemorySpanExporter()
  const metricsExporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE)

  const instrumentation = new KafkaInstrumentation(options)

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: name, [ATTR_SERVICE_VERSION]: version }),
    spanProcessors: [new SimpleSpanProcessor(traceExporter)],
    metricReader: new PeriodicExportingMetricReader({ exporter: metricsExporter, exportIntervalMillis: 100 }),
    instrumentations: [instrumentation]
  })

  await sdk.start()

  t.after(async () => {
    await sdk.shutdown()
    trace.disable()
    metrics.disable()
    // Note: the following is not invoked by the SDK, so we need to do it manually
    instrumentation.disable()
  })

  const rootSpan = trace.getTracer(name, version)!.startSpan('root')

  let promise
  context.with(trace.setSpan(context.active(), rootSpan), async () => {
    if (typeof before === 'function') {
      await before({ instrumentation })
    }

    promise = fn()
    await promise

    if (typeof after === 'function') {
      await after?.({ instrumentation })
    }

    rootSpan.end()
  })

  await promise

  // Wait for the exporters to flush
  metricsExporter.reset()

  // Wait up to 1000 for the metrics to be exported
  for (let i = 0; i < 10; i++) {
    await sleep(100)

    if (metricsExporter.getMetrics().length) {
      break
    }
  }

  return { traceExporter, metricsExporter }
}
