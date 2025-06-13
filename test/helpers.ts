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
import { Admin, Producer, sleep, type ProducerOptions } from '@platformatic/kafka'
import { randomUUID } from 'crypto'
import { type TestContext } from 'node:test'
import { KafkaInstrumentation } from '../src/instrumentation.ts'
import { name, version } from '../src/version.ts'

const bootstrapBrokers = ['localhost:9092']

interface TracingContext {
  instrumentation: KafkaInstrumentation
}

export function createProducer<K = Buffer, V = Buffer, HK = Buffer, HV = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ProducerOptions<K, V, HK, HV>> = {}
) {
  const options: ProducerOptions<K, V, HK, HV> = {
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers,
    ...overrideOptions
  }

  const producer = new Producer<K, V, HK, HV>(options)
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
  before?: (ctx: TracingContext) => void,
  after?: (ctx: TracingContext) => void
) {
  trace.disable()
  metrics.disable()

  const traceExporter = new InMemorySpanExporter()
  const metricsExporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE)

  const instrumentation = new KafkaInstrumentation()

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: name, [ATTR_SERVICE_VERSION]: version }),
    spanProcessors: [new SimpleSpanProcessor(traceExporter)],
    metricReader: new PeriodicExportingMetricReader({ exporter: metricsExporter, exportIntervalMillis: 100 }),
    instrumentations: [instrumentation]
  })

  await sdk.start()

  t.after(() => {
    sdk.shutdown()
    trace.disable()
    metrics.disable()
  })

  const rootSpan = trace.getTracer(name, version)!.startSpan('root')

  let promise
  context.with(trace.setSpan(context.active(), rootSpan), async () => {
    await before?.({ instrumentation })
    promise = fn()
    await after?.({ instrumentation })
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
