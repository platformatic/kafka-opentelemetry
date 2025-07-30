import {
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_SYSTEM,
  ATTR_SERVER_ADDRESS,
  ATTR_SERVER_PORT
} from '@opentelemetry/semantic-conventions/incubating'
import { connectionsApiChannel, producerSendsChannel, stringSerializers } from '@platformatic/kafka'
import { deepStrictEqual } from 'node:assert'
import { test } from 'node:test'
import { metricClientOperationDurationName } from '../src/attributes.ts'
import { KafkaInstrumentation } from '../src/instrumentation.ts'
import { kInitialized } from '../src/symbols.ts'
import { type Config } from '../src/types.ts'
import { name, version } from '../src/version.ts'
import { createProducer, createTopic, runWithTracing } from './helpers.ts'

test('should protect access in the constructor', async () => {
  class MockInstrumentation extends KafkaInstrumentation {
    constructor (config: Config) {
      super(config)
      this[kInitialized] = false // Simulate that the instrumentation is NOT initialized

      this.enable()
      this.disable()
    }
  }

  const instance = new MockInstrumentation({ enabled: false })
  deepStrictEqual(instance[kInitialized], false)
  instance[kInitialized] = true // Simulate that the instrumentation is initialized

  deepStrictEqual(connectionsApiChannel.hasSubscribers, false)
  deepStrictEqual(producerSendsChannel.hasSubscribers, false)

  instance.enable()
  deepStrictEqual(connectionsApiChannel.hasSubscribers, true)
  deepStrictEqual(producerSendsChannel.hasSubscribers, true)

  instance.disable()
  deepStrictEqual(connectionsApiChannel.hasSubscribers, false)
  deepStrictEqual(producerSendsChannel.hasSubscribers, false)
})

test('should add and remove diagnostic channel subscriber', async () => {
  class MockInstrumentation extends KafkaInstrumentation {
    constructor (config: Config) {
      super(config)
      this[kInitialized] = false // Do not instrument automatically
    }
  }

  const instance = new MockInstrumentation({ enabled: false })
  instance[kInitialized] = true // Simulate that the instrumentation is initialized

  deepStrictEqual(connectionsApiChannel.hasSubscribers, false)
  deepStrictEqual(producerSendsChannel.hasSubscribers, false)

  instance.enable()
  deepStrictEqual(connectionsApiChannel.hasSubscribers, true)
  deepStrictEqual(producerSendsChannel.hasSubscribers, true)

  instance.disable()
  deepStrictEqual(connectionsApiChannel.hasSubscribers, false)
  deepStrictEqual(producerSendsChannel.hasSubscribers, false)
})

test('should not trace if instrumentation is disabled', async t => {
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter } = await runWithTracing(
    t,
    async () => {
      return producer.send({
        messages: [
          {
            topic,
            key: 'test-key1',
            value: 'test-value',
            headers: {
              'test-header': 'test-header-value'
            },
            partition: 0
          }
        ]
      })
    },
    {},
    ({ instrumentation }) => {
      instrumentation.disable()
    },
    ({ instrumentation }) => {
      instrumentation.enable()
    }
  )

  const spans = traceExporter.getFinishedSpans()

  // Only the root span should be present
  deepStrictEqual(spans.length, 1)
})

test('should record metrics for all API calls', async t => {
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { metricsExporter } = await runWithTracing(t, async () => {
    return producer.send({
      messages: [
        {
          topic,
          key: 'test-key1',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )

  const apiCalls = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricClientOperationDurationName)

  deepStrictEqual(apiCalls?.dataPoints.length, 3)
  deepStrictEqual(apiCalls?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_OPERATION_NAME]: 'ApiVersions',
    [ATTR_MESSAGING_SYSTEM]: 'kafka',
    [ATTR_SERVER_ADDRESS]: 'localhost',
    [ATTR_SERVER_PORT]: '9092'
  })

  deepStrictEqual(apiCalls?.dataPoints[1].attributes[ATTR_MESSAGING_OPERATION_NAME], 'Metadata')
  deepStrictEqual(apiCalls?.dataPoints[2].attributes[ATTR_MESSAGING_OPERATION_NAME], 'Produce')
})
