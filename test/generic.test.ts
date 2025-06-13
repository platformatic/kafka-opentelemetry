import { stringSerializers } from '@platformatic/kafka'
import { deepStrictEqual } from 'node:assert'
import { test } from 'node:test'
import { KafkaInstrumentation } from '../src/instrumentation.ts'
import { kInitialized } from '../src/symbols.ts'
import { createProducer, createTopic, runWithTracing } from './helpers.ts'

test('should protect access in the constructor', async () => {
  class MockInstrumentation extends KafkaInstrumentation {
    constructor () {
      super()
      this[kInitialized] = false // Simulate that the instrumentation is initialized

      this.enable()
      this.disable()
    }
  }

  const a = new MockInstrumentation()
  deepStrictEqual(a[kInitialized], false)
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
