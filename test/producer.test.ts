import { SpanKind, SpanStatusCode } from '@opentelemetry/api'
import {
  ATTR_ERROR_TYPE,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY,
  ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_SYSTEM,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_SYSTEM_VALUE_KAFKA
} from '@opentelemetry/semantic-conventions/incubating'
import { stringSerializers } from '@platformatic/kafka'
import { deepStrictEqual, ok, rejects } from 'node:assert'
import { test } from 'node:test'
import { metricSentMessagesName } from '../src/attributes.ts'
import { name, version } from '../src/version.ts'
import { createProducer, createTopic, runWithTracing } from './helpers.ts'

test('should trace each produced message', async t => {
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
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
        },
        // Tombstone
        {
          topic,
          key: 'test-key2'
        }
      ]
    })
  })

  const spans = traceExporter.getFinishedSpans()

  const rootSpans = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)

  ok(rootSpans)
  deepStrictEqual(producerSpans.length, 2)

  deepStrictEqual(producerSpans[0].kind, SpanKind.PRODUCER)
  deepStrictEqual(producerSpans[0].spanContext().traceId, rootSpans.spanContext().traceId)
  deepStrictEqual(producerSpans[0].parentSpanContext!.spanId, rootSpans.spanContext().spanId)
  deepStrictEqual(producerSpans[0].status.code, SpanStatusCode.OK)
  deepStrictEqual(producerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND
  })

  deepStrictEqual(producerSpans[1].kind, SpanKind.PRODUCER)
  deepStrictEqual(producerSpans[1].spanContext().traceId, rootSpans.spanContext().traceId)
  deepStrictEqual(producerSpans[1].parentSpanContext!.spanId, rootSpans.spanContext().spanId)
  deepStrictEqual(producerSpans[1].status.code, SpanStatusCode.OK)
  deepStrictEqual(producerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: 'kafka',
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const sentMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricSentMessagesName)

  deepStrictEqual(sentMessages?.dataPoints.length, 2)
  deepStrictEqual(sentMessages?.dataPoints[0].value, 1)
  deepStrictEqual(sentMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0'
  })
  deepStrictEqual(sentMessages?.dataPoints[1].value, 1)
  deepStrictEqual(sentMessages?.dataPoints[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic
  })
})

test('should trace each failed produce', async t => {
  const producer = await createProducer(t, { serializers: stringSerializers, retries: 1, retryDelay: 100 })

  // Note that the topic is not created, so the produce will fail
  const topic = await createTopic(t, false)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await rejects(() => {
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
          },
          // Tombstone
          {
            topic,
            key: 'test-key2'
          }
        ]
      })
    })
  })

  const spans = traceExporter.getFinishedSpans()

  const rootSpans = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)

  ok(rootSpans)
  deepStrictEqual(producerSpans.length, 2)

  deepStrictEqual(producerSpans[0].kind, SpanKind.PRODUCER)
  deepStrictEqual(producerSpans[0].spanContext().traceId, rootSpans.spanContext().traceId)
  deepStrictEqual(producerSpans[0].parentSpanContext!.spanId, rootSpans.spanContext().spanId)
  deepStrictEqual(producerSpans[0].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(producerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_RESPONSE'
  })

  deepStrictEqual(producerSpans[1].kind, SpanKind.PRODUCER)
  deepStrictEqual(producerSpans[1].spanContext().traceId, rootSpans.spanContext().traceId)
  deepStrictEqual(producerSpans[1].parentSpanContext!.spanId, rootSpans.spanContext().spanId)
  deepStrictEqual(producerSpans[1].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(producerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: 'kafka',
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_RESPONSE'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const sentMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricSentMessagesName)

  deepStrictEqual(sentMessages?.dataPoints.length, 2)
  deepStrictEqual(sentMessages?.dataPoints[0].value, 1)
  deepStrictEqual(sentMessages?.dataPoints[0].attributes[ATTR_ERROR_TYPE], 'PLT_KFK_RESPONSE')
  deepStrictEqual(sentMessages?.dataPoints[1].value, 1)
  deepStrictEqual(sentMessages?.dataPoints[1].attributes[ATTR_ERROR_TYPE], 'PLT_KFK_RESPONSE')
})

test('should allow customization via hooks', async t => {
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  let i = 0
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
          },
          // Tombstone
          {
            topic,
            key: 'test-key2'
          }
        ]
      })
    },
    {
      beforeProduce (_, span) {
        span.setAttribute('custom', (++i).toString())
      }
    }
  )

  const spans = traceExporter.getFinishedSpans()
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)

  deepStrictEqual(producerSpans.length, 2)
  deepStrictEqual(producerSpans[0].attributes.custom, '1')
  deepStrictEqual(producerSpans[1].attributes.custom, '2')
})
