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
import { deepStrictEqual, rejects } from 'node:assert'
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

  deepStrictEqual(spans.length, 3)

  deepStrictEqual(spans[1].kind, SpanKind.PRODUCER)
  deepStrictEqual(spans[1].spanContext().traceId, spans[0].spanContext().traceId)
  deepStrictEqual(spans[1].parentSpanContext!.spanId, spans[0].spanContext().spanId)
  deepStrictEqual(spans[1].status.code, SpanStatusCode.OK)
  deepStrictEqual(spans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND
  })

  deepStrictEqual(spans[2].kind, SpanKind.PRODUCER)
  deepStrictEqual(spans[2].spanContext().traceId, spans[0].spanContext().traceId)
  deepStrictEqual(spans[2].parentSpanContext!.spanId, spans[0].spanContext().spanId)
  deepStrictEqual(spans[2].status.code, SpanStatusCode.OK)
  deepStrictEqual(spans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: 'kafka',
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: true
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const sentMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricSentMessagesName)

  deepStrictEqual(sentMessages?.dataPoints.length, 2)
  deepStrictEqual(sentMessages?.dataPoints[0].value, 1)
  deepStrictEqual(sentMessages?.dataPoints[1].value, 1)
})

test('should trace each failed produce', async t => {
  const producer = await createProducer(t, { serializers: stringSerializers, retries: 1, retryDelay: 100 })

  // Note that the topic is not created, so the produce will fail
  const topic = await createTopic(t, false)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await rejects(() =>
      producer.send({
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
    )
  })

  const spans = traceExporter.getFinishedSpans()

  deepStrictEqual(spans.length, 3)

  deepStrictEqual(spans[1].kind, SpanKind.PRODUCER)
  deepStrictEqual(spans[1].spanContext().traceId, spans[0].spanContext().traceId)
  deepStrictEqual(spans[1].parentSpanContext!.spanId, spans[0].spanContext().spanId)
  deepStrictEqual(spans[1].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(spans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_RESPONSE'
  })

  deepStrictEqual(spans[2].kind, SpanKind.PRODUCER)
  deepStrictEqual(spans[2].spanContext().traceId, spans[0].spanContext().traceId)
  deepStrictEqual(spans[2].parentSpanContext!.spanId, spans[0].spanContext().spanId)
  deepStrictEqual(spans[2].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(spans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: 'kafka',
    [ATTR_MESSAGING_OPERATION_NAME]: 'send',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: true,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_RESPONSE'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const sentMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricSentMessagesName)

  deepStrictEqual(sentMessages?.dataPoints.length, 2)
  deepStrictEqual(sentMessages?.dataPoints[0].value, 1)
  deepStrictEqual(sentMessages?.dataPoints[1].value, 1)
})
