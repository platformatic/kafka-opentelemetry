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
  MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
  MESSAGING_SYSTEM_VALUE_KAFKA
} from '@opentelemetry/semantic-conventions/incubating'
import { AuthenticationError, MessagesStreamModes, stringDeserializers, stringSerializers } from '@platformatic/kafka'
import { deepStrictEqual, ok, rejects } from 'node:assert'
import { test } from 'node:test'
import { metricConsumedMessagesName } from '../src/attributes.ts'
import { processWithTracing } from '../src/process.ts'
import { name, version } from '../src/version.ts'
import { createConsumer, createProducer, createTopic, runWithTracing } from './helpers.ts'

test('should trace each consumed message (sync function)', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await producer.send({
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
        {
          topic,
          key: 'test-key2',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        },
        // Tombstone
        {
          topic,
          key: 'test-key3',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })

    let i = 0

    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })
    for await (const message of stream) {
      await processWithTracing(message, () => {})

      if (i++ === 2) {
        break
      }
    }
  })

  const spans = traceExporter.getFinishedSpans()
  const rootSpan = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  ok(rootSpan)
  deepStrictEqual(producerSpans.length, 3)
  deepStrictEqual(consumerSpans.length, 3)

  deepStrictEqual(consumerSpans[0].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[0].spanContext().traceId, producerSpans[0].spanContext().traceId)
  deepStrictEqual(consumerSpans[0].parentSpanContext!.spanId, producerSpans[0].spanContext().spanId)
  deepStrictEqual(consumerSpans[0].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS
  })

  deepStrictEqual(consumerSpans[1].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[1].spanContext().traceId, producerSpans[1].spanContext().traceId)
  deepStrictEqual(consumerSpans[1].parentSpanContext!.spanId, producerSpans[1].spanContext().spanId)
  deepStrictEqual(consumerSpans[1].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS
  })

  deepStrictEqual(consumerSpans[2].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[2].spanContext().traceId, producerSpans[2].spanContext().traceId)
  deepStrictEqual(consumerSpans[2].parentSpanContext!.spanId, producerSpans[2].spanContext().spanId)
  deepStrictEqual(consumerSpans[2].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key3',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const consumedMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricConsumedMessagesName)

  deepStrictEqual(consumedMessages?.dataPoints.length, 1)
  deepStrictEqual(consumedMessages?.dataPoints[0].value, 3)
  deepStrictEqual(consumedMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0'
  })
})

test('should trace each consumed message (resolved promise)', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await producer.send({
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
        {
          topic,
          key: 'test-key2',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        },
        // Tombstone
        {
          topic,
          key: 'test-key3',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })

    let i = 0

    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })
    for await (const message of stream) {
      await processWithTracing(message, async () => {})

      if (i++ === 2) {
        break
      }
    }
  })

  const spans = traceExporter.getFinishedSpans()
  const rootSpan = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  ok(rootSpan)
  deepStrictEqual(producerSpans.length, 3)
  deepStrictEqual(consumerSpans.length, 3)

  deepStrictEqual(consumerSpans[0].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[0].spanContext().traceId, producerSpans[0].spanContext().traceId)
  deepStrictEqual(consumerSpans[0].parentSpanContext!.spanId, producerSpans[0].spanContext().spanId)
  deepStrictEqual(consumerSpans[0].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS
  })

  deepStrictEqual(consumerSpans[1].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[1].spanContext().traceId, producerSpans[1].spanContext().traceId)
  deepStrictEqual(consumerSpans[1].parentSpanContext!.spanId, producerSpans[1].spanContext().spanId)
  deepStrictEqual(consumerSpans[1].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS
  })

  deepStrictEqual(consumerSpans[2].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[2].spanContext().traceId, producerSpans[2].spanContext().traceId)
  deepStrictEqual(consumerSpans[2].parentSpanContext!.spanId, producerSpans[2].spanContext().spanId)
  deepStrictEqual(consumerSpans[2].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key3',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const consumedMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricConsumedMessagesName)

  deepStrictEqual(consumedMessages?.dataPoints.length, 1)
  deepStrictEqual(consumedMessages?.dataPoints[0].value, 3)
  deepStrictEqual(consumedMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0'
  })
})

test('should trace each consumed message (callback)', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await producer.send({
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
        {
          topic,
          key: 'test-key2',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        },
        // Tombstone
        {
          topic,
          key: 'test-key3',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })

    let i = 0

    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })

    stream.on('data', async message => {
      processWithTracing(
        message,
        (_, callback) => {
          callback()
        },
        () => {}
      )

      if (i++ === 2) {
        stream.destroy()
      }
    })
  })

  const spans = traceExporter.getFinishedSpans()
  const rootSpan = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  ok(rootSpan)
  deepStrictEqual(producerSpans.length, 3)
  deepStrictEqual(consumerSpans.length, 3)

  deepStrictEqual(consumerSpans[0].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[0].spanContext().traceId, producerSpans[0].spanContext().traceId)
  deepStrictEqual(consumerSpans[0].parentSpanContext!.spanId, producerSpans[0].spanContext().spanId)
  deepStrictEqual(consumerSpans[0].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS
  })

  deepStrictEqual(consumerSpans[1].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[1].spanContext().traceId, producerSpans[1].spanContext().traceId)
  deepStrictEqual(consumerSpans[1].parentSpanContext!.spanId, producerSpans[1].spanContext().spanId)
  deepStrictEqual(consumerSpans[1].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS
  })

  deepStrictEqual(consumerSpans[2].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[2].spanContext().traceId, producerSpans[2].spanContext().traceId)
  deepStrictEqual(consumerSpans[2].parentSpanContext!.spanId, producerSpans[2].spanContext().spanId)
  deepStrictEqual(consumerSpans[2].status.code, SpanStatusCode.OK)
  deepStrictEqual(consumerSpans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key3',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const consumedMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricConsumedMessagesName)

  deepStrictEqual(consumedMessages?.dataPoints.length, 1)
  deepStrictEqual(consumedMessages?.dataPoints[0].value, 3)
  deepStrictEqual(consumedMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0'
  })
})

test('should trace each failed processing (sync function)', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await producer.send({
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
        {
          topic,
          key: 'test-key2',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        },
        // Tombstone
        {
          topic,
          key: 'test-key3',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })

    let i = 0

    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })
    for await (const message of stream) {
      await rejects(() => {
        return processWithTracing(message, () => {
          throw i === 0 ? new AuthenticationError('KABOOM!') : new Error('KABOOM!')
        })
      })

      if (i++ === 2) {
        break
      }
    }
  })

  const spans = traceExporter.getFinishedSpans()
  const rootSpan = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  ok(rootSpan)
  deepStrictEqual(producerSpans.length, 3)
  deepStrictEqual(consumerSpans.length, 3)

  deepStrictEqual(consumerSpans[0].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[0].spanContext().traceId, producerSpans[0].spanContext().traceId)
  deepStrictEqual(consumerSpans[0].parentSpanContext!.spanId, producerSpans[0].spanContext().spanId)
  deepStrictEqual(consumerSpans[0].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_AUTHENTICATION'
  })

  deepStrictEqual(consumerSpans[1].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[1].spanContext().traceId, producerSpans[1].spanContext().traceId)
  deepStrictEqual(consumerSpans[1].parentSpanContext!.spanId, producerSpans[1].spanContext().spanId)
  deepStrictEqual(consumerSpans[1].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })

  deepStrictEqual(consumerSpans[2].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[2].spanContext().traceId, producerSpans[2].spanContext().traceId)
  deepStrictEqual(consumerSpans[2].parentSpanContext!.spanId, producerSpans[2].spanContext().spanId)
  deepStrictEqual(consumerSpans[2].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key3',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const consumedMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricConsumedMessagesName)

  deepStrictEqual(consumedMessages?.dataPoints.length, 2)
  deepStrictEqual(consumedMessages?.dataPoints[0].value, 1)
  deepStrictEqual(consumedMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_AUTHENTICATION'
  })
  deepStrictEqual(consumedMessages?.dataPoints[1].value, 2)
  deepStrictEqual(consumedMessages?.dataPoints[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })
})

test('should trace each failed processing (rejected promise)', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await producer.send({
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
        {
          topic,
          key: 'test-key2',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        },
        // Tombstone
        {
          topic,
          key: 'test-key3',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })

    let i = 0

    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })
    for await (const message of stream) {
      await rejects(() => {
        return processWithTracing(message, async () => {
          throw i === 0 ? new AuthenticationError('KABOOM!') : new Error('KABOOM!')
        })
      })

      if (i++ === 2) {
        break
      }
    }
  })

  const spans = traceExporter.getFinishedSpans()
  const rootSpan = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  ok(rootSpan)
  deepStrictEqual(producerSpans.length, 3)
  deepStrictEqual(consumerSpans.length, 3)

  deepStrictEqual(consumerSpans[0].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[0].spanContext().traceId, producerSpans[0].spanContext().traceId)
  deepStrictEqual(consumerSpans[0].parentSpanContext!.spanId, producerSpans[0].spanContext().spanId)
  deepStrictEqual(consumerSpans[0].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_AUTHENTICATION'
  })

  deepStrictEqual(consumerSpans[1].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[1].spanContext().traceId, producerSpans[1].spanContext().traceId)
  deepStrictEqual(consumerSpans[1].parentSpanContext!.spanId, producerSpans[1].spanContext().spanId)
  deepStrictEqual(consumerSpans[1].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })

  deepStrictEqual(consumerSpans[2].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[2].spanContext().traceId, producerSpans[2].spanContext().traceId)
  deepStrictEqual(consumerSpans[2].parentSpanContext!.spanId, producerSpans[2].spanContext().spanId)
  deepStrictEqual(consumerSpans[2].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key3',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const consumedMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricConsumedMessagesName)

  deepStrictEqual(consumedMessages?.dataPoints.length, 2)
  deepStrictEqual(consumedMessages?.dataPoints[0].value, 1)
  deepStrictEqual(consumedMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_AUTHENTICATION'
  })
  deepStrictEqual(consumedMessages?.dataPoints[1].value, 2)
  deepStrictEqual(consumedMessages?.dataPoints[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })
})

test('should trace each failed processing (callback with error)', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  const { traceExporter, metricsExporter } = await runWithTracing(t, async () => {
    await producer.send({
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
        {
          topic,
          key: 'test-key2',
          value: 'test-value',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        },
        // Tombstone
        {
          topic,
          key: 'test-key3',
          headers: {
            'test-header': 'test-header-value'
          },
          partition: 0
        }
      ]
    })

    let i = 0

    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })

    stream.on('data', async message => {
      processWithTracing(
        message,
        (_, callback) => {
          callback(i === 0 ? new AuthenticationError('KABOOM!') : new Error('KABOOM!'))
        },
        () => {}
      )

      if (i++ === 2) {
        stream.destroy()
      }
    })
  })

  const spans = traceExporter.getFinishedSpans()
  const rootSpan = spans.find(span => !span.parentSpanContext)
  const producerSpans = spans.filter(span => span.kind === SpanKind.PRODUCER)
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  ok(rootSpan)
  deepStrictEqual(producerSpans.length, 3)
  deepStrictEqual(consumerSpans.length, 3)

  deepStrictEqual(consumerSpans[0].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[0].spanContext().traceId, producerSpans[0].spanContext().traceId)
  deepStrictEqual(consumerSpans[0].parentSpanContext!.spanId, producerSpans[0].spanContext().spanId)
  deepStrictEqual(consumerSpans[0].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key1',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_AUTHENTICATION'
  })

  deepStrictEqual(consumerSpans[1].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[1].spanContext().traceId, producerSpans[1].spanContext().traceId)
  deepStrictEqual(consumerSpans[1].parentSpanContext!.spanId, producerSpans[1].spanContext().spanId)
  deepStrictEqual(consumerSpans[1].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key2',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })

  deepStrictEqual(consumerSpans[2].kind, SpanKind.CONSUMER)
  deepStrictEqual(consumerSpans[2].spanContext().traceId, producerSpans[2].spanContext().traceId)
  deepStrictEqual(consumerSpans[2].parentSpanContext!.spanId, producerSpans[2].spanContext().spanId)
  deepStrictEqual(consumerSpans[2].status.code, SpanStatusCode.ERROR)
  deepStrictEqual(consumerSpans[2].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: 'test-key3',
    [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: 'true',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })

  const metrics = metricsExporter.getMetrics()

  const kafkaMetrics = metrics[0].scopeMetrics.find(
    scope => scope.scope.name === name && scope.scope.version === version
  )
  const consumedMessages = kafkaMetrics?.metrics.find(metric => metric.descriptor.name === metricConsumedMessagesName)

  deepStrictEqual(consumedMessages?.dataPoints.length, 2)
  deepStrictEqual(consumedMessages?.dataPoints[0].value, 1)
  deepStrictEqual(consumedMessages?.dataPoints[0].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_AUTHENTICATION'
  })
  deepStrictEqual(consumedMessages?.dataPoints[1].value, 2)
  deepStrictEqual(consumedMessages?.dataPoints[1].attributes, {
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: '0',
    [ATTR_ERROR_TYPE]: 'PLT_KFK_USER'
  })
})

test('should allow customization via hooks', async t => {
  const consumer = await createConsumer(t, { deserializers: stringDeserializers })
  const producer = await createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  let j = 0
  const { traceExporter } = await runWithTracing(
    t,
    async () => {
      await producer.send({
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
          {
            topic,
            key: 'test-key2',
            value: 'test-value',
            headers: {
              'test-header': 'test-header-value'
            },
            partition: 0
          },
          // Tombstone
          {
            topic,
            key: 'test-key3',
            headers: {
              'test-header': 'test-header-value'
            },
            partition: 0
          }
        ]
      })

      let i = 0

      const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxWaitTime: 100 })
      for await (const message of stream) {
        await processWithTracing(message, () => {})

        if (i++ === 2) {
          break
        }
      }
    },
    {
      beforeProcess (_, span) {
        span.setAttribute('custom', (++j).toString())
      }
    }
  )

  const spans = traceExporter.getFinishedSpans()
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER)

  deepStrictEqual(consumerSpans.length, 3)
  deepStrictEqual(consumerSpans[0].attributes.custom, '1')
  deepStrictEqual(consumerSpans[1].attributes.custom, '2')
  deepStrictEqual(consumerSpans[2].attributes.custom, '3')
})
