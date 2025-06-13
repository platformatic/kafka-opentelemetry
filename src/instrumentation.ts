import {
  type Context,
  context,
  type Counter,
  type Histogram,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace
} from '@opentelemetry/api'
import { InstrumentationBase } from '@opentelemetry/instrumentation'
import {
  ATTR_ERROR_TYPE,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY,
  ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_SYSTEM,
  ATTR_SERVER_ADDRESS,
  ATTR_SERVER_PORT,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_SYSTEM_VALUE_KAFKA
} from '@opentelemetry/semantic-conventions/incubating'
import {
  connectionsApiChannel,
  type GenericError,
  MultipleErrors,
  producerSendsChannel,
  protocolAPIsById,
  UserError
} from '@platformatic/kafka'
import { type TracingChannel, type TracingChannelSubscribers } from 'node:diagnostics_channel'
import {
  durationHistograms,
  metricClientOperationDurationName,
  metricConsumedMessagesName,
  metricProcessDurationName,
  metricSentMessagesName
} from './attributes.ts'
import { consumerProcessesChannel } from './process.ts'
import { kInitialized } from './symbols.ts'
import {
  type ApiContext,
  type Config,
  type Hook,
  type KeySerializer,
  type ProcessContext,
  type SendContext
} from './types.ts'
import { name, version } from './version.ts'

export const textMapGetter = {
  /* c8 ignore next 3 - Needed by opentelemetry-js interface */
  keys (carrier: Map<string, string>): string[] {
    return Array.from(carrier.keys())
  },
  get (carrier: Map<string, string>, key: string): undefined | string {
    return carrier.get(key)?.toString()
  }
}

export function defaultKeySerialzier (key: unknown): string {
  return (key as any).toString()
}

function noop () {}

export class KafkaInstrumentation extends InstrumentationBase<Config> {
  [kInitialized]: boolean = false
  #subscriptions: Record<string, TracingChannelSubscribers<object>>
  #consumedMessagesHistogram!: Counter
  #sentMessagesHistogram!: Counter
  #clientDurationHistogram!: Histogram
  #processDurationHistogram!: Histogram
  #producedKeySerializer: KeySerializer
  #consumedKeySerializer: KeySerializer
  #beforeProduce: Hook
  #beforeProcess: Hook

  /*
    IMPORTANT

    The InstrumentationBase and InstrumentationAbstract constructors internally
    call both enable and _updateMetricInstruments.
    Since it is done before the constructor ends, private fields are not yet initialized and available.

    To avoid this, we protect private fields access in enable, disable, and _updateMetricInstruments methods.
    Then we reinvoke enable and _updateMetricInstruments in the constructor if needed.
  */
  constructor (config: Config = {}) {
    super(name, version, config)
    this[kInitialized] = true
    this.#subscriptions = {}
    this.#producedKeySerializer = config.producedKeySerializer || defaultKeySerialzier
    this.#consumedKeySerializer = config.consumedKeySerializer || defaultKeySerialzier
    this.#beforeProduce = typeof config.beforeProduce === 'function' ? config.beforeProduce : noop
    this.#beforeProcess = typeof config.beforeProcess === 'function' ? config.beforeProcess : noop

    if (this._config.enabled) {
      this.#enable()
    }

    this._updateMetricInstruments()
  }

  override _updateMetricInstruments (): void {
    if (!this[kInitialized]) {
      return
    }

    this.#consumedMessagesHistogram = this.meter.createCounter(metricConsumedMessagesName)
    this.#sentMessagesHistogram = this.meter.createCounter(metricSentMessagesName)
    this.#clientDurationHistogram = this.meter.createHistogram(metricClientOperationDurationName, {
      advice: { explicitBucketBoundaries: durationHistograms }
    })
    this.#processDurationHistogram = this.meter.createHistogram(metricProcessDurationName, {
      advice: { explicitBucketBoundaries: durationHistograms }
    })
  }

  // No-op as we dont't need to patch the module
  override init (): void {}

  override enable (): void {
    if (!this[kInitialized]) {
      return
    }

    this.#enable()
  }

  override disable (): void {
    if (!this[kInitialized]) {
      return
    }

    this.#disable()
  }

  #enable (): void {
    // Subscribe to API calls
    this.#subscribe('api', connectionsApiChannel, {
      start: this.#onApiRequestStart.bind(this),
      asyncStart: this.#onApiRequestEnd.bind(this),
      error: this.#onApiRequestEnd.bind(this)
    })

    // Subscribe to consumer events
    this.#subscribe('consumer', consumerProcessesChannel, {
      start: this.#onProcessingStart.bind(this),
      asyncStart: this.#onProcessingEnd.bind(this)
    })

    // Subscribe to producer events
    this.#subscribe('producer', producerSendsChannel, {
      start: this.#onSendStart.bind(this),
      asyncStart: this.#onSendEnd.bind(this)
    })
  }

  #disable (): void {
    this.#unsubscribe('api', connectionsApiChannel)
    this.#unsubscribe('consumer', consumerProcessesChannel)
    this.#unsubscribe('producer', producerSendsChannel)
  }

  #subscribe<T extends object> (
    id: string,
    channel: TracingChannel,
    subscribers: Partial<TracingChannelSubscribers<T>>
  ): void {
    this.#subscriptions[id] = subscribers as TracingChannelSubscribers<object>
    channel.subscribe(this.#subscriptions[id] as TracingChannelSubscribers<object>)
  }

  #unsubscribe (id: string, channel: TracingChannel): void {
    channel.unsubscribe(this.#subscriptions[id] as TracingChannelSubscribers<object>)
  }

  #onApiRequestStart (ctx: ApiContext): void {
    ctx.startTime = process.hrtime.bigint()
  }

  #onApiRequestEnd (ctx: ApiContext): void {
    // Since the API call can fail in both the sync and async phases, the error channel can publish with our without
    // a publish from asyncStart. This was we avoid duplication.
    if (typeof ctx.startTime !== 'bigint') {
      return
    }

    const duration = Number(process.hrtime.bigint() - ctx.startTime) / 1e9
    ctx.startTime = undefined

    this.#clientDurationHistogram.record(duration, {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_OPERATION_NAME]: protocolAPIsById[ctx.apiKey],
      [ATTR_SERVER_ADDRESS]: ctx.connection.host,
      [ATTR_SERVER_PORT]: ctx.connection.port?.toString()
    })
  }

  #onProcessingStart (ctx: ProcessContext): void {
    const { topic, partition, key, value, headers } = ctx.message
    const propagatedContext: Context = propagation.extract(ROOT_CONTEXT, headers, textMapGetter)

    ctx.attributes = {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_OPERATION_NAME]: 'process',
      [ATTR_MESSAGING_DESTINATION_NAME]: ctx.message.topic,
      [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partition.toString()
    }

    const spanAttributes: Record<string, string> = {
      ...ctx.attributes,
      [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND
    }

    if (typeof key !== 'undefined') {
      spanAttributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] = this.#consumedKeySerializer(key)

      /* c8 ignore next - Else branch */
      if (value == null || value === '' || (Buffer.isBuffer(value) && value.length === 0)) {
        spanAttributes[ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE] = 'true'
      }
    }

    ctx.span = this.tracer.startSpan(
      `process message from ${topic}`,
      { kind: SpanKind.CONSUMER, attributes: spanAttributes },
      propagatedContext
    )

    ctx.startTime = process.hrtime.bigint()

    this.#beforeProcess?.(ctx.message, ctx.span)
  }

  #onProcessingEnd (ctx: ProcessContext): void {
    const duration = Number(process.hrtime.bigint() - ctx.startTime) / 1e9

    const error = ctx.error as GenericError
    let errorAttributes = {}
    let errorType
    let errorMessage

    const span = ctx.span

    if (error) {
      errorType = error.code ?? UserError.code
      errorMessage = error.message
      errorAttributes = { [ATTR_ERROR_TYPE]: errorType }
      span.setAttribute(ATTR_ERROR_TYPE, errorType!)
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage })
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }

    span.end()

    this.#processDurationHistogram.record(duration, {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_OPERATION_NAME]: 'process',
      [ATTR_ERROR_TYPE]: errorType
    })

    this.#consumedMessagesHistogram!.add(1, { ...ctx.attributes, ...errorAttributes })
  }

  #onSendStart (ctx: SendContext): void {
    ctx.spans = []
    ctx.attributes = []

    for (const message of ctx.options.messages) {
      const attributes: Record<string, string> = {
        [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
        [ATTR_MESSAGING_OPERATION_NAME]: 'send',
        [ATTR_MESSAGING_DESTINATION_NAME]: message.topic
      }

      if (typeof message.partition === 'number') {
        attributes[ATTR_MESSAGING_DESTINATION_PARTITION_ID] = message.partition.toString()
      }

      const spanAttributes: Record<string, string> = {
        ...attributes,
        [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND
      }

      if (typeof message.key !== 'undefined') {
        spanAttributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] = this.#producedKeySerializer(message.key)

        // == null to match null and undefined
        if (message.value == null) {
          spanAttributes[ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE] = 'true'
        }
      }

      const span = this.tracer.startSpan(`send to ${message.topic}`, {
        kind: SpanKind.PRODUCER,
        attributes: spanAttributes
      })

      message.headers ??= {}
      this.#beforeProduce?.(message, span)

      propagation.inject(trace.setSpan(context.active(), span), message.headers)

      ctx.spans.push(span)
      ctx.attributes.push(attributes)
    }
  }

  #onSendEnd (ctx: SendContext): void {
    let error = ctx.error as GenericError
    let errorAttributes = {}
    let errorType
    let errorMessage

    if (error) {
      // Find the inner error in case of multiple errors
      if (MultipleErrors.isMultipleErrors(error)) {
        error = error.errors[0] as GenericError
      }

      errorType = error.code
      errorMessage = error.message
      errorAttributes = { [ATTR_ERROR_TYPE]: errorType }
    }

    for (let i = 0; i < ctx.spans.length; i++) {
      const span = ctx.spans[i]
      const attributes = ctx.attributes[i]

      if (error) {
        span.setAttribute(ATTR_ERROR_TYPE, errorType!)
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: errorMessage
        })
      } else {
        span.setStatus({ code: SpanStatusCode.OK })
      }

      this.#sentMessagesHistogram!.add(1, { ...attributes, ...errorAttributes })

      span.end()
    }
  }
}
