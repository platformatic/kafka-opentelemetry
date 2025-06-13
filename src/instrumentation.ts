import { context, type Counter, type Histogram, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'
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
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_SYSTEM_VALUE_KAFKA
} from '@opentelemetry/semantic-conventions/incubating'
import { type GenericError, MultipleErrors, producerSendsChannel } from '@platformatic/kafka'
import { type TracingChannel, type TracingChannelSubscribers } from 'node:diagnostics_channel'
import {
  durationHistograms,
  filterAttribute as filter,
  metricClientOperationDurationName,
  metricConsumedMessagesName,
  metricProcessDurationName,
  metricSentMessagesName
} from './attributes.ts'
import { kInitialized } from './symbols.ts'
import { type Config, type SendContext } from './types.ts'
import { name, version } from './version.ts'

export class KafkaInstrumentation extends InstrumentationBase<Config> {
  [kInitialized]: boolean = false
  #sendSubscribers!: TracingChannelSubscribers<SendContext>
  #consumedMessagesHistogram!: Counter
  #sentMessagesHistogram!: Counter
  #clientDurationHistogram!: Histogram
  #processDurationHistogram!: Histogram

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
    // Subscribe to consumer events
    // TODO

    // Subscribe to producer events
    this.#sendSubscribers = {
      start: this.#onSendStart.bind(this),
      asyncStart: this.#onSendEnd.bind(this)
    } as TracingChannelSubscribers<SendContext>

    const producersChannel = producerSendsChannel as unknown as TracingChannel<{}, SendContext>
    producersChannel.subscribe(this.#sendSubscribers)
  }

  #disable (): void {
    const producersChannel = producerSendsChannel as unknown as TracingChannel<{}, SendContext>
    producersChannel.unsubscribe(this.#sendSubscribers)
  }

  #onSendStart (kafkaContext: SendContext): void {
    /* c8 ignore next - Else branch */
    const messages = kafkaContext.options.messages || []

    kafkaContext.spans = []
    for (const message of messages) {
      const attributes = {
        [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
        [ATTR_MESSAGING_OPERATION_NAME]: 'send',
        [ATTR_MESSAGING_DESTINATION_NAME]: message.topic,
        [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: filter(
          message.partition?.toString(),
          typeof message.partition === 'number'
        )
      }

      const span = this.tracer.startSpan(`send to ${message.topic}`, {
        kind: SpanKind.PRODUCER,
        attributes: {
          ...attributes,
          [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: filter<unknown, string>(message.key),
          [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]: filter(true, message.key && typeof message.value === 'undefined'),
          [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND
        }
      })

      message.headers ??= {}
      propagation.inject(trace.setSpan(context.active(), span), message.headers)
      kafkaContext.spans.push({ span, attributes })
    }
  }

  #onSendEnd (kafkaContext: SendContext): void {
    let error = kafkaContext.error as GenericError
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

    for (const { span, attributes } of kafkaContext.spans) {
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
