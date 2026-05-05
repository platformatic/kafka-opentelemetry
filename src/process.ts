import { context } from '@opentelemetry/api'
import { createDiagnosticContext, createTracingChannel } from '@platformatic/kafka'
import {
  type AsyncProcessor,
  type Callback,
  type CallbackProcessor,
  type GenericMessage,
  type ProcessContext,
  type Processor,
  type SyncProcessor
} from './types.ts'

export const consumerProcessesChannel = createTracingChannel<ProcessContext>('consumer:processes')

export function processWithTracing (message: GenericMessage, processor: Processor, callback: Callback<void>): void
export function processWithTracing (message: GenericMessage, processor: SyncProcessor | AsyncProcessor): Promise<void>
export function processWithTracing (
  message: GenericMessage,
  processor: Processor,
  callback?: Callback<void>
): void | Promise<void> {
  const ctx = createDiagnosticContext({ message }) as unknown as ProcessContext

  // The wrapping in the tracePromise is needed to allow throwing of sync functions
  if (callback) {
    return consumerProcessesChannel.traceCallback(
      (message, callback) => {
        /* c8 ignore next - Else branch */
        const activeContext = ctx.activeContext ?? context.active()
        return context.with(activeContext, () => {
          return (processor as CallbackProcessor)(message, callback)
        })
      },
      1,
      ctx,
      null,
      message,
      callback
    )
  }

  return consumerProcessesChannel.tracePromise(
    async message => {
      /* c8 ignore next - Else branch */
      const activeContext = ctx.activeContext ?? context.active()
      return context.with(activeContext, () => {
        return (processor as AsyncProcessor)(message)
      })
    },
    ctx,
    null,
    message
  )
}
