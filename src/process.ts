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

export function processWithTracing<T = GenericMessage> (
  payload: T,
  processor: Processor<T>,
  callback: Callback<void>
): void
export function processWithTracing<T = GenericMessage> (
  payload: T,
  processor: SyncProcessor<T> | AsyncProcessor<T>
): Promise<void>
export function processWithTracing<T = GenericMessage> (
  payload: T,
  processor: Processor<T>,
  callback?: Callback<void>
): void | Promise<void> {
  const ctx = createDiagnosticContext({ message: payload }) as unknown as ProcessContext
  consumerProcessesChannel.start.publish(ctx)

  const activeContext = ctx.activeContext ?? context.active()

  if (callback) {
    const cbProcessor = processor as CallbackProcessor<T>

    try {
      context.with(activeContext, () => {
        cbProcessor(payload, error => {
          if (error) {
            ctx.error = error
          }

          consumerProcessesChannel.asyncStart.publish(ctx)
          callback(error)
        })
      })
    } catch (error) {
      ctx.error = error as Error
      consumerProcessesChannel.asyncStart.publish(ctx)
      callback(error as Error)
    }

    return
  }

  try {
    const result = context.with(activeContext, () => {
      return (processor as SyncProcessor<T> | AsyncProcessor<T>)(payload)
    })

    if (typeof result?.then !== 'function') {
      consumerProcessesChannel.asyncStart.publish(ctx)
      return
    }

    return result.then(
      () => {
        consumerProcessesChannel.asyncStart.publish(ctx)
      },
      error => {
        ctx.error = error
        consumerProcessesChannel.asyncStart.publish(ctx)
        throw error
      }
    )
  } catch (error) {
    ctx.error = error
    consumerProcessesChannel.asyncStart.publish(ctx)
    throw error
  }
}
