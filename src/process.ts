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
  return callback
    ? consumerProcessesChannel.traceCallback(processor as CallbackProcessor, 1, ctx, null, message, callback)
    : consumerProcessesChannel.tracePromise(async message => (processor as AsyncProcessor)(message), ctx, null, message)
}
