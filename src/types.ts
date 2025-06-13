import { type Attributes, type Span } from '@opentelemetry/api'
import { type InstrumentationConfig } from '@opentelemetry/instrumentation'
import { type GenericError, type ClientDiagnosticEvent, type Producer, type SendOptions } from '@platformatic/kafka'
import { type kInitialized } from './symbols.ts'

export interface SendContextSpan {
  span: Span
  attributes: Attributes
}

export interface SendContextContent {
  error?: GenericError
  options: SendOptions<unknown, unknown, unknown, unknown>
  spans: SendContextSpan[]
}

export type SendContext = ClientDiagnosticEvent<Producer, SendContextContent>

export interface Config extends InstrumentationConfig {
  [kInitialized]?: boolean // This is set at runtime and not meant to be configured by the user

  // TODO: Add beforeProduce and beforeConsume hooks
}
