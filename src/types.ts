import { type Attributes, type Span } from '@opentelemetry/api'
import { type InstrumentationConfig } from '@opentelemetry/instrumentation'
import {
  type ClientDiagnosticEvent,
  type ConnectionDiagnosticEvent,
  type Consumer,
  type DiagnosticContext,
  type GenericError,
  type Message,
  type MessageToProduce,
  type Producer,
  type SendOptions
} from '@platformatic/kafka'
import { type kInitialized } from './symbols.ts'

export type GenericMessage = Message<unknown, unknown, unknown, unknown>

export type Callback<ReturnType> = (error?: Error | null, payload?: ReturnType) => void

export type SyncProcessor = (message: GenericMessage) => void
export type CallbackProcessor = (message: GenericMessage, callback: Callback<void>) => void
export type AsyncProcessor = (message: GenericMessage) => Promise<void>
export type Processor = SyncProcessor | CallbackProcessor | AsyncProcessor

export type KeySerializer = (key: unknown) => string
export type Hook = (message: MessageToProduce<unknown, unknown, unknown, unknown> | GenericMessage, span: Span) => void

export interface ProcessContextProperties {
  message: GenericMessage
  startTime: bigint
  span: Span
  attributes: Attributes
}

export interface ApiContextProperties {
  apiKey: number
  startTime: bigint | undefined
}

export interface SendContextProperties {
  error?: GenericError
  options: SendOptions<unknown, unknown, unknown, unknown>
  spans: Span[]
  attributes: Attributes[]
}

export type ApiContext = ConnectionDiagnosticEvent<ApiContextProperties>
export type SendContext = ClientDiagnosticEvent<Producer, SendContextProperties>
export type ProcessContext = DiagnosticContext<ClientDiagnosticEvent<Consumer, ProcessContextProperties>>

export interface Config extends InstrumentationConfig {
  [kInitialized]?: boolean // This is set at runtime and not meant to be configured by the user

  producedKeySerializer?: (key: unknown) => string
  consumedKeySerializer?: (key: unknown) => string

  beforeProduce?: Hook
  beforeProcess?: Hook
}
