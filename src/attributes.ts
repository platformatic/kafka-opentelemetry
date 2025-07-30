export const metricConsumedMessagesName = 'messaging.client.consumed.messages'
export const metricSentMessagesName = 'messaging.client.sent.messages'
export const metricClientOperationDurationName = 'messaging.client.operation.duration'
export const metricProcessDurationName = 'messaging.process.duration'

// These are histogram buckets for the duration of operations in seconds.
// To facilitate transition, we chose to use the same in KafkaJS's OpenTelemetry instrumentation.
// See: https://github.com/open-telemetry/opentelemetry-js-contrib/blob/215c2b5a6f2706430e98375377cc091d3c99a2b1/packages/instrumentation-kafkajs/src/instrumentation.ts#L163
export const durationHistograms = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
