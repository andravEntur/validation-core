package no.entur.kafka

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

/**
 * Stamps every record with [CLIENT_NAME_HEADER] = [clientName], replacing any the record already
 * carries (a dead-lettered record copies its original sender's). A record without a
 * [CORRELATION_ID_HEADER] gets the one being processed on this thread (see [TracingRecordInterceptor]),
 * or [NOT_SET]; one set explicitly is kept. Either way it is sent under the canonical header name,
 * since Kafka header lookups are case-sensitive.
 *
 * Meant for [org.springframework.kafka.core.KafkaTemplate.setProducerInterceptor], which runs it on
 * the sending thread, so the MDC it reads is the caller's.
 */
class TracingProducerInterceptor<K, V>(
    private val clientName: String,
) : ProducerInterceptor<K, V> {
    override fun onSend(record: ProducerRecord<K, V>): ProducerRecord<K, V> {
        val headers = record.headers()
        headers.replace(CLIENT_NAME_HEADER, clientName)
        headers.replace(CORRELATION_ID_HEADER, headers.correlationId() ?: currentCorrelationId() ?: NOT_SET)
        return record
    }

    override fun onAcknowledgement(
        metadata: RecordMetadata?,
        exception: Exception?,
    ) = Unit

    override fun close() = Unit

    override fun configure(configs: Map<String, *>) = Unit
}
