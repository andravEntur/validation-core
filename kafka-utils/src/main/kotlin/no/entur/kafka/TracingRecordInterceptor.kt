package no.entur.kafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.MDC
import org.springframework.kafka.listener.RecordInterceptor

/**
 * Puts the consumed record's correlation id and sender's client name in the MDC while the listener
 * processes it, and removes them afterwards. A record without a correlation id gets
 * [missingCorrelationId]'s: [NOT_SET] by default, a fresh id in a service that mints its own.
 *
 * Meant for [org.springframework.kafka.config.AbstractKafkaListenerContainerFactory.setRecordInterceptor].
 */
class TracingRecordInterceptor<K : Any, V : Any>(
    private val missingCorrelationId: () -> String = { NOT_SET },
) : RecordInterceptor<K, V> {
    override fun intercept(
        record: ConsumerRecord<K, V>,
        consumer: Consumer<K, V>,
    ): ConsumerRecord<K, V> {
        MDC.put(CORRELATION_ID_MDC_KEY, record.headers().correlationId() ?: missingCorrelationId())
        MDC.put(CLIENT_NAME_MDC_KEY, record.headers().clientName() ?: NOT_SET)
        return record
    }

    override fun afterRecord(
        record: ConsumerRecord<K, V>,
        consumer: Consumer<K, V>,
    ) {
        MDC.remove(CORRELATION_ID_MDC_KEY)
        MDC.remove(CLIENT_NAME_MDC_KEY)
    }
}
