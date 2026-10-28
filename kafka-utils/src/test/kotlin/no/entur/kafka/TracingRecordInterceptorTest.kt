package no.entur.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.slf4j.MDC

class TracingRecordInterceptorTest {
    private val consumer = MockConsumer<String, String>("earliest")

    @AfterEach
    fun clearMdc() = MDC.clear()

    private fun record(vararg headers: Pair<String, String>): ConsumerRecord<String, String> {
        val record = ConsumerRecord("topic", 0, 0L, "key", "value")
        headers.forEach { (k, v) -> record.headers().add(k, v.toByteArray()) }
        return record
    }

    @Test
    fun `puts the record's correlation id and sender in the MDC for the record's duration`() {
        val interceptor = TracingRecordInterceptor<String, String>()
        val record = record(CORRELATION_ID_HEADER to "abc", "et-client-name" to "orch")

        interceptor.intercept(record, consumer)
        assertEquals("abc", MDC.get(CORRELATION_ID_MDC_KEY))
        assertEquals("orch", MDC.get(CLIENT_NAME_MDC_KEY))

        interceptor.afterRecord(record, consumer)
        assertNull(MDC.get(CORRELATION_ID_MDC_KEY))
        assertNull(MDC.get(CLIENT_NAME_MDC_KEY))
    }

    @Test
    fun `uses not set for a record without headers by default`() {
        TracingRecordInterceptor<String, String>().intercept(record(), consumer)

        assertEquals(NOT_SET, MDC.get(CORRELATION_ID_MDC_KEY))
        assertEquals(NOT_SET, MDC.get(CLIENT_NAME_MDC_KEY))
        assertNull(currentCorrelationId())
    }

    @Test
    fun `mints a correlation id for a record without one, or with not set`() {
        val interceptor = TracingRecordInterceptor<String, String> { "minted" }

        interceptor.intercept(record(CORRELATION_ID_HEADER to NOT_SET), consumer)

        assertEquals("minted", currentCorrelationId())
    }

    @Test
    fun `reads headers in any casing, the last one winning`() {
        val headers = RecordHeaders().add("x-correlation-id", "first".toByteArray()).add(CORRELATION_ID_HEADER, "second".toByteArray())

        assertEquals("second", headers.correlationId())
    }
}
