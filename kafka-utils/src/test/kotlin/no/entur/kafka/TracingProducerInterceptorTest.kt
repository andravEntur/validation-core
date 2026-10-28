package no.entur.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.MDC

class TracingProducerInterceptorTest {
    private val interceptor = TracingProducerInterceptor<String, String>("ishum")

    @AfterEach
    fun clearMdc() = MDC.clear()

    private fun ProducerRecord<String, String>.values(key: String) =
        headers().filter { it.key().equals(key, ignoreCase = true) }.map { it.value().decodeToString() }

    @Test
    fun `stamps the client name, replacing the original sender's in any casing`() {
        val record = ProducerRecord("topic", "key", "value").apply { headers().add("et-client-name", "orch".toByteArray()) }

        interceptor.onSend(record)

        assertEquals(listOf("ishum"), record.values(CLIENT_NAME_HEADER))
    }

    @Test
    fun `keeps a correlation id set explicitly`() {
        MDC.put(CORRELATION_ID_MDC_KEY, "from-mdc")
        val record = ProducerRecord("topic", "key", "value").apply { headers().add(CORRELATION_ID_HEADER, "explicit".toByteArray()) }

        interceptor.onSend(record)

        assertEquals(listOf("explicit"), record.values(CORRELATION_ID_HEADER))
    }

    @Test
    fun `sends a correlation id set in another casing under the canonical header name`() {
        val record = ProducerRecord("topic", "key", "value").apply { headers().add("x-correlation-id", "lowercase".toByteArray()) }

        interceptor.onSend(record)

        assertEquals(listOf("lowercase"), record.values(CORRELATION_ID_HEADER))
        assertEquals(
            "lowercase",
            record
                .headers()
                .lastHeader(CORRELATION_ID_HEADER)
                ?.value()
                ?.decodeToString(),
        )
    }

    @Test
    fun `takes the correlation id of the record being processed`() {
        MDC.put(CORRELATION_ID_MDC_KEY, "from-mdc")
        val record = ProducerRecord("topic", "key", "value")

        interceptor.onSend(record)

        assertEquals(listOf("from-mdc"), record.values(CORRELATION_ID_HEADER))
    }

    @Test
    fun `falls back to not set`() {
        val record = ProducerRecord("topic", "key", "value")

        interceptor.onSend(record)

        assertEquals(listOf(NOT_SET), record.values(CORRELATION_ID_HEADER))
    }

    @Test
    fun `replaces an explicit not set with the one being processed`() {
        MDC.put(CORRELATION_ID_MDC_KEY, "from-mdc")
        val record = ProducerRecord("topic", "key", "value").apply { headers().add(CORRELATION_ID_HEADER, NOT_SET.toByteArray()) }

        interceptor.onSend(record)

        assertEquals(listOf("from-mdc"), record.values(CORRELATION_ID_HEADER))
    }
}
