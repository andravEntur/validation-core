package no.entur.kafka.deadletter

import com.google.protobuf.InvalidProtocolBufferException
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.SerializationException
import org.springframework.kafka.support.converter.ConversionException
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.messaging.converter.MessageConversionException
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException
import java.io.EOFException
import java.io.IOException

/**
 * Whether a listener failure is a property of the record, so it fails again however often it is
 * redelivered ([PERMANENT]), or of the moment ([TRANSIENT]). Anything unrecognised is transient: a
 * wrong guess that way stalls the partition, visible as consumer lag, instead of dropping a record.
 *
 * Verdicts are [PERMANENT], [TRANSIENT] or null for none. [overrides] hold whatever the exception
 * wraps, and the outermost one wins; otherwise the innermost cause with a [causes] verdict wins,
 * since the outer frames only say where the failure surfaced. Both are asked before the shared ones.
 */
class FailureClassifier(
    private val overrides: (Throwable) -> Boolean? = { null },
    private val causes: (Throwable) -> Boolean? = { null },
) {
    fun isPermanent(exception: Throwable): Boolean {
        val chain =
            generateSequence(exception) { current -> current.cause?.takeIf { it !== current } }
                .take(MAX_CAUSE_DEPTH)
                .toList()
        return chain.firstNotNullOfOrNull { overrides(it) ?: sharedOverride(it) }
            ?: chain.mapNotNull { causes(it) ?: sharedCause(it) }.lastOrNull()
            ?: TRANSIENT
    }

    companion object {
        const val PERMANENT = true
        const val TRANSIENT = false
        private const val MAX_CAUSE_DEPTH = 20

        /** Confluent's error codes for a subject or schema id the registry does not know. */
        private val SCHEMA_NOT_FOUND = setOf(40401, 40403)

        private fun sharedOverride(exception: Throwable): Boolean? =
            when (exception) {
                // Spring's default fatal exceptions, which the error handler no longer short-circuits:
                // a record the listener method cannot be invoked with.
                is MessageConversionException, is ConversionException, is MethodArgumentResolutionException,
                is NoSuchMethodException, is ClassCastException,
                -> PERMANENT

                else -> null
            }

        private fun sharedCause(exception: Throwable): Boolean? =
            when (exception) {
                // Also what a registry that could not be asked surfaces as, so a cause below usually decides.
                is DeserializationException -> PERMANENT

                // A wrapper the Confluent deserializers put around both bad bytes and registry I/O.
                is SerializationException -> null

                // Ahead of IOException, which both extend.
                is EOFException -> PERMANENT

                is InvalidProtocolBufferException -> PERMANENT

                // The record's schema id is not in the registry: a real one answers 40403, the mock 40401.
                is RestClientException if exception.errorCode in SCHEMA_NOT_FOUND -> PERMANENT

                is RestClientException -> TRANSIENT

                is KafkaException -> TRANSIENT

                is IOException -> TRANSIENT

                else -> null
            }
    }
}
