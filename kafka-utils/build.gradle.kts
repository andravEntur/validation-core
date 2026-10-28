description = "Kafka tracing support (X-Correlation-Id/ET-Client-Name headers and the matching MDC " +
    "entries) shared by validation-platform services."

plugins {
    alias(libs.plugins.ktlint)
}

dependencies {
    // Scope application BOMs to application/test classpaths, not tool configurations like ktlint -
    // same reasoning as proto-utils.
    implementation(platform(libs.spring.boot.bom))
    implementation(platform(libs.kotlin.bom))

    // RecordInterceptor and Kafka's Headers/ProducerInterceptor appear in this module's public signatures.
    api("org.springframework.kafka:spring-kafka")
    api("org.slf4j:slf4j-api")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    // A real MDC adapter: without one slf4j falls back to a no-op MDC and the tests see nothing.
    testRuntimeOnly("ch.qos.logback:logback-classic")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// The Boot BOM above is `implementation`, so without this the published api variant lists
// spring-kafka/slf4j-api versionless, and a Gradle consumer without that BOM can't resolve them.
publishing {
    publications.named<MavenPublication>("maven") {
        versionMapping { allVariants { fromResolutionResult() } }
    }
}

ktlint {
    version.set(libs.versions.ktlint)
}
