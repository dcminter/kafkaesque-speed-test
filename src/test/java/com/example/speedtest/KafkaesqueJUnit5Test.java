package com.example.speedtest;

import eu.kafkaesque.junit5.Kafkaesque;
import eu.kafkaesque.junit5.KafkaesqueTopic;

/**
 * Uses the Kafkaesque JUnit 5 extension via the {@code @Kafkaesque} annotation.
 * The extension creates an in-process mock Kafka broker and makes a
 * KafkaesqueServer instance available via JUnit 5 parameter injection.
 * <p>
 * We capture the server's bootstrap address in a {@code @BeforeEach} method
 * (which runs before each inherited test method) so that
 * {@link #getBootstrapServers()} can return it.
 */
@Kafkaesque(topics = {
        @KafkaesqueTopic(name = AbstractOrderProcessorTest.INPUT_TOPIC),
        @KafkaesqueTopic(name = AbstractOrderProcessorTest.OUTPUT_TOPIC)
})
class KafkaesqueJUnit5Test extends AbstractOrderProcessorTest {

    private String bootstrapServers;

    @org.junit.jupiter.api.BeforeEach
    void captureBootstrapServers(eu.kafkaesque.core.KafkaesqueServer server) throws Exception {
        this.bootstrapServers = server.getBootstrapServers();
    }

    @Override
    protected String getBootstrapServers() {
        return bootstrapServers;
    }
}
