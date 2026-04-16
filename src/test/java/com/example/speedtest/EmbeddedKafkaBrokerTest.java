package com.example.speedtest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * Uses Spring Kafka's {@link EmbeddedKafkaBroker} directly (no Spring context).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmbeddedKafkaBrokerTest extends AbstractOrderProcessorTest {

    private EmbeddedKafkaBroker broker;

    @BeforeAll
    void startBroker() {
        broker = new EmbeddedKafkaBroker(1, false, 1, INPUT_TOPIC, OUTPUT_TOPIC);
        broker.afterPropertiesSet();
    }

    @AfterAll
    void stopBroker() {
        if (broker != null) {
            broker.destroy();
        }
    }

    @Override
    protected String getBootstrapServers() {
        return broker.getBrokersAsString();
    }
}
