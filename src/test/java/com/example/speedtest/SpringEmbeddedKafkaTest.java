package com.example.speedtest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Uses Spring's {@code @EmbeddedKafka} annotation to manage the broker
 * lifecycle via the Spring test context.
 */
@SpringBootTest
@EmbeddedKafka(
        topics = {AbstractOrderProcessorTest.INPUT_TOPIC, AbstractOrderProcessorTest.OUTPUT_TOPIC},
        partitions = 1
)
class SpringEmbeddedKafkaTest extends AbstractOrderProcessorTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Override
    protected String getBootstrapServers() {
        return broker.getBrokersAsString();
    }
}
