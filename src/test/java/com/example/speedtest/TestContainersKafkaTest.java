package com.example.speedtest;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Uses Testcontainers to spin up a real Kafka broker in Docker.
 */
@Testcontainers
class TestContainersKafkaTest extends AbstractOrderProcessorTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Override
    protected String getBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
