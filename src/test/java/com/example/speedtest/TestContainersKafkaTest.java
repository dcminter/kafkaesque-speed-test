package com.example.speedtest;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
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
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
                    .waitingFor(Wait.forListeningPort());

    @Override
    protected String getBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
