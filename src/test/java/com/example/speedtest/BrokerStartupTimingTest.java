package com.example.speedtest;

import eu.kafkaesque.core.KafkaesqueServer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures raw broker startup and shutdown time for each implementation.
 * Each test method creates, starts, verifies, and stops a broker, printing
 * timing results to stdout.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BrokerStartupTimingTest {
    final static Logger LOGGER = LoggerFactory.getLogger(BrokerStartupTimingTest.class);

    private static final String[] TOPICS = {"orders", "order-confirmations"};

    @Test
    @Order(1)
    void kafkaesqueServer() throws Exception {
        LOGGER.info("Starting kafkaesque server (warmed up logger)");
        long startNanos = System.nanoTime();
        KafkaesqueServer server = new KafkaesqueServer("localhost", 0);
        server.start();
        long startedNanos = System.nanoTime();

        String bootstrap = server.getBootstrapServers();
        assertThat(bootstrap).isNotEmpty();

        server.close();
        long stoppedNanos = System.nanoTime();

        reportTiming("KafkaesqueServer", startNanos, startedNanos, stoppedNanos);
    }

    @Test
    @Order(2)
    void embeddedKafkaBroker() {
        LOGGER.info("Starting embedded kafka (warmed up logger)");
        long startNanos = System.nanoTime();
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, false, 1, TOPICS);
        broker.afterPropertiesSet();
        long startedNanos = System.nanoTime();

        String bootstrap = broker.getBrokersAsString();
        assertThat(bootstrap).isNotEmpty();

        broker.destroy();
        long stoppedNanos = System.nanoTime();

        reportTiming("EmbeddedKafkaBroker", startNanos, startedNanos, stoppedNanos);
    }

    @Test
    @Order(3)
    void testContainersKafka() {
        LOGGER.info("Starting testcontainers kafka (warmed up logger)");
        long startNanos = System.nanoTime();
        final DockerImageName image = DockerImageName.parse("apache/kafka-native:3.8.0");
        KafkaContainer kafka = new KafkaContainer(image).waitingFor(Wait.forListeningPort());
        kafka.start();
        long startedNanos = System.nanoTime();

        String bootstrap = kafka.getBootstrapServers();
        assertThat(bootstrap).isNotEmpty();

        kafka.stop();
        long stoppedNanos = System.nanoTime();

        reportTiming("TestContainers Kafka", startNanos, startedNanos, stoppedNanos);
    }

    @Test
    @Order(4)
    void testContainersKafkaesque() {
        LOGGER.info("Starting testcontainers kafkaesque (warmed up logger)");
        long startNanos = System.nanoTime();
        GenericContainer<?> kafkaesque = new GenericContainer<>(DockerImageName.parse("ghcr.io/dcminter/kafkaesque:latest"))
                .withExposedPorts(9092)
                .waitingFor(Wait.forListeningPort());
        kafkaesque.start();
        long startedNanos = System.nanoTime();

        String bootstrap = kafkaesque.getHost() + ":" + kafkaesque.getMappedPort(9092);
        assertThat(bootstrap).isNotEmpty();

        kafkaesque.stop();
        long stoppedNanos = System.nanoTime();

        reportTiming("TestContainers Kafkaesque", startNanos, startedNanos, stoppedNanos);
    }

    private void reportTiming(String name, long startNanos, long startedNanos, long stoppedNanos) {
        long startupMs = (startedNanos - startNanos) / 1_000_000;
        long shutdownMs = (stoppedNanos - startedNanos) / 1_000_000;
        System.out.println();
        System.out.printf("[TIMING] %-25s startup: %6d ms   shutdown: %6d ms%n", name, startupMs, shutdownMs);
        System.out.println();
    }
}
