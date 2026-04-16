package com.example.speedtest;

import eu.kafkaesque.core.KafkaesqueServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Explicitly creates and manages a {@link KafkaesqueServer} instance
 * without relying on JUnit 4/5 integration annotations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaesqueServerExplicitTest extends AbstractOrderProcessorTest {

    private KafkaesqueServer server;

    @BeforeAll
    void startServer() throws Exception {
        server = new KafkaesqueServer("localhost", 0);
        server.start();
    }

    @AfterAll
    void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Override
    protected String getBootstrapServers() {
        try {
            return server.getBootstrapServers();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
