package com.example.speedtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract integration test that exercises an {@link OrderProcessorService}
 * against whatever Kafka broker the concrete subclass provides.
 * <p>
 * Subclasses must implement {@link #getBootstrapServers()} and ensure
 * that the topics {@value INPUT_TOPIC} and {@value OUTPUT_TOPIC} exist
 * before tests run.
 * <p>
 * Each test method uses unique message keys so that tests are isolated even
 * when sharing a broker that retains records across methods.
 */
abstract class AbstractOrderProcessorTest {

    protected static final String INPUT_TOPIC = "orders";
    protected static final String OUTPUT_TOPIC = "order-confirmations";

    private final ObjectMapper objectMapper = new ObjectMapper();

    protected abstract String getBootstrapServers();

    @Test
    void singleOrderIsProcessed() throws Exception {
        String key = "ORD-" + UUID.randomUUID();
        try (OrderProcessorService service = createAndStartService();
             KafkaProducer<String, String> producer = createProducer();
             KafkaConsumer<String, String> consumer = createConsumer()) {

            String orderJson = "{\"orderId\":\"" + key + "\",\"customer\":\"Alice\",\"item\":\"Kafka In Action\",\"quantity\":2}";
            producer.send(new ProducerRecord<>(INPUT_TOPIC, key, orderJson)).get();

            List<ConsumerRecord<String, String>> results =
                    pollUntilKeysSeen(consumer, Collections.singleton(key), Duration.ofSeconds(15));

            assertThat(results).hasSize(1);
            JsonNode confirmation = objectMapper.readTree(results.get(0).value());
            assertThat(confirmation.get("orderId").asText()).isEqualTo(key);
            assertThat(confirmation.get("customer").asText()).isEqualTo("Alice");
            assertThat(confirmation.get("item").asText()).isEqualTo("Kafka In Action");
            assertThat(confirmation.get("quantity").asInt()).isEqualTo(2);
            assertThat(confirmation.get("status").asText()).isEqualTo("CONFIRMED");
            assertThat(confirmation.has("processedAt")).isTrue();
        }
    }

    @Test
    void multipleOrdersAreProcessed() throws Exception {
        String batchId = UUID.randomUUID().toString().substring(0, 8);
        Set<String> expectedKeys = new java.util.LinkedHashSet<>();

        try (OrderProcessorService service = createAndStartService();
             KafkaProducer<String, String> producer = createProducer();
             KafkaConsumer<String, String> consumer = createConsumer()) {

            for (int i = 1; i <= 5; i++) {
                String key = "BATCH-" + batchId + "-" + i;
                expectedKeys.add(key);
                String orderJson = String.format(
                        "{\"orderId\":\"%s\",\"customer\":\"Customer%d\",\"item\":\"Item%d\",\"quantity\":%d}",
                        key, i, i, i);
                producer.send(new ProducerRecord<>(INPUT_TOPIC, key, orderJson)).get();
            }

            List<ConsumerRecord<String, String>> results =
                    pollUntilKeysSeen(consumer, expectedKeys, Duration.ofSeconds(15));

            assertThat(results).hasSize(5);
            for (ConsumerRecord<String, String> record : results) {
                JsonNode confirmation = objectMapper.readTree(record.value());
                assertThat(confirmation.get("status").asText()).isEqualTo("CONFIRMED");
                assertThat(confirmation.has("processedAt")).isTrue();
            }
        }
    }

    @Test
    void messageKeyIsPreserved() throws Exception {
        String key = "key-preserve-" + UUID.randomUUID();
        try (OrderProcessorService service = createAndStartService();
             KafkaProducer<String, String> producer = createProducer();
             KafkaConsumer<String, String> consumer = createConsumer()) {

            String orderJson = "{\"orderId\":\"KEY-TEST\",\"customer\":\"Bob\",\"item\":\"Test Item\",\"quantity\":1}";
            producer.send(new ProducerRecord<>(INPUT_TOPIC, key, orderJson)).get();

            List<ConsumerRecord<String, String>> results =
                    pollUntilKeysSeen(consumer, Collections.singleton(key), Duration.ofSeconds(15));

            assertThat(results).hasSize(1);
            assertThat(results.get(0).key()).isEqualTo(key);
        }
    }

    // --- helpers ---

    protected OrderProcessorService createAndStartService() {
        OrderProcessorService service = new OrderProcessorService(getBootstrapServers(), INPUT_TOPIC, OUTPUT_TOPIC);
        service.start();
        return service;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));
        return consumer;
    }

    /**
     * Polls until all expected keys have been seen, returning only records
     * whose keys are in the expected set. This provides test isolation when
     * the broker retains records across test methods.
     */
    private List<ConsumerRecord<String, String>> pollUntilKeysSeen(
            KafkaConsumer<String, String> consumer, Set<String> expectedKeys, Duration timeout) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        Set<String> remaining = new java.util.HashSet<>(expectedKeys);
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (!remaining.isEmpty() && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                if (expectedKeys.contains(record.key())) {
                    matched.add(record);
                    remaining.remove(record.key());
                }
            }
        }
        return matched;
    }
}
