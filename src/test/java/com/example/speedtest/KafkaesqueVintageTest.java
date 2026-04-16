package com.example.speedtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.kafkaesque.junit4.KafkaesqueRule;
import eu.kafkaesque.junit4.TopicDefinition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JUnit 4 test using {@link KafkaesqueRule}, executed via the JUnit Vintage engine.
 * This cannot extend the abstract JUnit 5 base class, so it contains its own
 * equivalent test methods.
 */
public class KafkaesqueVintageTest {

    private static final String INPUT_TOPIC = "orders";
    private static final String OUTPUT_TOPIC = "order-confirmations";

    @ClassRule
    public static KafkaesqueRule kafkaesqueRule = KafkaesqueRule.builder()
            .topics(
                    new TopicDefinition(INPUT_TOPIC),
                    new TopicDefinition(OUTPUT_TOPIC))
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void singleOrderIsProcessed() throws Exception {
        String bootstrapServers = kafkaesqueRule.getBootstrapServers();
        String key = "ORD-" + UUID.randomUUID();

        try (OrderProcessorService service = new OrderProcessorService(bootstrapServers, INPUT_TOPIC, OUTPUT_TOPIC)) {
            service.start();

            try (KafkaProducer<String, String> producer = createProducer(bootstrapServers);
                 KafkaConsumer<String, String> consumer = createConsumer(bootstrapServers)) {

                String orderJson = "{\"orderId\":\"" + key + "\",\"customer\":\"Alice\",\"item\":\"Kafka In Action\",\"quantity\":2}";
                producer.send(new ProducerRecord<>(INPUT_TOPIC, key, orderJson)).get();

                List<ConsumerRecord<String, String>> results =
                        pollUntilKeysSeen(consumer, Collections.singleton(key), Duration.ofSeconds(15));

                assertThat(results).hasSize(1);
                JsonNode confirmation = objectMapper.readTree(results.get(0).value());
                assertThat(confirmation.get("orderId").asText()).isEqualTo(key);
                assertThat(confirmation.get("customer").asText()).isEqualTo("Alice");
                assertThat(confirmation.get("status").asText()).isEqualTo("CONFIRMED");
            }
        }
    }

    @Test
    public void multipleOrdersAreProcessed() throws Exception {
        String bootstrapServers = kafkaesqueRule.getBootstrapServers();
        String batchId = UUID.randomUUID().toString().substring(0, 8);
        Set<String> expectedKeys = new LinkedHashSet<>();

        try (OrderProcessorService service = new OrderProcessorService(bootstrapServers, INPUT_TOPIC, OUTPUT_TOPIC)) {
            service.start();

            try (KafkaProducer<String, String> producer = createProducer(bootstrapServers);
                 KafkaConsumer<String, String> consumer = createConsumer(bootstrapServers)) {

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
                }
            }
        }
    }

    @Test
    public void messageKeyIsPreserved() throws Exception {
        String bootstrapServers = kafkaesqueRule.getBootstrapServers();
        String key = "key-preserve-" + UUID.randomUUID();

        try (OrderProcessorService service = new OrderProcessorService(bootstrapServers, INPUT_TOPIC, OUTPUT_TOPIC)) {
            service.start();

            try (KafkaProducer<String, String> producer = createProducer(bootstrapServers);
                 KafkaConsumer<String, String> consumer = createConsumer(bootstrapServers)) {

                String orderJson = "{\"orderId\":\"KEY-TEST\",\"customer\":\"Bob\",\"item\":\"Test Item\",\"quantity\":1}";
                producer.send(new ProducerRecord<>(INPUT_TOPIC, key, orderJson)).get();

                List<ConsumerRecord<String, String>> results =
                        pollUntilKeysSeen(consumer, Collections.singleton(key), Duration.ofSeconds(15));

                assertThat(results).hasSize(1);
                assertThat(results.get(0).key()).isEqualTo(key);
            }
        }
    }

    private KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));
        return consumer;
    }

    private List<ConsumerRecord<String, String>> pollUntilKeysSeen(
            KafkaConsumer<String, String> consumer, Set<String> expectedKeys, Duration timeout) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        Set<String> remaining = new HashSet<>(expectedKeys);
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
