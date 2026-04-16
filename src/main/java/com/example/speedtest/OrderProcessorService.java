package com.example.speedtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes order messages from an input topic, enriches them with a confirmation
 * status, and produces the result to an output topic. Uses raw Kafka clients so
 * it can be tested with any broker implementation without requiring Spring.
 */
public class OrderProcessorService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessorService.class);

    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private Thread processingThread;

    public OrderProcessorService(String bootstrapServers, String inputTopic, String outputTopic) {
        this.bootstrapServers = bootstrapServers;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public void start() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processor-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        consumer = new KafkaConsumer<>(consumerProps);
        producer = new KafkaProducer<>(producerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        running.set(true);
        processingThread = new Thread(this::processLoop, "order-processor");
        processingThread.setDaemon(true);
        processingThread.start();
        log.info("OrderProcessorService started, consuming from [{}], producing to [{}]", inputTopic, outputTopic);
    }

    public void stop() {
        running.set(false);
        if (processingThread != null) {
            processingThread.interrupt();
            try {
                processingThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(5));
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        log.info("OrderProcessorService stopped");
    }

    @Override
    public void close() {
        stop();
    }

    private void processLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String confirmation = processOrder(record.key(), record.value());
                        producer.send(new ProducerRecord<>(outputTopic, record.key(), confirmation)).get();
                    } catch (Exception e) {
                        log.error("Failed to process order with key [{}]", record.key(), e);
                    }
                }
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("Processing loop terminated unexpectedly", e);
            }
        }
    }

    String processOrder(String key, String orderJson) throws Exception {
        JsonNode order = objectMapper.readTree(orderJson);
        ObjectNode confirmation = objectMapper.createObjectNode();
        confirmation.put("orderId", order.has("orderId") ? order.get("orderId").asText() : key);
        confirmation.put("customer", order.get("customer").asText());
        confirmation.put("item", order.get("item").asText());
        confirmation.put("quantity", order.get("quantity").asInt());
        confirmation.put("status", "CONFIRMED");
        confirmation.put("processedAt", Instant.now().toString());
        return objectMapper.writeValueAsString(confirmation);
    }
}
