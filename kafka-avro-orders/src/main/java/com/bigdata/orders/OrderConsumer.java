package com.bigdata.orders;

import com.bigdata.orders.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumes Avro-serialized order messages from the "orders" topic.
 *
 * <p>This is the basic version: it deserializes and prints each order, then
 * commits offsets manually once the batch has been handled. Running-average
 * aggregation, retry logic and DLQ handling are added in later parts.</p>
 *
 * <p>Usage: {@code mvn exec:java -Dexec.mainClass=com.bigdata.orders.OrderConsumer}</p>
 */
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    /** Print an aggregation summary after every this-many successfully processed orders. */
    private static final int SUMMARY_EVERY = 10;

    public static void main(String[] args) {
        Properties props = buildConsumerProperties();
        RunningAverage runningAverage = new RunningAverage();

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(Config.ORDERS_TOPIC));
            log.info("Consumer started. Subscribed to '{}', group='{}'. Waiting for messages...",
                    Config.ORDERS_TOPIC, Config.CONSUMER_GROUP_ID);

            Runtime.getRuntime().addShutdownHook(new Thread(() ->
                    log.info("Shutting down consumer...")));

            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, Order> record : records) {
                    Order order = record.value();
                    log.info("Received orderId={} product={} price={} (partition={}, offset={})",
                            order.getOrderId(), order.getProduct(), order.getPrice(),
                            record.partition(), record.offset());

                    // Only successfully processed orders count toward the average.
                    // (Failed/DLQ'd orders are excluded once that logic is added.)
                    runningAverage.add(order.getProduct(), order.getPrice());

                    if (runningAverage.globalCount() % SUMMARY_EVERY == 0) {
                        log.info("[AGG] {}", runningAverage.summary());
                    }
                }

                // Manual commit: only after the whole batch has been handled.
                // In later parts this moves to "after processed OR sent to DLQ".
                consumer.commitSync();
            }
        }
    }

    private static Properties buildConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.SCHEMA_REGISTRY_URL);

        // Deserialize into the generated specific Order class rather than a
        // GenericRecord.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // Manual commits: we control exactly when an offset is safe to commit
        // (after successful processing, or after a DLQ write in later parts).
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return props;
    }
}
