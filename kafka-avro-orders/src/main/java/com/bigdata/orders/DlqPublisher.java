package com.bigdata.orders;

import com.bigdata.orders.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Publishes permanently-failed order records to the Dead Letter Queue
 * ({@link Config#DLQ_TOPIC}).
 *
 * <p>The order payload is republished unchanged (still Avro, still readable
 * in Kafka UI / with a console consumer), with the failure context attached
 * as Kafka headers rather than folded into the payload — this keeps the DLQ
 * message schema-compatible with the original and keeps the diagnostic
 * metadata clearly separate from the business data.</p>
 *
 * <p>Sends are synchronous ({@code .get()}): the consumer must know the DLQ
 * write actually succeeded before it commits the original record's offset,
 * otherwise a failed order could be lost silently (neither processed nor
 * dead-lettered).</p>
 */
public class DlqPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final KafkaProducer<String, Order> producer;

    public DlqPublisher() {
        this.producer = new KafkaProducer<>(buildProducerProperties());
    }

    /**
     * Sends {@code order} to the DLQ, attaching headers describing why and
     * where it originally failed. Blocks until the broker acknowledges the
     * write.
     */
    public void send(ConsumerRecord<String, Order> originalRecord, Order order,
                      Exception cause, int attempts) {
        ProducerRecord<String, Order> dlqRecord =
                new ProducerRecord<>(Config.DLQ_TOPIC, order.getOrderId(), order);

        addHeader(dlqRecord, "x-error-class", cause.getClass().getSimpleName());
        addHeader(dlqRecord, "x-error-message", String.valueOf(cause.getMessage()));
        addHeader(dlqRecord, "x-original-topic", originalRecord.topic());
        addHeader(dlqRecord, "x-original-partition", String.valueOf(originalRecord.partition()));
        addHeader(dlqRecord, "x-original-offset", String.valueOf(originalRecord.offset()));
        addHeader(dlqRecord, "x-attempts", String.valueOf(attempts));
        addHeader(dlqRecord, "x-failed-at", Instant.now().toString());

        try {
            producer.send(dlqRecord).get(); // synchronous: must know it landed before we commit
            log.warn("Sent orderId={} to DLQ (topic={}) after {} attempt(s): {}: {}",
                    order.getOrderId(), Config.DLQ_TOPIC, attempts,
                    cause.getClass().getSimpleName(), cause.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while writing to DLQ for orderId=" + order.getOrderId(), e);
        } catch (ExecutionException e) {
            // If the DLQ write itself fails, we must not silently drop the
            // record - surface this loudly rather than committing the offset.
            throw new RuntimeException("Failed to write orderId=" + order.getOrderId() + " to DLQ", e);
        }
    }

    private static void addHeader(ProducerRecord<String, Order> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    private static Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    @Override
    public void close() {
        producer.close();
    }
}
