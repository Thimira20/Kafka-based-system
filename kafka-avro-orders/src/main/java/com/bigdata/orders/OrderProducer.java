package com.bigdata.orders;

import com.bigdata.orders.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Produces Avro-serialized order messages onto the "orders" topic.
 *
 * <p>This is the basic version: it generates plausible-looking orders and
 * sends them. Fault injection (flaky/invalid orders, for exercising retry
 * and DLQ logic) is added in a later part.</p>
 *
 * <p>Usage: {@code mvn exec:java -Dexec.mainClass=com.bigdata.orders.OrderProducer
 * -Dexec.args="--count 30 --delay-ms 500"}</p>
 */
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private static final String[] PRODUCTS = {
            "Item1", "Item2", "Item3", "Item4", "Item5"
    };

    public static void main(String[] args) throws InterruptedException {
        int count = getIntArg(args, "--count", 30);
        long delayMs = getIntArg(args, "--delay-ms", 300);

        Properties props = buildProducerProperties();

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            log.info("Starting producer: sending {} orders to topic '{}' (delay={}ms)",
                    count, Config.ORDERS_TOPIC, delayMs);

            for (int i = 1; i <= count; i++) {
                Order order = buildOrder(1000 + i);
                ProducerRecord<String, Order> record =
                        new ProducerRecord<>(Config.ORDERS_TOPIC, order.getOrderId(), order);

                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        log.error("Failed to send order {}", order.getOrderId(), exception);
                    } else {
                        log.info("Sent orderId={} product={} price={} -> partition={} offset={}",
                                order.getOrderId(), order.getProduct(), order.getPrice(),
                                metadata.partition(), metadata.offset());
                    }
                });

                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }
            }

            producer.flush();
            log.info("Done. Sent {} orders.", count);
        }
    }

    private static Order buildOrder(int orderIdNum) {
        String orderId = String.valueOf(orderIdNum);
        String product = PRODUCTS[ThreadLocalRandom.current().nextInt(PRODUCTS.length)];
        float price = roundToTwoDecimals(ThreadLocalRandom.current().nextFloat() * 499f + 1f);

        return Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();
    }

    private static float roundToTwoDecimals(float value) {
        return Math.round(value * 100f) / 100f;
    }

    private static Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.SCHEMA_REGISTRY_URL);

        // Stronger delivery guarantees; this is broker-level retry for transient
        // network issues, distinct from the application-level retry/DLQ logic
        // implemented in the consumer for business-rule failures.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return props;
    }

    private static int getIntArg(String[] args, String flag, int defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return Integer.parseInt(args[i + 1]);
            }
        }
        return defaultValue;
    }
}
