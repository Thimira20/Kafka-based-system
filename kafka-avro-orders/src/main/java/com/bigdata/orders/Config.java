package com.bigdata.orders;

/**
 * Shared constants for brokers, topics and the schema registry.
 * Centralised here so producer/consumer/DLQ code all agree on names.
 */
public final class Config {

    private Config() {
        // constants holder, not instantiable
    }

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static final String ORDERS_TOPIC = "orders";
    public static final String DLQ_TOPIC = "orders.DLQ";

    public static final String CONSUMER_GROUP_ID = "orders-consumer-group";
}
