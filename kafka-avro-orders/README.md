# Kafka Avro Order Pipeline

A Kafka-based producer/consumer system for order messages, using Avro
serialization with Confluent Schema Registry. Built for the Big Data
"Chapter 3" assignment.

## Features (built incrementally — see commit history)

- [ ] Avro-serialized order messages (`orderId`, `product`, `price`)
- [ ] Producer publishing to a `orders` topic
- [ ] Consumer with manual offset commits
- [ ] Real-time running average of prices (global + per-product)
- [ ] Retry logic for transient failures (exponential backoff)
- [ ] Dead Letter Queue for permanently failed messages
- [ ] Live demo walkthrough

## Prerequisites

- Docker Desktop (Kafka, Schema Registry, Kafka UI run in containers)
- Java 21+
- Maven 3.9+

## Project layout

```
kafka-avro-orders/
├── docker-compose.yml       Kafka (KRaft) + Schema Registry + Kafka UI
├── scripts/                 topic creation helpers
├── pom.xml
└── src/main/
    ├── avro/order.avsc      Avro schema (source of truth)
    ├── java/com/bigdata/orders/   producer, consumer, aggregation, retry, DLQ
    └── resources/log4j2.xml
```

## Setup & run

_(filled in as each part is built — see below)_

---

This document is updated as the project grows; each part of the assignment
is committed separately.
