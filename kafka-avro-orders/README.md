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

### 1. Start the infrastructure

```powershell
docker compose up -d
```

This brings up:

| Service | Address | Purpose |
|---|---|---|
| Kafka broker (KRaft, no ZooKeeper) | `localhost:9092` | message broker |
| Schema Registry | `localhost:8081` | stores `order.avsc`, checks compatibility |
| Kafka UI | `localhost:8080` | browse topics/messages in the browser |

Wait for all three containers to report healthy:

```powershell
docker compose ps
```

### 2. Create the topics

```powershell
.\scripts\create-topics.ps1
```

(Bash equivalent: `./scripts/create-topics.sh`, for WSL/macOS/Linux.)

This creates:

- `orders` — 3 partitions, the main topic
- `orders.DLQ` — 1 partition, dead letter queue for permanently failed messages

Topic auto-creation is disabled on the broker so these must be created explicitly.

### 3. Avro schema

The order message schema lives at [`src/main/avro/order.avsc`](src/main/avro/order.avsc):

```json
{
  "orderId": "string",
  "product": "string",
  "price":   "float"
}
```

The `avro-maven-plugin` generates a typed `com.bigdata.orders.avro.Order` class
from this file during `mvn generate-sources` / `mvn compile`. The generated
code is **not** committed to Git — it's build output, produced fresh into
`target/generated-sources/avro` every build. To generate it without a full
build:

```powershell
mvn generate-sources
```

### 4. Run the producer

Sends Avro-serialized orders to the `orders` topic (random product + price,
no faults yet — fault injection is added in a later part).

```powershell
mvn compile exec:java "-Dexec.mainClass=com.bigdata.orders.OrderProducer" "-Dexec.args=--count 30 --delay-ms 300"
```

Arguments (both optional):

| Flag | Default | Meaning |
|---|---|---|
| `--count` | 30 | number of orders to send |
| `--delay-ms` | 300 | pause between sends, for a readable live demo |

Each order is keyed by `orderId`, so retries/reprocessing of the same order
always land on the same partition (ordering is preserved per order).

To verify it worked:

- Check `http://localhost:8081/subjects` — should list `orders-value` after
  the first send (the producer auto-registers the schema).
- Open Kafka UI (`http://localhost:8080`) → Topics → `orders` → Messages, to
  see the Avro-decoded orders.

### 5. Run the consumer

In a second terminal, while the producer is running (or after it's finished —
the consumer starts from the earliest offset on first run):

```powershell
mvn compile exec:java "-Dexec.mainClass=com.bigdata.orders.OrderConsumer"
```

This deserializes each Avro order and logs it. Offsets are committed
**manually** (`enable.auto.commit=false`) once a poll batch has been handled —
never before — so that a crash mid-processing doesn't silently skip or drop
records. (Running-average aggregation and DLQ handling are added in later
parts; right now every record is just printed.)

Stop it with `Ctrl+C`. Restarting it resumes from the last committed offset
(no re-processing of already-committed records, no gaps).

### 6. Real-time aggregation

The consumer now tracks a **running average of prices** as it processes
orders — both a global average and one per product — via
[`RunningAverage`](src/main/java/com/bigdata/orders/RunningAverage.java).
It's incremental (keeps only a running `count`/`sum` per key, not the whole
history) so it stays O(1) per message regardless of how many orders have
been processed.

Only **successfully processed** orders count toward the average — a message
that ends up in the DLQ (added in a later part) must never skew it.

Every 10 successfully processed orders, the consumer logs a summary line:

```
[AGG] processed=30 globalAvg=248.37 | Item1 avg=251.02 (n=6) | Item2 avg=239.88 (n=7) ...
```

No setup changes needed — just re-run the consumer from part 5, it now logs
this automatically.

### 7. (later parts) Retry logic, DLQ

_(filled in as those parts are built)_

---

This document is updated as the project grows; each part of the assignment
is committed separately.
