# Kafka Streams Order Processor — Vibe Coding Experiment

This project was built entirely through **vibe coding** with [Claude Code](https://claude.ai/claude-code) and the [Confluent Agent Skills](https://github.com/confluentinc/agent-skills) plugin.

The goal was to explore how far AI-assisted development can go when combining Claude's coding capabilities with Confluent's domain-specific streaming skills — from architecture design to working, tested code in a single conversation.

## What Are Confluent Agent Skills?

[Confluent Agent Skills](https://github.com/confluentinc/agent-skills) are a collection of AI skills that provide guided assistance and code generation for building streaming applications. Once installed in Claude Code, the relevant skill activates automatically based on your request.

This project was generated using the **kafka-streams-programming** skill, which handles topology design, code generation with Schema Registry integration, and unit testing scaffolding.

Install in Claude Code:

```
/plugin marketplace add confluentinc/agent-skills
/plugin install streaming-skills-plugin@confluent-agent-skills
```

## What This App Does

A Kafka Streams application that processes a multi-event order lifecycle and emits an enriched aggregate **only when the order is closed**.

### Topology

```
customers topic ──> GlobalKTable (broadcast lookup)
                                                  ╲
orders topic ──> groupByKey ──> aggregate ──> filter(isClosed) ──> join ──> enriched-orders topic
    (ORDER_CREATED, ORDER_LINE_ADDED,                              ╱
     ORDER_LINE_REMOVED, ORDER_PAYED,
     ORDER_CLOSED)
```

### Event Types

| Event | Effect on Aggregate |
|-------|-------------------|
| `ORDER_CREATED` | Initializes a new order |
| `ORDER_LINE_ADDED` | Adds item, recalculates total |
| `ORDER_LINE_REMOVED` | Removes item by product name, recalculates total |
| `ORDER_PAYED` | Marks order as paid |
| `ORDER_CLOSED` | Marks order as closed — **triggers emission** |

The output on `enriched-orders` contains customer info (name, email, tier), the full list of line items, total amount, and item count.

## Prerequisites

- Java 17+
- Docker & Docker Compose

## Build

```bash
./gradlew build
```

## Run Tests

Unit tests use `TopologyTestDriver` with a mock Schema Registry (`mock://test-sr`) — no running Kafka needed.

```bash
./gradlew test
```

Six test scenarios are included:

| Test | Scenario |
|------|----------|
| `testFullOrderLifecycle` | Complete flow: create, add 2 items, remove 1, pay, close |
| `testNoOutputBeforeClose` | Verifies no output is emitted until `ORDER_CLOSED` |
| `testEmptyOrderClosed` | Closing an empty order emits 0 items / 0 total |
| `testMultipleLineItemsAggregation` | 3 items with correct total calculation |
| `testLineRemovedReducesTotal` | Item removal recalculates the total |
| `testIndependentOrders` | Two orders for the same customer, only the closed one emits |

## Run the Application

### 1. Start the local Confluent Platform environment

```bash
docker compose up -d
```

Wait for services to be healthy:

```bash
docker compose ps
curl -s http://localhost:8081/subjects | echo "Schema Registry is up"
```

### 2. Create topics

```bash
./create-topics.sh
```

### 3. Start the Streams app

```bash
./gradlew run
```

Watch for `State transition from REBALANCING to RUNNING` in the logs.

### 4. Open a consumer on the output topic

In a separate terminal, start consuming from `enriched-orders`:

```bash
docker exec schema-registry kafka-avro-console-consumer \
  --topic enriched-orders \
  --from-beginning \
  --bootstrap-server broker:29092 \
  --property schema.registry.url=http://localhost:8081 \
  --property print.key=true \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer
```

### 5. Seed a customer

```bash
docker exec -i schema-registry kafka-avro-console-producer \
  --topic customers \
  --bootstrap-server broker:29092 \
  --property schema.registry.url=http://localhost:8081 \
  --property parse.key=true \
  --property key.separator="|" \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.schema='{"type":"record","name":"Customer","namespace":"com.example.orderprocessor","fields":[{"name":"customer_id","type":"string"},{"name":"name","type":"string"},{"name":"email","type":"string"},{"name":"tier","type":{"type":"enum","name":"CustomerTier","symbols":["BRONZE","SILVER","GOLD","PLATINUM"],"default":"BRONZE"},"default":"BRONZE"}]}' <<EOF
cust-1|{"customer_id":"cust-1","name":"Alice Smith","email":"alice@example.com","tier":"GOLD"}
EOF
```

### 6. Simulate an order lifecycle

Send each event one at a time. **Nothing appears on the consumer until the final `ORDER_CLOSED`.**

```bash
docker exec -i schema-registry kafka-avro-console-producer \
  --topic orders \
  --bootstrap-server broker:29092 \
  --property schema.registry.url=http://localhost:8081 \
  --property parse.key=true \
  --property key.separator="|" \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.schema='{"type":"record","name":"OrderEvent","namespace":"com.example.orderprocessor","fields":[{"name":"order_id","type":"string"},{"name":"customer_id","type":"string"},{"name":"event_type","type":{"type":"enum","name":"OrderEventType","symbols":["ORDER_CREATED","ORDER_LINE_ADDED","ORDER_LINE_REMOVED","ORDER_PAYED","ORDER_CLOSED"],"default":"ORDER_CREATED"}},{"name":"product_name","type":["null","string"],"default":null},{"name":"quantity","type":["null","int"],"default":null},{"name":"unit_price","type":["null","double"],"default":null},{"name":"payment_method","type":["null","string"],"default":null},{"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}}]}' <<EOF
order-1|{"order_id":"order-1","customer_id":"cust-1","event_type":"ORDER_CREATED","product_name":null,"quantity":null,"unit_price":null,"payment_method":null,"timestamp":1715000000000}
order-1|{"order_id":"order-1","customer_id":"cust-1","event_type":"ORDER_LINE_ADDED","product_name":{"string":"Laptop"},"quantity":{"int":1},"unit_price":{"double":999.99},"payment_method":null,"timestamp":1715000001000}
order-1|{"order_id":"order-1","customer_id":"cust-1","event_type":"ORDER_LINE_ADDED","product_name":{"string":"Mouse"},"quantity":{"int":2},"unit_price":{"double":25.50},"payment_method":null,"timestamp":1715000002000}
order-1|{"order_id":"order-1","customer_id":"cust-1","event_type":"ORDER_LINE_ADDED","product_name":{"string":"USB-C Hub"},"quantity":{"int":1},"unit_price":{"double":45.00},"payment_method":null,"timestamp":1715000003000}
order-1|{"order_id":"order-1","customer_id":"cust-1","event_type":"ORDER_PAYED","product_name":null,"quantity":null,"unit_price":null,"payment_method":{"string":"CREDIT_CARD"},"timestamp":1715000004000}
order-1|{"order_id":"order-1","customer_id":"cust-1","event_type":"ORDER_CLOSED","product_name":null,"quantity":null,"unit_price":null,"payment_method":null,"timestamp":1715000005000}
EOF
```

After `ORDER_CLOSED`, the consumer should display the enriched aggregate with all three items and customer info:

```json
order-1  {"order_id":"order-1","customer_id":"cust-1","customer_name":"Alice Smith","customer_email":"alice@example.com","customer_tier":"GOLD","items":[{"product_name":"Laptop","quantity":1,"unit_price":999.99},{"product_name":"Mouse","quantity":2,"unit_price":25.5},{"product_name":"USB-C Hub","quantity":1,"unit_price":45.0}],"total_amount":1095.99,"item_count":3}
```

The aggregate contains: 1 Laptop (999.99) + 2 Mice (51.00) + 1 USB-C Hub (45.00) = **1095.99 total**, with customer info from the GlobalKTable joined in.

## Tear Down

```bash
# Stop the Streams app (Ctrl+C)
# Delete topics and local state
./teardown.sh
# Stop containers
docker compose down
```
