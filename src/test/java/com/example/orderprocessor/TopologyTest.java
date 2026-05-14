package com.example.orderprocessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Customer> customersTopic;
    private TestInputTopic<String, OrderEvent> ordersTopic;
    private TestOutputTopic<String, EnrichedOrder> enrichedOrdersTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put("schema.registry.url", "mock://test-sr");
        props.put("statestore.cache.max.bytes", "0");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());

        Topology topology = TopologyBuilder.build(props);
        driver = new TopologyTestDriver(topology, props);

        Map<String, String> srConfig = Map.of("schema.registry.url", "mock://test-sr");

        SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(srConfig, false);

        SpecificAvroSerde<OrderEvent> orderEventSerde = new SpecificAvroSerde<>();
        orderEventSerde.configure(srConfig, false);

        SpecificAvroSerde<EnrichedOrder> enrichedOrderSerde = new SpecificAvroSerde<>();
        enrichedOrderSerde.configure(srConfig, false);

        customersTopic = driver.createInputTopic(
                TopologyBuilder.CUSTOMERS_TOPIC,
                new StringSerializer(),
                customerSerde.serializer());

        ordersTopic = driver.createInputTopic(
                TopologyBuilder.ORDERS_TOPIC,
                new StringSerializer(),
                orderEventSerde.serializer());

        enrichedOrdersTopic = driver.createOutputTopic(
                TopologyBuilder.ENRICHED_ORDERS_TOPIC,
                new StringDeserializer(),
                enrichedOrderSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        if (driver != null) driver.close();
    }

    @Test
    void testFullOrderLifecycle() {
        seedCustomer("cust-1", "Alice Smith", "alice@example.com", CustomerTier.GOLD);

        String orderId = "order-1";
        pipeOrderEvent(orderId, "cust-1", OrderEventType.ORDER_CREATED);
        pipeLineAdded(orderId, "cust-1", "Laptop", 1, 999.99);
        pipeLineAdded(orderId, "cust-1", "Mouse", 2, 25.50);
        pipeLineRemoved(orderId, "cust-1", "Mouse");
        pipeOrderEvent(orderId, "cust-1", OrderEventType.ORDER_PAYED);

        assertTrue(enrichedOrdersTopic.isEmpty(), "No output before OrderClosed");

        pipeOrderEvent(orderId, "cust-1", OrderEventType.ORDER_CLOSED);

        assertFalse(enrichedOrdersTopic.isEmpty());
        KeyValue<String, EnrichedOrder> result = enrichedOrdersTopic.readKeyValue();

        assertEquals(orderId, result.key);
        EnrichedOrder enriched = result.value;
        assertEquals("order-1", enriched.getOrderId());
        assertEquals("cust-1", enriched.getCustomerId());
        assertEquals("Alice Smith", enriched.getCustomerName());
        assertEquals("alice@example.com", enriched.getCustomerEmail());
        assertEquals("GOLD", enriched.getCustomerTier());
        assertEquals(1, enriched.getItems().size());
        assertEquals("Laptop", enriched.getItems().get(0).getProductName());
        assertEquals(999.99, enriched.getTotalAmount(), 0.001);
        assertEquals(1, enriched.getItemCount());
    }

    @Test
    void testNoOutputBeforeClose() {
        seedCustomer("cust-2", "Bob Jones", "bob@example.com", CustomerTier.SILVER);

        pipeOrderEvent("order-2", "cust-2", OrderEventType.ORDER_CREATED);
        pipeLineAdded("order-2", "cust-2", "Keyboard", 1, 75.00);
        pipeOrderEvent("order-2", "cust-2", OrderEventType.ORDER_PAYED);

        assertTrue(enrichedOrdersTopic.isEmpty(), "Should not emit until OrderClosed");
    }

    @Test
    void testEmptyOrderClosed() {
        seedCustomer("cust-3", "Carol White", "carol@example.com", CustomerTier.BRONZE);

        pipeOrderEvent("order-3", "cust-3", OrderEventType.ORDER_CREATED);
        pipeOrderEvent("order-3", "cust-3", OrderEventType.ORDER_CLOSED);

        assertFalse(enrichedOrdersTopic.isEmpty());
        EnrichedOrder enriched = enrichedOrdersTopic.readKeyValue().value;

        assertEquals(0, enriched.getItems().size());
        assertEquals(0.0, enriched.getTotalAmount(), 0.001);
        assertEquals(0, enriched.getItemCount());
        assertEquals("Carol White", enriched.getCustomerName());
    }

    @Test
    void testMultipleLineItemsAggregation() {
        seedCustomer("cust-4", "Dave Brown", "dave@example.com", CustomerTier.PLATINUM);

        String orderId = "order-4";
        pipeOrderEvent(orderId, "cust-4", OrderEventType.ORDER_CREATED);
        pipeLineAdded(orderId, "cust-4", "Monitor", 1, 450.00);
        pipeLineAdded(orderId, "cust-4", "Cable", 3, 12.99);
        pipeLineAdded(orderId, "cust-4", "Stand", 1, 89.00);
        pipeOrderEvent(orderId, "cust-4", OrderEventType.ORDER_PAYED);
        pipeOrderEvent(orderId, "cust-4", OrderEventType.ORDER_CLOSED);

        EnrichedOrder enriched = enrichedOrdersTopic.readKeyValue().value;

        assertEquals(3, enriched.getItems().size());
        double expectedTotal = 450.00 + (3 * 12.99) + 89.00;
        assertEquals(expectedTotal, enriched.getTotalAmount(), 0.001);
        assertEquals(3, enriched.getItemCount());
        assertEquals("PLATINUM", enriched.getCustomerTier());
    }

    @Test
    void testLineRemovedReducesTotal() {
        seedCustomer("cust-5", "Eve Green", "eve@example.com", CustomerTier.GOLD);

        String orderId = "order-5";
        pipeOrderEvent(orderId, "cust-5", OrderEventType.ORDER_CREATED);
        pipeLineAdded(orderId, "cust-5", "ItemA", 2, 50.00);
        pipeLineAdded(orderId, "cust-5", "ItemB", 1, 30.00);
        pipeLineRemoved(orderId, "cust-5", "ItemA");
        pipeOrderEvent(orderId, "cust-5", OrderEventType.ORDER_CLOSED);

        EnrichedOrder enriched = enrichedOrdersTopic.readKeyValue().value;

        assertEquals(1, enriched.getItems().size());
        assertEquals("ItemB", enriched.getItems().get(0).getProductName());
        assertEquals(30.00, enriched.getTotalAmount(), 0.001);
    }

    @Test
    void testIndependentOrders() {
        seedCustomer("cust-6", "Frank Black", "frank@example.com", CustomerTier.SILVER);

        pipeOrderEvent("order-6a", "cust-6", OrderEventType.ORDER_CREATED);
        pipeLineAdded("order-6a", "cust-6", "Widget", 1, 10.00);

        pipeOrderEvent("order-6b", "cust-6", OrderEventType.ORDER_CREATED);
        pipeLineAdded("order-6b", "cust-6", "Gadget", 1, 20.00);

        pipeOrderEvent("order-6b", "cust-6", OrderEventType.ORDER_CLOSED);

        assertFalse(enrichedOrdersTopic.isEmpty());
        EnrichedOrder enrichedB = enrichedOrdersTopic.readKeyValue().value;
        assertEquals("order-6b", enrichedB.getOrderId());
        assertEquals(20.00, enrichedB.getTotalAmount(), 0.001);

        assertTrue(enrichedOrdersTopic.isEmpty(), "order-6a not closed yet");
    }

    private void seedCustomer(String customerId, String name, String email, CustomerTier tier) {
        Customer customer = Customer.newBuilder()
                .setCustomerId(customerId)
                .setName(name)
                .setEmail(email)
                .setTier(tier)
                .build();
        customersTopic.pipeInput(customerId, customer);
    }

    private void pipeOrderEvent(String orderId, String customerId, OrderEventType type) {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setEventType(type)
                .setTimestamp(Instant.now())
                .build();
        ordersTopic.pipeInput(orderId, event);
    }

    private void pipeLineAdded(String orderId, String customerId, String product, int qty, double price) {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setEventType(OrderEventType.ORDER_LINE_ADDED)
                .setProductName(product)
                .setQuantity(qty)
                .setUnitPrice(price)
                .setTimestamp(Instant.now())
                .build();
        ordersTopic.pipeInput(orderId, event);
    }

    private void pipeLineRemoved(String orderId, String customerId, String productName) {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setEventType(OrderEventType.ORDER_LINE_REMOVED)
                .setProductName(productName)
                .setTimestamp(Instant.now())
                .build();
        ordersTopic.pipeInput(orderId, event);
    }
}
