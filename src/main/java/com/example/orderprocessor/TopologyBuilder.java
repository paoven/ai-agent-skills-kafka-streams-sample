package com.example.orderprocessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TopologyBuilder {

    public static final String CUSTOMERS_TOPIC = "customers";
    public static final String ORDERS_TOPIC = "orders";
    public static final String ENRICHED_ORDERS_TOPIC = "enriched-orders";
    public static final String ORDER_AGGREGATES_STORE = "order-aggregates-store";

    public static Topology build(Properties props) {
        String srUrl = props.getProperty("schema.registry.url");
        Map<String, String> srConfig = Map.of("schema.registry.url", srUrl);

        SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(srConfig, false);

        SpecificAvroSerde<OrderEvent> orderEventSerde = new SpecificAvroSerde<>();
        orderEventSerde.configure(srConfig, false);

        SpecificAvroSerde<OrderAggregate> orderAggregateSerde = new SpecificAvroSerde<>();
        orderAggregateSerde.configure(srConfig, false);

        SpecificAvroSerde<EnrichedOrder> enrichedOrderSerde = new SpecificAvroSerde<>();
        enrichedOrderSerde.configure(srConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, Customer> customers = builder.globalTable(
                CUSTOMERS_TOPIC,
                Consumed.with(Serdes.String(), customerSerde));

        KStream<String, OrderEvent> orders = builder.stream(
                ORDERS_TOPIC,
                Consumed.with(Serdes.String(), orderEventSerde).withName("source-orders"));

        KTable<String, OrderAggregate> orderAggregates = orders
                .groupByKey(Grouped.with(Serdes.String(), orderEventSerde).withName("group-orders"))
                .aggregate(
                        TopologyBuilder::initAggregate,
                        TopologyBuilder::updateAggregate,
                        Named.as("aggregate-orders"),
                        Materialized.<String, OrderAggregate, KeyValueStore<Bytes, byte[]>>
                                as(ORDER_AGGREGATES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(orderAggregateSerde));

        orderAggregates
                .toStream(Named.as("to-stream"))
                .filter((orderId, agg) -> agg != null && agg.getIsClosed(), Named.as("filter-closed"))
                .join(customers,
                        (orderId, agg) -> agg.getCustomerId(),
                        TopologyBuilder::enrichOrder)
                .to(ENRICHED_ORDERS_TOPIC,
                        Produced.with(Serdes.String(), enrichedOrderSerde).withName("sink-enriched"));

        return builder.build();
    }

    static OrderAggregate initAggregate() {
        return OrderAggregate.newBuilder()
                .setOrderId("")
                .setCustomerId("")
                .setItems(new ArrayList<>())
                .setTotalAmount(0.0)
                .setIsPaid(false)
                .setIsClosed(false)
                .setLastUpdated(java.time.Instant.EPOCH)
                .build();
    }

    static OrderAggregate updateAggregate(String orderId, OrderEvent event, OrderAggregate agg) {
        agg.setOrderId(event.getOrderId());
        agg.setCustomerId(event.getCustomerId());
        agg.setLastUpdated(event.getTimestamp());

        switch (event.getEventType()) {
            case ORDER_CREATED:
                agg.setItems(new ArrayList<>());
                agg.setTotalAmount(0.0);
                agg.setIsPaid(false);
                agg.setIsClosed(false);
                break;

            case ORDER_LINE_ADDED:
                OrderLineItem item = OrderLineItem.newBuilder()
                        .setProductName(event.getProductName())
                        .setQuantity(event.getQuantity())
                        .setUnitPrice(event.getUnitPrice())
                        .build();
                List<OrderLineItem> added = new ArrayList<>(agg.getItems());
                added.add(item);
                agg.setItems(added);
                agg.setTotalAmount(computeTotal(added));
                break;

            case ORDER_LINE_REMOVED:
                List<OrderLineItem> remaining = new ArrayList<>(agg.getItems());
                remaining.removeIf(i -> i.getProductName().equals(event.getProductName()));
                agg.setItems(remaining);
                agg.setTotalAmount(computeTotal(remaining));
                break;

            case ORDER_PAYED:
                agg.setIsPaid(true);
                break;

            case ORDER_CLOSED:
                agg.setIsClosed(true);
                break;
        }

        return agg;
    }

    static EnrichedOrder enrichOrder(OrderAggregate agg, Customer customer) {
        return EnrichedOrder.newBuilder()
                .setOrderId(agg.getOrderId())
                .setCustomerId(agg.getCustomerId())
                .setCustomerName(customer.getName())
                .setCustomerEmail(customer.getEmail())
                .setCustomerTier(customer.getTier().name())
                .setItems(new ArrayList<>(agg.getItems()))
                .setTotalAmount(agg.getTotalAmount())
                .setItemCount(agg.getItems().size())
                .build();
    }

    private static double computeTotal(List<OrderLineItem> items) {
        return items.stream()
                .mapToDouble(i -> i.getQuantity() * i.getUnitPrice())
                .sum();
    }
}
