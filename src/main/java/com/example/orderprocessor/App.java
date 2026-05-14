package com.example.orderprocessor;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        Properties props = loadConfig();
        Topology topology = TopologyBuilder.build(props);

        LOG.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setStateListener((newState, oldState) ->
                LOG.info("State transition from {} to {}", oldState, newState));

        streams.setUncaughtExceptionHandler(ex -> {
            LOG.error("Uncaught exception in stream thread", ex);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down...");
            streams.close(Duration.ofSeconds(30));
        }));

        streams.start();
    }

    private static Properties loadConfig() throws Exception {
        Properties props = new Properties();
        try (InputStream is = App.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            }
        }

        overrideFromEnv(props, "BOOTSTRAP_SERVERS", StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        overrideFromEnv(props, "SCHEMA_REGISTRY_URL", "schema.registry.url");

        return props;
    }

    private static void overrideFromEnv(Properties props, String envVar, String propKey) {
        String value = System.getenv(envVar);
        if (value != null && !value.isEmpty()) {
            props.put(propKey, value);
        }
    }
}
