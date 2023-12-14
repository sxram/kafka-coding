package org.sxram.kafka.tutorial.streams;

import lombok.val;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.sxram.kafka.tutorial.basic.MyConsumer;
import org.sxram.kafka.tutorial.basic.RecordProcessor;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

/**
 * Test against confluent server.
 */
@Testcontainers
class StreamsAppWithTestContainerIT {

    private static final String MOCK_SCHEMA_REGISTRY = "mock://localhost:8081";

    private static final DockerImageName KAFKA_KRAFT_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.1");

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_KRAFT_TEST_IMAGE).withNetwork(Network.SHARED);

    @Test
    void consumesStreamedMessage() throws Exception {
        Properties props = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.STREAM_PROPERTIES);
        props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        props.put("schema.registry.url", MOCK_SCHEMA_REGISTRY);

        new StreamsApp().stream(props, Duration.ofSeconds(5));

        // assert stream by consuming output topic
        RecordProcessor<String, String> recordProcessor = new RecordProcessor<>();
        final String outputTopic = props.getProperty("output.topic.name");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "kafka-java-getting-started");

        try (val consumer = new MyConsumer(outputTopic, props, recordProcessor)) {
            Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
                consumer.poll();
                val countPolled = recordProcessor.getRecords().size();
                return countPolled > 0;
            });
        }
    }

}
