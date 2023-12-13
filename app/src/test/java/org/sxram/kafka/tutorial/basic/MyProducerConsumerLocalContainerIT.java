package org.sxram.kafka.tutorial.basic;

import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class MyProducerConsumerLocalContainerIT {

    private static final DockerImageName KAFKA_KRAFT_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.1");

    private static KafkaContainer kafkaContainer;

    @BeforeAll
    static void containerSetup() {
        kafkaContainer = new KafkaContainer(KAFKA_KRAFT_TEST_IMAGE).withNetwork(Network.SHARED);
        kafkaContainer.start();
    }

    @Test
    void consumesProducedMessage() throws IOException {
        RecordProcessor<String, String> handlerMock = spy(new RecordProcessor<>());
        Path producerConfigPath = Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT);
        val producerProps = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.PRODUCER_PROPERTIES);
        producerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        val consumerProps = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES);
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

        new MyProducer(App.TOPIC, producerProps).produce(Files.readAllLines(producerConfigPath));
        new MyConsumer(App.TOPIC, consumerProps, handlerMock, Duration.ofSeconds(5)).consume();

        try (val lines = Files.lines(producerConfigPath)) {
            verify(handlerMock, times((int) lines.count())).accept(any());
        }
    }

    @AfterAll
    static void shutdown() {
        kafkaContainer.stop();
    }

}
