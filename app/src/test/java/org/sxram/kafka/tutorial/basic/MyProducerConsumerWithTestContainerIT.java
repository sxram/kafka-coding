package org.sxram.kafka.tutorial.basic;

import lombok.val;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;
import static org.sxram.kafka.tutorial.TestUtils.createProps;

@Testcontainers
class MyProducerConsumerWithTestContainerIT {

    private static final DockerImageName KAFKA_KRAFT_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.1");

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_KRAFT_TEST_IMAGE).withNetwork(Network.SHARED);

    @Test
    void consumesProducedMessages() throws IOException {
        RecordProcessor<String, String> handlerMock = spy(new RecordProcessor<>());
        Path producerConfigPath = Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT);
        val producerProps = createProps(App.PRODUCER_PROPERTIES);
        producerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        val consumerProps = createProps(App.CONSUMER_PROPERTIES);
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

        new MyProducer(App.TOPIC, producerProps).produce(Files.readAllLines(producerConfigPath));

        try (val consumer = new MyConsumer(App.TOPIC, consumerProps, handlerMock)) {
            consumer.consume(Duration.ofSeconds(5));
        }

        try (val lines = Files.lines(producerConfigPath).filter(l -> !l.trim().isEmpty())) {
            verify(handlerMock, atLeast((int) lines.count())).accept(any());
        }
    }

    @Test
    void consumesProducedMessagesParallel() throws IOException {
        RecordProcessor<String, String> handlerMock = spy(new RecordProcessor<>());
        Path producerConfigPath = Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT);
        val producerProps = createProps(App.PRODUCER_PROPERTIES);
        producerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        val consumerProps = createProps(App.CONSUMER_PROPERTIES);
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

        new MyProducer(App.TOPIC, producerProps).produce(Files.readAllLines(producerConfigPath));

        try (val consumer = new MyConsumer(App.TOPIC, consumerProps, handlerMock)) {
            consumer.consumeWithParallelProcessing(Duration.ofSeconds(5));
        }

        verify(handlerMock, atLeast(MyConsumer.PARALLEL_PROCESSING_BATCH_SIZE)).accept(any());
    }

    @Test
    void pollsProducedMessages() throws IOException {
        RecordProcessor<String, String> recordProcessor = new RecordProcessor<>();
        Path producerConfigPath = Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT);
        val producerProps = createProps(App.PRODUCER_PROPERTIES);
        producerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        val consumerProps = createProps(App.CONSUMER_PROPERTIES);
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

        new MyProducer(App.TOPIC, producerProps).produce(Files.readAllLines(producerConfigPath));

        try (val consumer = new MyConsumer(App.TOPIC, consumerProps, recordProcessor);
             val lines = Files.lines(producerConfigPath).filter(l -> !l.trim().isEmpty())) {

            val linesCountProduced = lines.count();

            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                consumer.poll();
                return recordProcessor.getRecords().size() >= linesCountProduced;
            });
        }
    }

}
