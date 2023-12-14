package org.sxram.kafka.tutorial.basic;

import lombok.val;
import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

/**
 * Test against confluent server.
 */
class MyProducerConsumerConfluenceIT {

    @Test
    void consumesProducedMessage() throws IOException {
        RecordProcessor<String, String> handlerMock = spy(new RecordProcessor<>());

        val producerConfigPath = Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT);

        new MyProducer(App.TOPIC, Utils.mergeProperties(
                CONFIG_PATH_PREFIX + App.CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.PRODUCER_PROPERTIES)).produce(Files.readAllLines(producerConfigPath));
        try(val consumer = new MyConsumer(App.TOPIC, Utils.mergeProperties(
                CONFIG_PATH_PREFIX + App.CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES), handlerMock)) {
            consumer.consume(Duration.ofSeconds(5));
        }

        try (val lines = Files.lines(producerConfigPath)) {
            verify(handlerMock, atLeast((int) lines.count())).accept(any());
        }
    }

}
