package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

/**
 * Test against confluent server.
 */
@Slf4j
class MyProducerConsumerToConfluenceServerIT {

    @Test
    void pollProducedMessages() throws IOException {
        RecordProcessor<String, String> recordProcessor = new RecordProcessor<>();

        val producerConfigPath = Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT);

        new MyProducer(App.TOPIC, Utils.mergeProperties(
                CONFIG_PATH_PREFIX + App.CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.PRODUCER_PROPERTIES)).produce(Files.readAllLines(producerConfigPath));

        try (val consumer = new MyConsumer(App.TOPIC, Utils.mergeProperties(
                CONFIG_PATH_PREFIX + App.CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES), recordProcessor);
             val lines = Files.lines(producerConfigPath).filter(l -> !l.trim().isEmpty())) {

            val linesCountProduced = lines.count();

            Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
                consumer.poll();
                val countPolled = recordProcessor.getRecords().size();
                return countPolled >= linesCountProduced;
            });

        }
    }

}
