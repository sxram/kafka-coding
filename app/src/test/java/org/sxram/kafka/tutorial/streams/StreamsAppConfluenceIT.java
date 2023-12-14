package org.sxram.kafka.tutorial.streams;

import lombok.val;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.sxram.kafka.tutorial.basic.MyConsumer;
import org.sxram.kafka.tutorial.basic.RecordProcessor;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

/**
 * Test against confluent server.
 */
class StreamsAppConfluenceIT {

    @Test
    void consumesStreamedMessage() throws Exception {
        Properties props = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.STREAM_PROPERTIES);
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
