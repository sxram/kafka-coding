package org.sxram.kafka.tutorial.streams;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.sxram.kafka.tutorial.basic.RecordProcessor;
import org.sxram.kafka.tutorial.basic.MyConsumer;

import java.time.Duration;
import java.util.Properties;

class StreamsAppIT {

    @Test
    void consumesStreamedMessage() throws Exception {
        Properties props = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.STREAM_PROPERTIES);
        new StreamsApp().stream(props, Duration.ofSeconds(5));

        // assert stream by consuming output topic
        RecordProcessor<String, String> handlerMock = spy(new RecordProcessor<>());
        final String outputTopic = props.getProperty("output.topic.name");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "kafka-java-getting-started");
        new MyConsumer(outputTopic, props, handlerMock, Duration.ofSeconds(10)).consume();

        verify(handlerMock, atLeastOnce()).accept(any());
    }

}
