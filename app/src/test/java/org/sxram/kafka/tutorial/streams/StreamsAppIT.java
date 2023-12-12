package org.sxram.kafka.tutorial.streams;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.sxram.kafka.tutorial.basic.ConsumHandler;
import org.sxram.kafka.tutorial.basic.MyConsumer;

import java.util.Properties;

class StreamsAppIT {

    @Test
    void consumesStreamedMessage() throws Exception {
        Properties props = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.STREAM_PROPERTIES);
        new StreamsApp().stream(props);

        ConsumHandler<String, String> handlerMock = spy(new ConsumHandler<>());
        final String outputTopic = props.getProperty("output.topic.name");
        new MyConsumer(outputTopic, props, handlerMock).consume();

        verify(handlerMock, atLeastOnce()).handle(any());
    }

}
