package org.sxram.kafka.tutorial.basic;

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

class MyProducerConsumerIT {

    @Test
    void consumesProducedMessageWithHandlerMock() throws IOException {
        RecordProcessor<String, String> handlerMock = spy(new RecordProcessor<>());

        new MyProducer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.PRODUCER_PROPERTIES)).
                produce(Files.readAllLines(Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT)));
        new MyConsumer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES), handlerMock, Duration.ofSeconds(3)).consume();

        verify(handlerMock, atLeastOnce()).process(any());
    }

}
