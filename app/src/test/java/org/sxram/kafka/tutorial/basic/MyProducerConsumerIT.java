package org.sxram.kafka.tutorial.basic;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class MyProducerConsumerIT {

    @Test
    void starts() throws IOException {
        new MyProducer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.PRODUCER_PROPERTIES)).
                produce(Files.readAllLines(Paths.get(CONFIG_PATH_PREFIX + App.PRODUCER_INPUT)));
        new MyConsumer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES)).consume();
    }

}
