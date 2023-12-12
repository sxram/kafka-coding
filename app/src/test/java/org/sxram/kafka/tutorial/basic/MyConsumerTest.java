package org.sxram.kafka.tutorial.basic;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class MyConsumerTest {

    @Test
    void trowsExceptionWhenPollIntervalTooSmall() {
        assertThrows(IllegalArgumentException.class, () -> new MyConsumer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES), new ConsumHandler<>(), MyConsumer.POLL_TIMEOUT));
    }
}
