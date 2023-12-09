package org.sxram.kafka.tutorial.basic;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class MyConsumerIT {

    @Test
    void starts() {
        new MyConsumer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES)).consume();
    }

}
