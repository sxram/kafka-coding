package org.sxram.kafka.tutorial.streams;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.sxram.kafka.tutorial.basic.MyConsumer;

import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class StreamsAppIT {

    @Test
    void starts() {
        new MyConsumer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.STREAM_PROPERTIES)).consume();
    }

}
