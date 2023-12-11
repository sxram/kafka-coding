package org.sxram.kafka.tutorial.streams;

import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

class StreamsAppIT {

    @Test
    @Disabled
    void starts() throws Exception {
        new StreamsApp().stream(Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.STREAM_PROPERTIES));
    }

}
