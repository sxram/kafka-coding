package org.sxram.kafka.tutorial;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class AppIT {

    @Test
    void runsProduceConsume() throws Exception {
        App.runProducerConsumer("../config/");
    }

    @Test
    @Disabled
    void runsStreams() throws Exception {
        App.runStreamsApp("../config/");
    }

}
