package org.sxram.kafka.tutorial;

import org.junit.jupiter.api.Test;

class AppIT {

    @Test
    void runsProduceConsume() throws Exception {
        App.runStreamsApp("../config/");
    }

}
