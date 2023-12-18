package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.sxram.kafka.tutorial.App;

import java.util.Properties;

import static org.sxram.kafka.tutorial.TestUtils.createConfluentProps;

/**
 * Test against confluent server.
 */
@Slf4j
@Disabled
class MyProducerConsumerToConfluentServerIT extends AbstractMyProducerConsumerIT {

    @Override
    Properties getConsumerProps() {
        return createConfluentProps(App.CONSUMER_PROPERTIES);
    }

    @Override
    Properties getProducerProps() {
        return createConfluentProps(App.PRODUCER_PROPERTIES);
    }

    @Override
    String getBootstrapServers() {
        // not needed, taken from config file
        return null;
    }

}
