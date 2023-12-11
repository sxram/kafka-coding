package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumHandler<T, K> {

    public void handle(final ConsumerRecord<T, K> record) {
        log.info("Consumed: key = {}, value = {}", record.key(), record.value());
    }

}



