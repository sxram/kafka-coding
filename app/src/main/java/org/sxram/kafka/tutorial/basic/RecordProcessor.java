package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;

@Slf4j
public class RecordProcessor<T, K> {

    public void process(final ConsumerRecord<T, K> record) {
        log.info("Consumed: key = {}, value = {}", record.key(), record.value());
    }

}



