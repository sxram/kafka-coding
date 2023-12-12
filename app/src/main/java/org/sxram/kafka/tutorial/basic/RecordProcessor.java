package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;

@Slf4j
public class RecordProcessor<T, K> implements java.util.function.Consumer<ConsumerRecord<T, K>> {

    @Override
    public void accept(ConsumerRecord<T, K> record) {
        log.info("Consumed: key = {}, value = {}", record.key(), record.value());
    }
}



