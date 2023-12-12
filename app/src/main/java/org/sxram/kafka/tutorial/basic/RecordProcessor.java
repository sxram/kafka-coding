package org.sxram.kafka.tutorial.basic;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Getter
public class RecordProcessor<T, K> implements java.util.function.Consumer<ConsumerRecord<T, K>> {

    private final List<ConsumerRecord<T, K>> records = new ArrayList<>();

    @Override
    public void accept(ConsumerRecord<T, K> record) {
        log.info("Consumed: key = {}, value = {}", record.key(), record.value());
        records.add(record);
    }

}



