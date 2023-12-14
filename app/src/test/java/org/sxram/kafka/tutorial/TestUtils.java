package org.sxram.kafka.tutorial;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;

public class TestUtils {

    public static final String CONFIG_PATH_PREFIX = "../config/";

    public static KeyValue<String, String> toKeyValue(final ProducerRecord<String, String> producerRecord) {
        return KeyValue.pair(producerRecord.key(), producerRecord.value());
    }

}
