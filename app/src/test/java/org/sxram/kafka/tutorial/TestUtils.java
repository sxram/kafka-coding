package org.sxram.kafka.tutorial;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;

import java.util.Properties;

public class TestUtils {

    public static final String CONFIG_PATH_PREFIX = "../config/";

    public static KeyValue<String, String> toKeyValue(final ProducerRecord<String, String> producerRecord) {
        return KeyValue.pair(producerRecord.key(), producerRecord.value());
    }

    public static Properties createConfluentProps(final String specialPropsPath) {
        return Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + specialPropsPath);
    }

    public static Properties createProps(final String propsPath) {
        return Utils.mergeProperties(CONFIG_PATH_PREFIX + propsPath);
    }

}
