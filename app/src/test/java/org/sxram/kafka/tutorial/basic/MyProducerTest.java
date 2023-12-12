package org.sxram.kafka.tutorial.basic;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.TestUtils;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MyProducerTest {

    @Test
    void produces() {
        final StringSerializer stringSerializer = new StringSerializer();
        final MockProducer<String, String> mockProducer = new MockProducer<>(true, stringSerializer, stringSerializer);
        final MyProducer producerApp = new MyProducer(App.TOPIC, mockProducer);

        final List<String> records = Arrays.asList("foo-bar", "bar-foo", "baz-bar", "great:weather");
        producerApp.produce(records);

        final List<KeyValue<String, String>> expectedList = Arrays.asList(KeyValue.pair("foo", "bar"),
                KeyValue.pair("bar", "foo"),
                KeyValue.pair("baz", "bar"),
                KeyValue.pair(null, "great:weather"));

        final List<KeyValue<String, String>> actualList = mockProducer.history().stream()
                .map(TestUtils::toKeyValue)
                .toList();

        assertThat(actualList).containsAll(expectedList);
    }
}
