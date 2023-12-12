package org.sxram.kafka.tutorial.basic;

import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class MyConsumerTest {

    public static final int PARTITION = 1;

    @Test
    void trowsExceptionWhenPollIntervalTooSmall() {
        assertThrows(IllegalArgumentException.class, () -> new MyConsumer(App.TOPIC,
                Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                        CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES), new RecordProcessor<>(), MyConsumer.POLL_TIMEOUT));
    }

    @Test
    @Disabled
    void consumes() {
        final MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final RecordProcessor<String, String> processor = new RecordProcessor<>();
        val myConsumer = new MyConsumer(App.TOPIC, mockConsumer, new RecordProcessor<>(), Duration.ofSeconds(1));

        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        val topicPartition = new TopicPartition(App.TOPIC, PARTITION);
        startOffsets.put(topicPartition, 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);
        val record = new ConsumerRecord<>(App.TOPIC, PARTITION, 0, "my-test-key","my-test-value");
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(record));

        myConsumer.consume();

        assertThat(processor.getRecords()).hasSize(1);
        assertThat(mockConsumer.closed()).isTrue();
    }
}
