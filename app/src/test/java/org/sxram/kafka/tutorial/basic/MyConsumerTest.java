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
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.sxram.kafka.tutorial.TestUtils.CONFIG_PATH_PREFIX;

class MyConsumerTest {

    public static final int PARTITION = 1;

    @Test
    void throwsExceptionWhenPollIntervalTooSmall() {
        val durationTooSmall = MyConsumer.POLL_TIMEOUT.minusMillis(1);
        val processor = new RecordProcessor<String, String>();
        val props = Utils.mergeProperties(CONFIG_PATH_PREFIX + App.CLIENT_PROPERTIES,
                CONFIG_PATH_PREFIX + App.CONSUMER_PROPERTIES);

        assertThrows(IllegalArgumentException.class, () -> new MyConsumer(App.TOPIC, props, processor, durationTooSmall));
    }

    /**
     * taken from here: <a href="https://www.baeldung.com/kafka-mockconsumer">https://www.baeldung.com/kafka-mockconsumer</a>
     * error: Cannot add records for a partition that is not assigned to the consumer
     * java.lang.IllegalStateException: Cannot add records for a partition that is not assigned to the consumer
     * 	at org.apache.kafka.clients.consumer.MockConsumer.addRecord(MockConsumer.java:232)
     */
    @Test
    @Disabled("adding record fails with exception")
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
