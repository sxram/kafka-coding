package org.sxram.kafka.tutorial.basic;

import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.App;

import java.time.Duration;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.sxram.kafka.tutorial.TestUtils.createConfluentProps;

class MyConsumerTest {

    public static final int PARTITION = 1;

    @Test
    void throwsExceptionWhenPollIntervalTooSmall() {
        try (MyConsumer consumer = new MyConsumer(App.TOPIC, createConfluentProps(App.CONSUMER_PROPERTIES), new RecordProcessor<>())) {
            assertThrows(IllegalArgumentException.class, () -> consumer.consume(MyConsumer.POLL_TIMEOUT.minusMillis(1)));
        }
    }

    @Test
    void consumes() {
        try (MyConsumer consumer = new MyConsumer(App.TOPIC, createConfluentProps(App.CONSUMER_PROPERTIES), new RecordProcessor<>())) {
            consumer.consume(MyConsumer.POLL_TIMEOUT);
        }
    }

    /**
     * taken from here: <a href="https://www.baeldung.com/kafka-mockconsumer">https://www.baeldung.com/kafka-mockconsumer</a>
     * error: Cannot add records for a partition that is not assigned to the consumer
     * java.lang.IllegalStateException: Cannot add records for a partition that is not assigned to the consumer
     * at org.apache.kafka.clients.consumer.MockConsumer.addRecord(MockConsumer.java:232)
     */
    @Test
    @Disabled("adding record fails with exception")
    void consumesRecords() {
        final MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final RecordProcessor<String, String> processor = new RecordProcessor<>();
        val myConsumer = new MyConsumer(App.TOPIC, mockConsumer, new RecordProcessor<>());

        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        val topicPartition = new TopicPartition(App.TOPIC, PARTITION);
        startOffsets.put(topicPartition, 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);
        val record = new ConsumerRecord<>(App.TOPIC, PARTITION, 0, "my-test-key", "my-test-value");
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(record));

        myConsumer.consume(Duration.ofSeconds(1));

        assertThat(processor.getRecords()).hasSize(1);
        assertThat(mockConsumer.closed()).isTrue();
    }
}
