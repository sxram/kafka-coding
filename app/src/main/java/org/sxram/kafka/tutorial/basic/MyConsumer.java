package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

@Slf4j
public class MyConsumer {

    private static final String CONSUME_OFFSET = "earliest";
    static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final Consumer<String, String> consumer;
    private final String topic;
    private final RecordProcessor<String, String> handler;
    private final Duration pollingDuration;

    public MyConsumer(final String topic, final Properties properties, RecordProcessor<String, String> handler,
                      final Duration pollingDuration) {
        if (pollingDuration.compareTo(POLL_TIMEOUT) <= 0) {
            throw new IllegalArgumentException("Polling duration too small (<" + POLL_TIMEOUT + ")");
        }
        this.topic = topic;
        this.handler = handler;
        this.pollingDuration = pollingDuration;
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUME_OFFSET);
        this.consumer = new KafkaConsumer<>(properties);
    }

    public MyConsumer(final String topic, final Consumer<String, String> consumer, RecordProcessor<String, String> handler,
                      final Duration pollingDuration) {
        if (pollingDuration.compareTo(POLL_TIMEOUT) <= 0) {
            throw new IllegalArgumentException("Polling duration too small (<" + POLL_TIMEOUT + ")");
        }
        this.topic = topic;
        this.handler = handler;
        this.pollingDuration = pollingDuration;
        this.consumer = consumer;
    }

    public void consume() {
        try {
            consumer.subscribe(List.of(topic));

            long duration = 0;
            while (duration < pollingDuration.toMillis()) {
                duration = duration + POLL_TIMEOUT.toMillis();
                poll();
            }
            consumer.close();
        } catch (WakeupException ex) {
            // ignore for shutdown
        } catch (RuntimeException ex) {
            throw(ex);
        } finally {
            consumer.close();
        }
    }

    private void poll() {
        log.debug("Polling ...");
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        StreamSupport.stream(records.spliterator(), false)
                .forEach(handler);
        consumer.commitSync();
    }

}



