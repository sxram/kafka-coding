package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.sxram.kafka.tutorial.Utils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class MyConsumer {

    private static final String CONSUME_OFFSET = "earliest";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration POLL_DURATION = Duration.ofSeconds(10);

    private final Consumer<String, String> consumer;

    private final String topic;

    private final ConsumHandler<String, String> handler;

    public MyConsumer(final String topic, final Properties properties, ConsumHandler<String, String> handler) {
        this.topic = topic;
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUME_OFFSET);
        this.consumer = new KafkaConsumer<>(properties);
        this.handler = handler;
    }

    public void consume() {
        consumer.subscribe(List.of(topic));

        long duration = 0;
        while (duration < POLL_DURATION.toMillis()) {
            duration = duration + POLL_TIMEOUT.toMillis();
            poll();
        }
        consumer.close();
    }

    private void poll() {
        log.debug("Polling ...");
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, String> consumedRecord : records) {
            handler.handle(consumedRecord);
        }
    }

}



