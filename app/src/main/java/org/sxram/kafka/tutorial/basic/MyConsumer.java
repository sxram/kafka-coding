package org.sxram.kafka.tutorial.basic;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;

@Slf4j
public class MyConsumer implements AutoCloseable {

    private static final String CONSUME_OFFSET = "earliest";
    static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private static final int PARALLEL_PROCESSING_NUM_THREADS = 2;
    static final int PARALLEL_PROCESSING_BATCH_SIZE = 10;

    private final Consumer<String, String> consumer;
    private final RecordProcessor<String, String> handler;

    public MyConsumer(final String topic, final Properties properties, RecordProcessor<String, String> handler) {
        this.handler = handler;
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUME_OFFSET);
        this.consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));
    }

    public MyConsumer(final String topic, final Consumer<String, String> consumer, RecordProcessor<String, String> handler) {
        this.handler = handler;
        this.consumer = consumer;

        consumer.subscribe(List.of(topic));
    }

    /**
     * blocks for a duration of <code>pollingDuration</code>
     * @param pollingDuration polling duration
     */
    public void consume(final Duration pollingDuration) {
        if (pollingDuration.compareTo(POLL_TIMEOUT) < 0) {
            throw new IllegalArgumentException("Polling duration too small (<" + POLL_TIMEOUT + ")");
        }

        try {
            long duration = 0;
            while (duration < pollingDuration.toMillis()) {
                duration = duration + POLL_TIMEOUT.toMillis();
                poll();
            }
        } catch (WakeupException ex) {
            // ignore for shutdown
        } catch (RuntimeException ex) {
            throw(ex);
        }
    }

    public void consumeWithParallelProcessing(final Duration pollingDuration) {
        if (pollingDuration.compareTo(POLL_TIMEOUT) < 0) {
            throw new IllegalArgumentException("Polling duration too small (<" + POLL_TIMEOUT + ")");
        }

        List<ConsumerRecord<String, String>> records = new ArrayList<>();

        try {
            long duration = 0;
            while (duration < pollingDuration.toMillis()) {
                duration = duration + POLL_TIMEOUT.toMillis();
                log.debug("Polling ...");
                ConsumerRecords<String, String> batchRecords = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, String> batchRecord : batchRecords) {
                    records.add(batchRecord);
                }

                if (records.size() >= PARALLEL_PROCESSING_BATCH_SIZE) {
                    handleParallel(records);
                    consumer.commitSync();
                    records.clear();
                }
            }
        } catch (WakeupException ex) {
            // ignore for shutdown
        } catch (RuntimeException ex) {
            throw(ex);
        }
    }

    private void handleParallel(List<ConsumerRecord<String, String>> records) {
        List<List<ConsumerRecord<String, String>>> partitions =
                Lists.partition(records, PARALLEL_PROCESSING_BATCH_SIZE / PARALLEL_PROCESSING_NUM_THREADS);

        List<Callable<Void>> tasks = new ArrayList<>();
        for (List<ConsumerRecord<String, String>> partition : partitions) {
            tasks.add(() -> {
                for (ConsumerRecord<String, String> recordToProcess : partition) {
                    handler.accept(recordToProcess); // Process the record
                }
                return null;
            });
        }
        ExecutorService executorService = Executors.newFixedThreadPool(PARALLEL_PROCESSING_NUM_THREADS);

        try {
            executorService.invokeAll(tasks);
        } catch (InterruptedException ex) {
            log.error("Interrupted: {}", ex.getMessage());
        } finally {
            executorService.shutdown();
        }
    }

    public void close() {
        consumer.close();
    }

    public void poll() {
        log.debug("Polling ...");
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        StreamSupport.stream(records.spliterator(), false)
                .forEach(handler);
        consumer.commitSync();
    }

}



