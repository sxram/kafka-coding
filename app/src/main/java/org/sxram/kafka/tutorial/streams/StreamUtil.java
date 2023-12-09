package org.sxram.kafka.tutorial.streams;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class StreamUtil implements AutoCloseable {

    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public static class RandomizeProducer implements AutoCloseable, Runnable {
        private final Properties props;
        private final String topic;
        private boolean closed;

        public RandomizeProducer(Properties producerProps, String topic) {
            this.closed = false;
            this.topic = topic;
            this.props = producerProps;
            this.props.setProperty("client.id", "faker");
        }

        public void run() {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                Faker faker = new Faker();
                while (!closed) {
                    produce(producer, faker);
                }
            } catch (Exception ex) {
                log.error(ex.toString());
            }
        }

        private void produce(KafkaProducer<String, String> producer, Faker faker) throws ExecutionException {
            try {
                producer.send(new ProducerRecord<>(this.topic, faker.chuckNorris().fact())).get();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.warn("{}", e.getMessage());
            }
        }

        public void close() {
            closed = true;
        }
    }

    public RandomizeProducer startNewRandomizer(Properties producerProps, String topic) {
        RandomizeProducer rv = new RandomizeProducer(producerProps, topic);
        executorService.submit(rv);
        return rv;
    }

    public void createTopics(final Properties allProps,
                             List<NewTopic> topics) throws InterruptedException, ExecutionException, TimeoutException {
        try (final AdminClient client = AdminClient.create(allProps)) {
            log.info("Creating topics");

            client.createTopics(topics).values().forEach((topic, future) -> {
                try {
                    future.get();
                } catch (Exception ex) {
                    log.error("Caught: {}", ex.getMessage(), ex);
                }
            });

            Collection<String> topicNames = topics.stream().map(NewTopic::name)
                    .collect(Collectors.toCollection(LinkedList::new));

            log.info("Asking cluster for topic descriptions");
            client.describeTopics(topicNames).allTopicNames().get(10, TimeUnit.SECONDS)
                    .forEach((name, description) -> log.info("Topic Description: {}", description.toString()));
        }
    }

    public void close() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }
}
