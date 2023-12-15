package org.sxram.kafka.tutorial;

import lombok.extern.slf4j.Slf4j;
import org.sxram.kafka.tutorial.basic.RecordProcessor;
import org.sxram.kafka.tutorial.basic.MyConsumer;
import org.sxram.kafka.tutorial.basic.MyProducer;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.JoinStreamToTable;
import org.sxram.kafka.tutorial.streams.StreamsApp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

@Slf4j
public class App {

    public static final String TOPIC = "my-topic";
    public static final String PRODUCER_INPUT = "producer_input.txt";

    private static final String CONFIG_PATH_PREFIX = "../config/";

    public static final String CLIENT_CONFLUENT_PROPERTIES = "client_confluent.properties";
    public static final String CONSUMER_PROPERTIES = "consumer.properties";
    public static final String PRODUCER_PROPERTIES = "producer.properties";
    public static final String STREAM_PROPERTIES = "stream.properties";
    private static final String JOIN_A_STREAM_TO_A_TABLE_PROPERTIES = "join-a-stream-to-a-table.properties";

    public static void main(String[] args) {
        try {
            log.info("Starting");
            runProducerConsumer();
            runStreamsApp();
            runJoinStreamToTable();
        } catch (Exception e) {
            log.error("Caught: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static void runJoinStreamToTable() throws InterruptedException {
        log.info("### Starting Joins Stream To Table");
        new JoinStreamToTable().run(Utils.mergeProperties(CONFIG_PATH_PREFIX + CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + JOIN_A_STREAM_TO_A_TABLE_PROPERTIES));
    }

    public static void runProducerConsumer() throws IOException {
        log.info("### Starting Producer/Consumer");
        new MyProducer(TOPIC,
                Utils.mergeProperties(
                        CONFIG_PATH_PREFIX + CLIENT_CONFLUENT_PROPERTIES, CONFIG_PATH_PREFIX + PRODUCER_PROPERTIES))
                .produce(Files.readAllLines(Paths.get(CONFIG_PATH_PREFIX + PRODUCER_INPUT)));
        try (MyConsumer consumer = new MyConsumer(TOPIC,
                Utils.mergeProperties(
                        CONFIG_PATH_PREFIX + CLIENT_CONFLUENT_PROPERTIES, CONFIG_PATH_PREFIX + CONSUMER_PROPERTIES),
                new RecordProcessor<>())) {
            consumer.consume(Duration.ofSeconds(3));
        }
    }

    public static void runStreamsApp() throws Exception {
        log.info("### Starting Streams");
        new StreamsApp().stream(Utils.mergeProperties(CONFIG_PATH_PREFIX + CLIENT_CONFLUENT_PROPERTIES,
                CONFIG_PATH_PREFIX + STREAM_PROPERTIES), Duration.ofSeconds(5));
    }

}



