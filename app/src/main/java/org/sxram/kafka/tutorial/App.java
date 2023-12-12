package org.sxram.kafka.tutorial;

import lombok.extern.slf4j.Slf4j;
import org.sxram.kafka.tutorial.basic.RecordProcessor;
import org.sxram.kafka.tutorial.basic.MyConsumer;
import org.sxram.kafka.tutorial.basic.MyProducer;
import org.sxram.kafka.tutorial.streams.StreamsApp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

@Slf4j
public class App {

    public static final String TOPIC = "my-topic";
    public static final String PRODUCER_INPUT = "producer_input.txt";

    public static final String CLIENT_PROPERTIES = "client.properties";
    public static final String CONSUMER_PROPERTIES = "consumer.properties";
    public static final String PRODUCER_PROPERTIES = "producer.properties";
    public static final String STREAM_PROPERTIES = "stream.properties";

    public static void main(String[] args) {
        try {
            log.info("Starting");
            runProducerConsumer("../config/");
            runStreamsApp("../config/");
        } catch (Exception e) {
            log.error("Caught: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static void runProducerConsumer(final String configPathPrefix) throws IOException {
        log.info("### Starting Producer/Consumer");
        new MyProducer(TOPIC,
                Utils.mergeProperties(configPathPrefix + CLIENT_PROPERTIES, configPathPrefix + PRODUCER_PROPERTIES))
                .produce(Files.readAllLines(Paths.get(configPathPrefix + PRODUCER_INPUT)));
        new MyConsumer(TOPIC,
                Utils.mergeProperties(configPathPrefix + CLIENT_PROPERTIES, configPathPrefix + CONSUMER_PROPERTIES),
                new RecordProcessor<>(), Duration.ofSeconds(3))
                .consume();
    }

    public static void runStreamsApp(final String configPathPrefix) throws Exception {
        log.info("### Starting Streams");
        new StreamsApp().stream(Utils.mergeProperties(configPathPrefix + CLIENT_PROPERTIES,
                configPathPrefix + STREAM_PROPERTIES), Duration.ofSeconds(5));
    }

}



