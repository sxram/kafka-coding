package org.sxram.kafka.tutorial.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.sxram.kafka.tutorial.Utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamsApp {

    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("Streams Closed");
    }

    static Topology buildTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
            .peek((k,v) -> log.info("Observed event: {}", v))
            .mapValues(s -> s.toUpperCase())
            .peek((k,v) -> log.info("Transformed event: {}", v))
            .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    public void stream(Properties props, Duration duration) throws Exception {
        final String inputTopic = props.getProperty("input.topic.name");
        final String outputTopic = props.getProperty("output.topic.name");

        try (StreamUtil utility = new StreamUtil()) {

            utility.createTopics(props, Arrays.asList(
                            new NewTopic(inputTopic, Optional.empty(), Optional.empty()),
                            new NewTopic(outputTopic, Optional.empty(), Optional.empty())));

            try (StreamUtil.RandomizeProducer randomizeProducer = utility.startNewRandomizer(props, inputTopic)) {

                KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(inputTopic, outputTopic), props);

                Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

                scheduleStreamingTermination(randomizeProducer, kafkaStreams, duration);

                log.info("Starting Kafka Streams");
                runKafkaStreams(kafkaStreams);
                log.info("Terminated Kafka Streams");
            }
        }
    }

    private void scheduleStreamingTermination(StreamUtil.RandomizeProducer randomizeProducer,
                                              KafkaStreams kafkaStreams,
                                              Duration duration) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.schedule(new TimerTask() {
            @Override
            public void run() {
                randomizeProducer.close();
                kafkaStreams.close();
            }
        }, duration.toSeconds(), TimeUnit.SECONDS);
    }

}
