package org.sxram.kafka.tutorial.streams;

import lombok.val;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class StreamsAppTest {

    @Test
    void topologyShouldUpperCaseInputs() {
        val inputTopicName = "random-strings";
        val outputTopicName = "tall-random-strings";

        final Topology topology = StreamsApp.buildTopology(inputTopicName, outputTopicName);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology);
             final Serde<String> stringSerde = Serdes.String()) {

            final TestInputTopic<String, String> inputTopic = testDriver
                    .createInputTopic(inputTopicName, stringSerde.serializer(), stringSerde.serializer());
            final TestOutputTopic<String, String> outputTopic = testDriver
                    .createOutputTopic(outputTopicName, stringSerde.deserializer(), stringSerde.deserializer());

            val inputs = Arrays.asList(
                    "Chuck Norris can write multi-threaded applications with a single thread.",
                    "No statement can catch the ChuckNorrisException.",
                    "Chuck Norris can divide by zero.",
                    "Chuck Norris can binary search unsorted data."
            );
            val expectedOutputs = inputs.stream()
                    .map(String::toUpperCase)
                    .toList();

            inputs.forEach(inputTopic::pipeInput);

            assertThat(outputTopic.readValuesToList()).containsAll(expectedOutputs);
        }

    }
}
