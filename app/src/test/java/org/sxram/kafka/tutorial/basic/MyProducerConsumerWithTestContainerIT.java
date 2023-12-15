package org.sxram.kafka.tutorial.basic;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class MyProducerConsumerWithTestContainerIT extends AbstractMyProducerConsumerIT {

    private static final DockerImageName KAFKA_KRAFT_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.1");

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_KRAFT_TEST_IMAGE).withNetwork(Network.SHARED);

    @Override
    String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

}
