package org.sxram.kafka.tutorial.basic;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.sxram.kafka.tutorial.Utils;

import java.util.List;
import java.util.Properties;

@Slf4j
public class MyProducer {

    private final Producer<String, String> producer;

    private final String topic;

    public MyProducer(final String topic, final Properties properties) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(properties);
    }

    public MyProducer(final String topic, final Producer<String, String> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    public void produce(final List<String> linesToProduce) {
        linesToProduce.stream()
                .filter(l -> !l.trim().isEmpty())
                .forEach(l -> this.produceRecord(producer, l));
        producer.close();
    }

    private void produceRecord(final Producer<String, String> producer, final String message) {
        val keyVal = Utils.parseKeyValue(message);

        producer.send(new ProducerRecord<>(topic, keyVal.getKey(), keyVal.getValue()), (recordMetadata, exception) -> {
            if (exception == null) {
                log.info("Record written to offset {}, timestamp: {}, partition: {}",
                        recordMetadata.offset(), recordMetadata.timestamp(), recordMetadata.partition());
            } else {
                log.error("An error occurred: {}", exception.getMessage(), exception);
            }
        });
    }

}



