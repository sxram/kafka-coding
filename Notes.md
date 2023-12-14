# CLI commands - Topics / Consume / Produce

```shell
confluent kafka topic list
```

```shell
confluent kafka topic create <topic>
confluent kafka topic create --partitions <num> <topic>
confluent kafka topic describe <topic>
```

Consume
```shell
confluent kafka topic consume <topic>
confluent kafka topic consume --from-beginning <topic>
confluent kafka topic consume --value-format avro <topic>
confluent kafka topic consume --value-format avro <topic>  --print-key --delimiter " -> "
```

Produce
```shell
confluent kafka topic produce <topic>
confluent kafka topic produce <topic> --value-format avro --schema orders-avro-schema.json --parse-key
```

# Streams

* [Streams DSL](https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl?session_ref=https://www.google.com/)

# Testing

* [Streams - Unit Tests](https://www.confluent.io/blog/stream-processing-part-2-testing-your-streaming-application/?session_ref=https://www.google.com/)
  * [TestDriver](https://www.confluent.io/de-de/blog/test-kafka-streams-with-topologytestdriver/)
* Stream - Integration Tests
  * EmbeddedKafkaCluster
    * [Example ITs](https://github.com/apache/kafka/tree/trunk/streams/src/test/java/org/apache/kafka/streams/integration)
  * [Test Docker Container](https://java.testcontainers.org/modules/kafka/)