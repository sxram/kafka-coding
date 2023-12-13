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
