# kafka-coding

## Confluent CLI

### Setup Confluent CLI

Install

```shell
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest
mv bin/confluent ~/bin 
rm -Rf bin
confluent update
```

Setup

```shell
export API_KEY=<api_key>
```

```shell
confluent login --save

#confluent environment list
env=$(confluent environment list | grep "*" | awk '{print $3 }')
confluent environment use $env

#confluent kafka cluster list
cluster=$(confluent kafka cluster list | grep "*" | awk '{print $3 }')
confluent kafka cluster use $cluster

confluent api-key store --resource $cluster
# or: confluent api-key create --resource $cluster
confluent api-key use $API_KEY
```

### CLI commands - Topics / Consume / Produce

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

# Build / Run

```shell
./gradlew clean shadowJar
```
```shell
java -jar ./app/build/libs/kafka-java-getting-started-0.0.1-all.jar
```

## Python

```shell
pip install virtualenv
virtualenv env
source env/bin/activate
pip install confluent-kafka
```

```shell
confluent kafka cluster describe
```

```shell
chmod u+x consumer.py
```
```shell
./consumer.py config.ini
```
