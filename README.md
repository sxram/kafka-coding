# kafka-coding

Inspired by [confluent tutorials](https://developer.confluent.io/tutorials).

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

## Java App Build / Test / Run

Build jar and run
```shell
./gradlew clean build
```
or skip tests:
```shell
./gradlew clean build -x test
```
```shell
java -jar ./app/build/libs/kafka-java-getting-started-0.0.1-all.jar
```

Test
```
./gradlew test
```
Run by gradle
```
./gradlew run
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
