# SimpleKafka

A simplified Kafka-like message broker, built one component at a time.

## Status

Stage 1: Maven project layout for separate broker and client code.

## Project structure

```
SimpleKafka/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── simplekafka/
│   │               ├── broker/
│   │               └── client/
│   └── test/
│       └── java/
└── pom.xml
```

## Build

Java 11 and Maven are required.

```shell
mvn clean package
```

The packaged jar's main class is `com.simplekafka.broker.SimpleKafkaBroker`.
