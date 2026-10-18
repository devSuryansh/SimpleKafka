# Contributing

## Build

Java 17 or newer and Maven 3.6.3 or newer.

```shell
mvn verify
```

`mvn verify` compiles, runs Checkstyle, and runs the tests. The cluster tests start ZooKeeper in-process. They do not need a broker already running.

## Style

The Checkstyle rules in `checkstyle/checkstyle.xml` follow the same shape used by Apache Kafka:

- 4-space indentation, no tabs
- lines at most 120 characters
- no wildcard imports
- braces on every control statement
- a license header on every Java file, matching `checkstyle/java.header`
- SLF4J for logging, with parameterized messages

Imports are grouped `java`, `javax`, `org`, `com`, separated by a blank line, with static imports first.

Public types should say what they are for and which invariant a caller has to know. Comments should explain a decision that the code cannot, such as why a lock is not held across a wait, or why a follower keeps records past its watermark.

## Tests

Protocol codecs and the partition log have unit tests that do not start a broker. `ClusterTest` covers produce, consume, keyed partitioning, and leader failure with replication. Prefer extending those over adding a test that sleeps for a fixed time without a condition.

## Scope

Keep the broker readable. A change that adds a protocol field needs a version story, a codec test, and a note in `docs/protocol.md`. Do not claim wire compatibility with Apache Kafka.
