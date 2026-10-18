# SimpleKafka

SimpleKafka is a small, readable distributed commit log. A producer appends records to a partition, the leader writes them to disk, followers pull a replica, and a consumer reads them back from a stable offset. The moving parts are the ones that matter in this kind of system: a length-prefixed binary protocol, a segmented log, a controller, and leader-follower replication coordinated by ZooKeeper.

It is not wire-compatible with Apache Kafka, and it is not a fork of it. The broker is small enough to read in an afternoon.

## What is implemented

- Produce, fetch, metadata, and create-topic requests on one framed protocol.
- An append-only log per partition, split into segments with a sparse offset index.
- Crash recovery that truncates a torn tail and rebuilds the index.
- A high watermark, so consumers read only records that the in-sync replicas have stored.
- ZooKeeper for broker liveness, controller election, and partition assignment.
- Leader-follower replication. Followers pull from the leader.
- A synchronous producer, a simple consumer with an explicit position, and an admin client.
- A command line for a local ZooKeeper, brokers, and clients.

## What is left out

Consumer-group rebalancing, transactions, idempotent produce, log compaction, retention, authentication, and quotas are not implemented. `acks=0` is accepted and treated as `acks=1`, because every request on a connection still needs a response. Topics are created explicitly. There is no automatic topic creation.

## Requirements

- Java 17 or newer
- Maven 3.6.3 or newer
- ZooKeeper 3.9 for a real cluster. The `zk` command starts one in-process so a laptop demo does not need a separate install.

## Quick start

```shell
mvn -B -DskipTests package
```

Start coordination and two brokers, each in its own terminal:

```shell
bin/simplekafka zk --config config/zookeeper.properties
bin/simplekafka broker --config config/server-0.properties
bin/simplekafka broker --config config/server-1.properties
```

Create a topic, write five records, and read them from partition 0:

```shell
bin/simplekafka topic create \
  --bootstrap 127.0.0.1:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 2

bin/simplekafka produce \
  --bootstrap 127.0.0.1:9092 \
  --topic orders \
  --message hello \
  --count 5

bin/simplekafka consume \
  --bootstrap 127.0.0.1:9092 \
  --topic orders \
  --partition 0 \
  --max-messages 5
```

Without `--message`, `produce` reads one record per stdin line. `consume` without `--max-messages` polls until interrupted. Stop a broker and produce again through the other one. The surviving replica is elected leader and the records it had replicated stay readable.

A third broker config is in `config/server-2.properties`.

## Layout

```
com.simplekafka.common      shared types, errors, buffers
com.simplekafka.protocol    request and response codecs
com.simplekafka.storage     segmented partition logs
com.simplekafka.network     length-prefixed connections
com.simplekafka.zookeeper   metadata paths and the embedded server
com.simplekafka.server      broker, controller, replication
com.simplekafka.clients     producer, consumer, admin
com.simplekafka.tools       command line
```

## How a record moves

1. The producer refreshes metadata and sends the record to the current leader.
2. The leader appends it to the active segment and fsyncs the log and the index.
3. Followers fetch from the leader's log end and append the same offsets locally.
4. The leader advances the high watermark to the minimum log end of the in-sync replicas.
5. `acks=1` returns after the local append. `acks=-1` waits until the watermark covers the record.
6. A consumer fetches with `replicaId = -1` and is limited to offsets below the watermark.

Key records use murmur2 so the same key stays on one partition. Records without a key round-robin. The consumer does not join a group. Call `seek` to choose a position, and keep that position wherever you like.

## Storage

Each broker keeps its logs under `log.dirs`:

```
data/broker-0/orders-0/00000000000000000000.log
data/broker-0/orders-0/00000000000000000000.index
data/broker-0/orders-0/high-watermark
```

Segment files are named by their base offset. The index is a sparse list of `(relativeOffset, position)` pairs, 8 bytes each. On open, the broker scans the active segment, drops a partial record at the tail, and rewrites the index. The on-disk record layout is in [docs/storage.md](docs/storage.md).

## Cluster metadata

ZooKeeper holds membership and leadership, not the records:

```
/brokers/ids/{id}                                 ephemeral host and port
/controller                                       ephemeral controller id
/brokers/topics/{topic}                           partition assignment
/brokers/topics/{topic}/partitions/{id}/state     leader and in-sync replicas
```

The controller is the broker that creates `/controller`. It places replicas round-robin across live brokers and writes the preferred leader. Every broker watches the tree and opens a local log for partitions it replicates. If the leader disappears, the controller picks the first live member of the in-sync set.

The znode bodies are line-oriented `key=value` text, so `get` in the ZooKeeper shell is enough to inspect them.

## Build and test

```shell
mvn verify
```

That compiles, runs Checkstyle, and runs the unit and cluster tests. The cluster tests start an in-process ZooKeeper and real brokers. `mvn -B -DskipTests package` writes `target/simplekafka-0.1.0.jar`, which is what `bin/simplekafka` runs.

## Configuration

Broker files use the familiar property names. See `config/server-0.properties`.

| Property | Meaning |
| --- | --- |
| `broker.id` | Unique broker id. Also the ephemeral znode name. |
| `host.name` | Bind address and the host advertised to clients. |
| `port` | Listen port. `0` asks the OS for a free port. |
| `zookeeper.connect` | ZooKeeper connect string. |
| `log.dirs` | Directory for this broker's partition logs. |
| `log.segment.bytes` | Roll the active segment after this many bytes. |
| `log.index.interval.bytes` | Minimum gap between sparse index entries. |
| `replica.lag.time.max.ms` | Drop a silent follower from the in-sync set after this long. |
| `replica.fetch.max.wait.ms` | How long a follower fetch waits for new records. |
| `zookeeper.session.timeout.ms` | Session timeout. When it expires, the broker's ephemeral nodes disappear. |

## License

[Apache License 2.0](LICENSE). SimpleKafka is an independent project and is not part of Apache Kafka.
