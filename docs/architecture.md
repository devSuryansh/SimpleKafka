# Architecture

SimpleKafka is one process per broker. Each process binds a port, registers an ephemeral ZooKeeper node, and then accepts client connections. One broker also holds `/controller`. That broker is the only one allowed to create topics and to choose a new leader when a broker disappears.

## Processes

```
producer  -->  leader broker  -->  partition log
                  ^   |
                  |   +--> followers pull records
                  |
consumer  -->  leader broker (reads below the high watermark)
```

Clients start from a bootstrap address, fetch metadata, and then talk to the leader of the partition they want. A `NOT_LEADER_OR_FOLLOWER` response makes the client refresh metadata and try again. Create-topic requests go to the controller, which metadata also reports.

## Broker startup

1. Bind the listen socket. If the configured port is 0, the operating system picks one.
2. Connect to ZooKeeper and create `/brokers/ids/{id}` with the bound host and port.
3. Watch `/brokers/ids`, `/controller`, and `/brokers/topics`.
4. Try to create `/controller`. The winner becomes controller and reconciles leadership.
5. Accept connections.

Registration happens before accept, so a client never sees a broker that cannot be looked up. Session expiry deletes the ephemeral nodes. The client reconnects, registers again, and the controller election runs once more.

## Request path

Connections are blocking and length-prefixed: 4 byte big-endian size, then the payload. Each connection runs on its own thread. Produce with `acks=-1` waits for followers, so the pool is unbounded. A fixed pool could stall a follower fetch behind those waits and stop the watermark from moving.

The payload starts with a common header (`apiKey`, `apiVersion`, `correlationId`, `clientId`). Version 0 is the only version. The response header echoes the correlation id and an error code. The body shape depends only on the api key, including when the error is set, so the peer can always read the next frame.

## Replication

Followers pull. A fetcher thread on each broker walks the partitions where it is not the leader and sends a fetch with its own broker id and its log end as the offset. The leader remembers that offset as the follower's log end.

The high watermark is the first offset that is not fully replicated. It is the minimum log end among in-sync replicas, and it only moves forward. Consumers pass `replicaId = -1` and are limited to the watermark. Followers are allowed to read up to the leader's log end, which is how they catch up.

A follower that has not fetched yet stays in the in-sync set for `replica.lag.time.max.ms`. After that, or as soon as it is no longer a live broker, the leader drops it and the watermark can move with the replicas that remain. A follower that catches up is added back.

When this broker is told it is the new leader, it keeps the local log end and moves the watermark there. Those bytes were fetched from the previous leader, so they are a prefix of that leader's log. Truncating back to the previous watermark would drop records the old leader had already acknowledged.

## Controller

`Controller.createTopic` checks the name, the partition count, and that the replication factor is between 1 and the number of live brokers. Replicas are assigned round-robin: partition `p` starts at broker `p % n` and takes the next `replicationFactor` live brokers. The first replica is the preferred leader.

`reconcile` runs after election and whenever the broker list changes. A partition whose leader is gone gets the first live member of its in-sync set, or the first live replica if none of the in-sync set remains. The leader itself updates the in-sync set with a versioned write, and it refuses the write if ZooKeeper already shows a different leader.

## Failure

| Event | What happens |
| --- | --- |
| Follower stops | Its ephemeral node disappears. The leader shrinks the in-sync set. `acks=-1` completes with the remaining replicas. |
| Leader stops | The controller elects another in-sync replica. Clients refresh metadata and send to the new leader. |
| Controller stops | `/controller` disappears. Another broker creates it and reconciles. |
| Broker restarts | It opens the existing segments, truncates a torn tail, and rejoins as leader or follower from the current ZooKeeper state. |
