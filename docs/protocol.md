# Protocol

All multi-byte integers are big-endian. A frame on the socket is:

```
int32  size          bytes that follow
bytes  payload
```

`size` must be between 1 and 8 MiB. The payload is a header plus a body.

## Request header

```
int16  apiKey
int16  apiVersion     0
int32  correlationId
string clientId       int16 length, then UTF-8 bytes
```

| apiKey | Name |
| --- | --- |
| 0 | Produce |
| 1 | Fetch |
| 2 | Metadata |
| 3 | CreateTopic |

## Response header

```
int32  correlationId
int16  errorCode
```

Error codes use the same numbers as the well-known Kafka codes for the cases this broker produces (`NONE` is 0, `UNKNOWN_TOPIC_OR_PARTITION` is 3, `NOT_LEADER_OR_FOLLOWER` is 6, `NOT_ENOUGH_REPLICAS` is 19, `TOPIC_ALREADY_EXISTS` is 36, `NOT_CONTROLLER` is 41, and so on). The full set is `com.simplekafka.common.Errors`.

Strings are an unsigned 16-bit length plus UTF-8 bytes. Nullable byte arrays are a signed 32-bit length, where `-1` means null. A response body is always present for a known api key, so the caller can decode it after reading the error code.

## Produce (0)

Request:

```
string topic
int32  partition
int16  acks             1 or -1 (0 is accepted and treated as 1)
int32  timeoutMs
int32  recordCount
recordCount times:
    int64  timestamp    0 asks the leader to stamp the record
    bytes  key          nullable
    bytes  value        nullable
```

Response:

```
int64  baseOffset       -1 when the header error is set
```

The returned offset is the offset of the first record in the batch. Later records are contiguous.

## Fetch (1)

Request:

```
string topic
int32  partition
int64  offset
int32  maxBytes
int32  maxWaitMs
int32  replicaId        -1 for a consumer, otherwise the follower's broker id
```

Response:

```
int64  highWatermark
int64  logEndOffset
int32  recordCount
recordCount times:
    int64  offset
    int64  timestamp
    bytes  key
    bytes  value
```

`OFFSET_OUT_OF_RANGE` still carries the watermark and the log end, so a follower that has diverged can truncate. An empty success means the wait elapsed with nothing new. The first record is returned even if it is larger than `maxBytes`.

## Metadata (2)

Request:

```
int32  topicCount       0 means every topic
topicCount times:
    string topic
```

Response:

```
int32  controllerId     -1 if none
int32  brokerCount
brokerCount times:
    int32  id
    string host
    int32  port
int32  topicCount
topicCount times:
    string name
    int16  error
    int32  partitionCount
    partitionCount times:
        int32  partition
        int32  leader          -1 if offline
        int32  replicaCount
        replicaCount times int32
        int32  isrCount
        isrCount times int32
```

A name the broker has never heard of comes back with `UNKNOWN_TOPIC_OR_PARTITION` on that topic. The rest of the response is still valid.

## CreateTopic (3)

Request:

```
string topic
int32  partitions
int16  replicationFactor
```

Response:

```
string topic
```

Only the controller accepts this. Any other broker returns `NOT_CONTROLLER`. The admin client reads the controller from metadata and retries.
