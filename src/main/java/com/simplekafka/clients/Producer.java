/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simplekafka.clients;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.LogRecord;
import com.simplekafka.common.Node;
import com.simplekafka.common.TopicInfo;
import com.simplekafka.common.Utils;
import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.ProduceRequest;
import com.simplekafka.protocol.ProduceResponse;

/**
 * Synchronous producer.
 *
 * <p>Records with a key are partitioned with murmur2 so the same key stays on one
 * partition. Records without a key round-robin across the topic. The call blocks
 * until the broker acknowledges the record, refreshing metadata and retrying when
 * the leader moves.
 */
public final class Producer implements Closeable {
    private final ClientConfig config;
    private final Metadata metadata;
    private final AtomicInteger roundRobin = new AtomicInteger();

    public Producer(String bootstrapServers) {
        this(ClientConfig.bootstrap(bootstrapServers).withClientId("simplekafka-producer"));
    }

    public Producer(ClientConfig config) {
        this.config = config;
        this.metadata = new Metadata(config);
    }

    public SendResult send(String topic, String value) {
        return send(topic, null, null, bytes(value));
    }

    public SendResult send(String topic, String key, String value) {
        return send(topic, null, bytes(key), bytes(value));
    }

    /**
     * Sends one record. A null {@code partition} lets the producer choose.
     */
    public SendResult send(String topic, Integer partition, byte[] key, byte[] value) {
        Utils.requireTopicName(topic);
        KafkaException last = null;
        for (int attempt = 0; attempt <= config.retries(); attempt++) {
            try {
                return sendOnce(topic, partition, key, value);
            } catch (KafkaException exception) {
                last = exception;
                if (!exception.error().retriable() || attempt == config.retries()) {
                    throw exception;
                }
                metadata.refresh(List.of(topic));
                sleep(config.retryBackoffMs());
            }
        }
        throw last == null ? new KafkaException(Errors.REQUEST_TIMED_OUT) : last;
    }

    @Override
    public void close() {
        metadata.close();
    }

    private SendResult sendOnce(String topic, Integer partition, byte[] key, byte[] value) {
        metadata.refreshIfStale(List.of(topic));
        TopicInfo info = metadata.snapshot().topic(topic);
        if (info == null || info.partitionCount() == 0 || info.error() != Errors.NONE.code()) {
            metadata.refresh(List.of(topic));
            info = metadata.snapshot().topic(topic);
        }
        if (info == null || info.partitionCount() == 0) {
            throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic " + topic);
        }
        int chosen = partition == null ? choose(info, key) : partition;
        if (chosen < 0 || chosen >= info.partitionCount()) {
            throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "partition " + chosen + " is out of range");
        }
        Node leader = metadata.snapshot().leader(topic, chosen);
        if (leader == null) {
            throw new KafkaException(Errors.LEADER_NOT_AVAILABLE, "no leader for " + topic + "-" + chosen);
        }
        LogRecord record = new LogRecord(-1L, System.currentTimeMillis(), key, value);
        ProduceRequest request = new ProduceRequest(topic, chosen, config.acks(), config.requestTimeoutMs(),
                List.of(record));
        int timeout = config.requestTimeoutMs() + 1_000;
        Metadata.Rpc rpc = metadata.send(leader, ApiKeys.PRODUCE, request.body(), timeout);
        if (rpc.header().error() != Errors.NONE) {
            throw new KafkaException(rpc.header().error());
        }
        long offset = ProduceResponse.read(rpc.body()).baseOffset();
        return new SendResult(topic, chosen, offset);
    }

    private int choose(TopicInfo topic, byte[] key) {
        int count = topic.partitionCount();
        if (key == null) {
            return Math.floorMod(roundRobin.getAndIncrement(), count);
        }
        return Utils.toPositive(Utils.murmur2(key)) % count;
    }

    private static byte[] bytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaException(Errors.REQUEST_TIMED_OUT, "interrupted while retrying");
        }
    }
}
