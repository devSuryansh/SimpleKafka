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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.LogRecord;
import com.simplekafka.common.Node;
import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;

/**
 * A simple consumer for one assigned partition.
 *
 * <p>The caller owns the position. {@link #poll(long)} reads from the current offset
 * and advances past the records it returns. There is no consumer-group coordinator:
 * use {@link #seek(long)} to resume from a stored offset.
 */
public final class Consumer implements Closeable {
    private static final int MAX_BYTES = 1024 * 1024;

    private final ClientConfig config;
    private final Metadata metadata;
    private String topic;
    private int partition = -1;
    private long position;

    public Consumer(String bootstrapServers) {
        this(ClientConfig.bootstrap(bootstrapServers).withClientId("simplekafka-consumer"));
    }

    public Consumer(ClientConfig config) {
        this.config = config;
        this.metadata = new Metadata(config);
    }

    public void assign(String topic, int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        this.topic = topic;
        this.partition = partition;
        this.position = 0L;
    }

    public void seek(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        this.position = offset;
    }

    public long position() {
        return position;
    }

    /**
     * Fetches the next records, waiting up to {@code timeoutMs} for some to arrive.
     * An empty list means the timeout elapsed with nothing new committed.
     */
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (topic == null) {
            throw new IllegalStateException("assign a topic-partition before polling");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMs));
        while (true) {
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMs <= 0) {
                return List.of();
            }
            metadata.refreshIfStale(List.of(topic));
            Node leader = metadata.snapshot().leader(topic, partition);
            if (leader == null) {
                metadata.refresh(List.of(topic));
                leader = metadata.snapshot().leader(topic, partition);
            }
            if (leader == null) {
                sleep(Math.min(remainingMs, config.retryBackoffMs()));
                continue;
            }
            int socketTimeout = (int) Math.max(1L, remainingMs);
            int wait = (int) Math.min(Math.max(0L, remainingMs - 50L), 1_000L);
            FetchRequest request = new FetchRequest(topic, partition, position, MAX_BYTES, wait,
                    FetchRequest.CONSUMER_REPLICA_ID);
            try {
                Metadata.Rpc rpc = metadata.send(leader, ApiKeys.FETCH, request.body(), socketTimeout);
                if (rpc.header().error().retriable()) {
                    metadata.refresh(List.of(topic));
                    continue;
                }
                if (rpc.header().error() != Errors.NONE) {
                    throw new KafkaException(rpc.header().error());
                }
                FetchResponse response = FetchResponse.read(rpc.body());
                if (response.records().isEmpty()) {
                    continue;
                }
                List<ConsumerRecord> records = new ArrayList<>(response.records().size());
                for (LogRecord record : response.records()) {
                    records.add(new ConsumerRecord(topic, partition, record.offset(), record.timestamp(),
                            record.key(), record.value()));
                }
                position = response.records().get(response.records().size() - 1).offset() + 1;
                return records;
            } catch (KafkaException exception) {
                if (!exception.error().retriable()) {
                    throw exception;
                }
                metadata.refresh(List.of(topic));
            }
        }
    }

    @Override
    public void close() {
        metadata.close();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(Math.max(1L, millis));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaException(Errors.REQUEST_TIMED_OUT, "interrupted while polling");
        }
    }
}
