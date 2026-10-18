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

import java.nio.charset.StandardCharsets;

/**
 * A record delivered to a consumer, with the topic and partition it was read from.
 */
public final class ConsumerRecord {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    public ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
