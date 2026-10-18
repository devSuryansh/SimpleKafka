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

package com.simplekafka.protocol;

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.ByteReader;

/**
 * A read from one partition.
 *
 * <p>{@code replicaId} is {@code -1} for a consumer. A follower sends its own broker id.
 * Consumers are limited to the high watermark. Followers may read through to the log end
 * so replication can make progress.
 */
public final class FetchRequest {
    public static final int CONSUMER_REPLICA_ID = -1;

    private final String topic;
    private final int partition;
    private final long offset;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int replicaId;

    public FetchRequest(String topic, int partition, long offset, int maxBytes, int maxWaitMs, int replicaId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.replicaId = replicaId;
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

    public int maxBytes() {
        return maxBytes;
    }

    public int maxWaitMs() {
        return maxWaitMs;
    }

    public int replicaId() {
        return replicaId;
    }

    public boolean fromFollower() {
        return replicaId >= 0;
    }

    public byte[] body() {
        ByteBuf buf = new ByteBuf();
        write(buf);
        return buf.toByteArray();
    }

    public void write(ByteBuf buf) {
        buf.writeString(topic);
        buf.writeInt(partition);
        buf.writeLong(offset);
        buf.writeInt(maxBytes);
        buf.writeInt(maxWaitMs);
        buf.writeInt(replicaId);
    }

    public static FetchRequest read(ByteReader reader) {
        return new FetchRequest(
                reader.readString(),
                reader.readInt(),
                reader.readLong(),
                reader.readInt(),
                reader.readInt(),
                reader.readInt());
    }
}
