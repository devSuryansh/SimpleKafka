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

import java.util.ArrayList;
import java.util.List;

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.ByteReader;
import com.simplekafka.common.Limits;
import com.simplekafka.common.LogRecord;

/**
 * A batch of records for one topic-partition.
 *
 * <p>{@code acks} is {@code 1} to wait for the leader log, or {@code -1} to wait until
 * every in-sync replica has the batch. {@code 0} is accepted and treated as {@code 1};
 * the server still returns an offset so the framed connection stays in lockstep.
 */
public final class ProduceRequest {
    private final String topic;
    private final int partition;
    private final short acks;
    private final int timeoutMs;
    private final List<LogRecord> records;

    public ProduceRequest(String topic, int partition, short acks, int timeoutMs, List<LogRecord> records) {
        this.topic = topic;
        this.partition = partition;
        this.acks = acks;
        this.timeoutMs = timeoutMs;
        this.records = List.copyOf(records);
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public short acks() {
        return acks;
    }

    public int timeoutMs() {
        return timeoutMs;
    }

    public List<LogRecord> records() {
        return records;
    }

    public byte[] body() {
        ByteBuf buf = new ByteBuf();
        write(buf);
        return buf.toByteArray();
    }

    public void write(ByteBuf buf) {
        buf.writeString(topic);
        buf.writeInt(partition);
        buf.writeShort(acks);
        buf.writeInt(timeoutMs);
        buf.writeInt(records.size());
        for (LogRecord record : records) {
            buf.writeLong(record.timestamp());
            buf.writeNullableBytes(record.key());
            buf.writeNullableBytes(record.value());
        }
    }

    public static ProduceRequest read(ByteReader reader) {
        String topic = reader.readString();
        int partition = reader.readInt();
        short acks = reader.readShort();
        int timeoutMs = reader.readInt();
        int count = reader.readCount(Limits.MAX_RECORD_BATCH);
        List<LogRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long timestamp = reader.readLong();
            byte[] key = reader.readNullableBytes();
            byte[] value = reader.readNullableBytes();
            records.add(new LogRecord(-1L, timestamp, key, value));
        }
        return new ProduceRequest(topic, partition, acks, timeoutMs, records);
    }
}
