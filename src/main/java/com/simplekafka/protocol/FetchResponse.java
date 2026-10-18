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
 * Records from a fetch, plus the leader's high watermark and log end offset.
 *
 * <p>The high watermark is the first offset that is not yet committed. Consumers
 * can read every offset below it. Followers use both watermarks to detect divergence.
 */
public final class FetchResponse {
    private final long highWatermark;
    private final long logEndOffset;
    private final List<LogRecord> records;

    public FetchResponse(long highWatermark, long logEndOffset, List<LogRecord> records) {
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
        this.records = List.copyOf(records);
    }

    public static FetchResponse empty() {
        return new FetchResponse(0L, 0L, List.of());
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long logEndOffset() {
        return logEndOffset;
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
        buf.writeLong(highWatermark);
        buf.writeLong(logEndOffset);
        buf.writeInt(records.size());
        for (LogRecord record : records) {
            buf.writeLong(record.offset());
            buf.writeLong(record.timestamp());
            buf.writeNullableBytes(record.key());
            buf.writeNullableBytes(record.value());
        }
    }

    public static FetchResponse read(ByteReader reader) {
        long highWatermark = reader.readLong();
        long logEndOffset = reader.readLong();
        int count = reader.readCount(Limits.MAX_RECORD_BATCH);
        List<LogRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = reader.readLong();
            long timestamp = reader.readLong();
            byte[] key = reader.readNullableBytes();
            byte[] value = reader.readNullableBytes();
            records.add(new LogRecord(offset, timestamp, key, value));
        }
        return new FetchResponse(highWatermark, logEndOffset, records);
    }
}
