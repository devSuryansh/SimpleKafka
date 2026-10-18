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

package com.simplekafka.common;

import java.util.Arrays;
import java.util.Objects;

/**
 * One record in a partition log.
 *
 * <p>An offset of {@code -1} means the leader has not assigned one yet. A timestamp
 * of {@code 0} asks the leader to stamp the record when it is appended.
 */
public final class LogRecord {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    public LogRecord(long offset, long timestamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key == null ? null : Arrays.copyOf(key, key.length);
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        return key == null ? null : Arrays.copyOf(key, key.length);
    }

    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LogRecord record)) {
            return false;
        }
        return offset == record.offset
                && timestamp == record.timestamp
                && Arrays.equals(key, record.key)
                && Arrays.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(offset, timestamp);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
