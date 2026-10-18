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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A big-endian reader over a fixed frame. Short reads become {@link Errors#INVALID_REQUEST}.
 */
public final class ByteReader {
    private final ByteBuffer buffer;

    public ByteReader(byte[] data) {
        this.buffer = ByteBuffer.wrap(data);
    }

    public ByteReader(byte[] data, int offset, int length) {
        this.buffer = ByteBuffer.wrap(data, offset, length);
    }

    public int remaining() {
        return buffer.remaining();
    }

    public byte readByte() {
        require(1);
        return buffer.get();
    }

    public short readShort() {
        require(2);
        return buffer.getShort();
    }

    public int readUnsignedShort() {
        return readShort() & 0xffff;
    }

    public int readInt() {
        require(4);
        return buffer.getInt();
    }

    public long readLong() {
        require(8);
        return buffer.getLong();
    }

    public String readString() {
        int length = readUnsignedShort();
        byte[] bytes = readBytes(length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public byte[] readNullableBytes() {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0) {
            throw new KafkaException(Errors.INVALID_REQUEST, "negative byte length " + length);
        }
        if (length > Limits.MAX_MESSAGE_BYTES) {
            throw new KafkaException(Errors.MESSAGE_TOO_LARGE, "byte field is " + length + " bytes");
        }
        return readBytes(length);
    }

    public List<Integer> readIntList() {
        int count = readInt();
        if (count < 0 || count > 100_000) {
            throw new KafkaException(Errors.INVALID_REQUEST, "invalid list length " + count);
        }
        List<Integer> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(readInt());
        }
        return values;
    }

    public int readCount(int max) {
        int count = readInt();
        if (count < 0 || count > max) {
            throw new KafkaException(Errors.INVALID_REQUEST, "invalid count " + count);
        }
        return count;
    }

    private byte[] readBytes(int length) {
        require(length);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    private void require(int bytes) {
        if (buffer.remaining() < bytes) {
            throw new KafkaException(Errors.INVALID_REQUEST, "truncated frame");
        }
    }
}
