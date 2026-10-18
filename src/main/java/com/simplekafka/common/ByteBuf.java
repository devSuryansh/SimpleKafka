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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * A growable big-endian buffer used by the wire protocol and the log format.
 */
public final class ByteBuf {
    private byte[] data;
    private int size;

    public ByteBuf() {
        this(64);
    }

    public ByteBuf(int capacity) {
        this.data = new byte[Math.max(16, capacity)];
    }

    public int size() {
        return size;
    }

    public void writeByte(int value) {
        ensure(1);
        data[size++] = (byte) value;
    }

    public void writeShort(int value) {
        ensure(2);
        data[size++] = (byte) (value >>> 8);
        data[size++] = (byte) value;
    }

    public void writeInt(int value) {
        ensure(4);
        data[size++] = (byte) (value >>> 24);
        data[size++] = (byte) (value >>> 16);
        data[size++] = (byte) (value >>> 8);
        data[size++] = (byte) value;
    }

    public void writeLong(long value) {
        writeInt((int) (value >>> 32));
        writeInt((int) value);
    }

    public void writeBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return;
        }
        ensure(bytes.length);
        System.arraycopy(bytes, 0, data, size, bytes.length);
        size += bytes.length;
    }

    public void writeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Short.MAX_VALUE) {
            throw new KafkaException(Errors.INVALID_REQUEST, "string exceeds " + Short.MAX_VALUE + " bytes");
        }
        writeShort(bytes.length);
        writeBytes(bytes);
    }

    /**
     * Writes a length-prefixed byte array. {@code null} is encoded as length {@code -1}.
     */
    public void writeNullableBytes(byte[] bytes) {
        if (bytes == null) {
            writeInt(-1);
            return;
        }
        writeInt(bytes.length);
        writeBytes(bytes);
    }

    public void writeIntList(List<Integer> values) {
        writeInt(values.size());
        for (int value : values) {
            writeInt(value);
        }
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(data, size);
    }

    private void ensure(int extra) {
        if (size + extra <= data.length) {
            return;
        }
        int next = data.length;
        while (next < size + extra) {
            next *= 2;
        }
        data = Arrays.copyOf(data, next);
    }
}
