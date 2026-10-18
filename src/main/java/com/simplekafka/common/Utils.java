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

import java.io.Closeable;
import java.util.regex.Pattern;

/**
 * Small helpers shared by the broker and the clients.
 */
public final class Utils {
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    private Utils() {
    }

    /**
     * Rejects topic names that would be ambiguous as a path segment.
     */
    public static void requireTopicName(String topic) {
        if (topic == null || !TOPIC_NAME.matcher(topic).matches() || ".".equals(topic) || "..".equals(topic)) {
            throw new KafkaException(Errors.INVALID_TOPIC_EXCEPTION, "invalid topic: " + topic);
        }
    }

    /**
     * Clears the sign bit so a hash can be used as an array index.
     */
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    /**
     * Murmur2 hash, the same mixing function Kafka uses for the default key partitioner.
     * Keyed records for the same key land on the same partition.
     */
    public static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff)
                    + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16)
                    + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int remaining = length % 4;
        int offset = length & ~3;
        if (remaining == 3) {
            h ^= (data[offset + 2] & 0xff) << 16;
        }
        if (remaining >= 2) {
            h ^= (data[offset + 1] & 0xff) << 8;
        }
        if (remaining >= 1) {
            h ^= data[offset] & 0xff;
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Closing is best-effort. The caller is already failing or shutting down.
        }
    }
}
