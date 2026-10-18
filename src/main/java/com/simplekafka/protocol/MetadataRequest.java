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

/**
 * Asks for cluster membership and, optionally, a subset of topics.
 * An empty topic list means "every topic the broker knows about".
 */
public final class MetadataRequest {
    private final List<String> topics;

    public MetadataRequest(List<String> topics) {
        this.topics = List.copyOf(topics);
    }

    public List<String> topics() {
        return topics;
    }

    public byte[] body() {
        ByteBuf buf = new ByteBuf();
        write(buf);
        return buf.toByteArray();
    }

    public void write(ByteBuf buf) {
        buf.writeInt(topics.size());
        for (String topic : topics) {
            buf.writeString(topic);
        }
    }

    public static MetadataRequest read(ByteReader reader) {
        int count = reader.readCount(10_000);
        List<String> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            topics.add(reader.readString());
        }
        return new MetadataRequest(topics);
    }
}
