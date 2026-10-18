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
 * Asks the controller to create a topic and place its replicas.
 */
public final class CreateTopicRequest {
    private final String topic;
    private final int partitions;
    private final short replicationFactor;

    public CreateTopicRequest(String topic, int partitions, short replicationFactor) {
        this.topic = topic;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public String topic() {
        return topic;
    }

    public int partitions() {
        return partitions;
    }

    public short replicationFactor() {
        return replicationFactor;
    }

    public byte[] body() {
        ByteBuf buf = new ByteBuf();
        write(buf);
        return buf.toByteArray();
    }

    public void write(ByteBuf buf) {
        buf.writeString(topic);
        buf.writeInt(partitions);
        buf.writeShort(replicationFactor);
    }

    public static CreateTopicRequest read(ByteReader reader) {
        return new CreateTopicRequest(reader.readString(), reader.readInt(), reader.readShort());
    }
}
