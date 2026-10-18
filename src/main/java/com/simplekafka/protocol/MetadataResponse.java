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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.ByteReader;
import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Node;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;

/**
 * Cluster metadata. Unknown topic names come back with a per-topic error rather than
 * failing the whole response, matching the way clients probe for a topic.
 */
public final class MetadataResponse {
    private final ClusterSnapshot cluster;

    public MetadataResponse(ClusterSnapshot cluster) {
        this.cluster = cluster;
    }

    public ClusterSnapshot cluster() {
        return cluster;
    }

    public byte[] body() {
        ByteBuf buf = new ByteBuf();
        write(buf);
        return buf.toByteArray();
    }

    public void write(ByteBuf buf) {
        buf.writeInt(cluster.controllerId());
        buf.writeInt(cluster.nodes().size());
        for (Node node : cluster.nodes()) {
            buf.writeInt(node.id());
            buf.writeString(node.host());
            buf.writeInt(node.port());
        }
        buf.writeInt(cluster.topics().size());
        for (TopicInfo topic : cluster.topics()) {
            buf.writeString(topic.name());
            buf.writeShort(topic.error());
            buf.writeInt(topic.partitionCount());
            for (PartitionInfo partition : topic.partitions()) {
                buf.writeInt(partition.partition());
                buf.writeInt(partition.leader());
                buf.writeIntList(partition.replicas());
                buf.writeIntList(partition.isr());
            }
        }
    }

    public static MetadataResponse read(ByteReader reader) {
        int controllerId = reader.readInt();
        int brokerCount = reader.readCount(10_000);
        Map<Integer, Node> nodes = new LinkedHashMap<>();
        for (int i = 0; i < brokerCount; i++) {
            int id = reader.readInt();
            String host = reader.readString();
            int port = reader.readInt();
            nodes.put(id, new Node(id, host, port));
        }
        int topicCount = reader.readCount(10_000);
        Map<String, TopicInfo> topics = new LinkedHashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String name = reader.readString();
            short error = reader.readShort();
            int partitionCount = reader.readCount(10_000);
            List<PartitionInfo> partitions = new ArrayList<>(partitionCount);
            for (int p = 0; p < partitionCount; p++) {
                int partition = reader.readInt();
                int leader = reader.readInt();
                List<Integer> replicas = reader.readIntList();
                List<Integer> isr = reader.readIntList();
                partitions.add(new PartitionInfo(partition, leader, replicas, isr));
            }
            Map<Integer, PartitionInfo> byId = new LinkedHashMap<>();
            for (PartitionInfo info : partitions) {
                byId.put(info.partition(), info);
            }
            topics.put(name, new TopicInfo(name, error, byId));
        }
        return new MetadataResponse(new ClusterSnapshot(controllerId, nodes, topics));
    }
}
