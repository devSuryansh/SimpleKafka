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

import java.util.Collection;
import java.util.Map;

/**
 * A point-in-time view of brokers, the controller, and topic metadata.
 */
public final class ClusterSnapshot {
    private final int controllerId;
    private final Map<Integer, Node> nodes;
    private final Map<String, TopicInfo> topics;

    public ClusterSnapshot(int controllerId, Map<Integer, Node> nodes, Map<String, TopicInfo> topics) {
        this.controllerId = controllerId;
        this.nodes = Map.copyOf(nodes);
        this.topics = Map.copyOf(topics);
    }

    public static ClusterSnapshot empty() {
        return new ClusterSnapshot(-1, Map.of(), Map.of());
    }

    public int controllerId() {
        return controllerId;
    }

    public Node controller() {
        return controllerId < 0 ? null : nodes.get(controllerId);
    }

    public Collection<Node> nodes() {
        return nodes.values();
    }

    public Node node(int id) {
        return nodes.get(id);
    }

    public Collection<TopicInfo> topics() {
        return topics.values();
    }

    public TopicInfo topic(String name) {
        return topics.get(name);
    }

    /**
     * Returns the current leader, or {@code null} when the topic, partition, or leader is unknown.
     */
    public Node leader(String topic, int partition) {
        TopicInfo info = topics.get(topic);
        if (info == null) {
            return null;
        }
        PartitionInfo partitionInfo = info.partition(partition);
        if (partitionInfo == null || partitionInfo.leader() < 0) {
            return null;
        }
        return nodes.get(partitionInfo.leader());
    }
}
