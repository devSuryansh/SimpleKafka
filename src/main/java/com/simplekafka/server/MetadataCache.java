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

package com.simplekafka.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Node;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;

/**
 * The broker's in-memory copy of the ZooKeeper metadata.
 */
public final class MetadataCache {
    private final Object lock = new Object();
    private final Map<Integer, Node> brokers = new HashMap<>();
    private final Map<String, TopicInfo> topics = new HashMap<>();
    private int controllerId = -1;

    public void setBrokers(Map<Integer, Node> next) {
        synchronized (lock) {
            brokers.clear();
            brokers.putAll(next);
        }
    }

    public void setControllerId(int controllerId) {
        synchronized (lock) {
            this.controllerId = controllerId;
        }
    }

    public void putTopic(TopicInfo topic) {
        synchronized (lock) {
            topics.put(topic.name(), topic);
        }
    }

    public boolean isLive(int brokerId) {
        synchronized (lock) {
            return brokers.containsKey(brokerId);
        }
    }

    public int brokerCount() {
        synchronized (lock) {
            return brokers.size();
        }
    }

    public List<Integer> liveBrokerIds() {
        synchronized (lock) {
            List<Integer> ids = new ArrayList<>(brokers.keySet());
            Collections.sort(ids);
            return ids;
        }
    }

    public Node broker(int brokerId) {
        synchronized (lock) {
            return brokers.get(brokerId);
        }
    }

    public int controllerId() {
        synchronized (lock) {
            return controllerId;
        }
    }

    public TopicInfo topic(String name) {
        synchronized (lock) {
            return topics.get(name);
        }
    }

    public PartitionInfo partition(String topic, int partition) {
        TopicInfo info = topic(topic);
        return info == null ? null : info.partition(partition);
    }

    public Collection<TopicInfo> topics() {
        synchronized (lock) {
            return List.copyOf(topics.values());
        }
    }

    public ClusterSnapshot snapshot() {
        synchronized (lock) {
            return new ClusterSnapshot(controllerId, new HashMap<>(brokers), new HashMap<>(topics));
        }
    }
}
