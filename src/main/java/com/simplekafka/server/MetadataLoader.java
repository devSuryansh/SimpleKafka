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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.Node;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;
import com.simplekafka.common.TopicPartition;
import com.simplekafka.zookeeper.ZkClient;
import com.simplekafka.zookeeper.ZkMetadata;
import com.simplekafka.zookeeper.ZkPaths;

/**
 * Projects the ZooKeeper tree into {@link MetadataCache} and tells the replica
 * manager when a partition's leader changes.
 */
final class MetadataLoader {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataLoader.class);

    private final int brokerId;
    private final ZkClient zk;
    private final MetadataCache cache;
    private final ReplicaManager replicas;
    private final Controller controller;
    private final Map<String, ZkMetadata.TopicAssignment> assignments = new HashMap<>();
    private final Map<TopicPartition, ZkMetadata.PartitionState> states = new HashMap<>();
    private final Set<String> watchedTopics = new HashSet<>();
    private final Set<TopicPartition> watchedStates = new HashSet<>();

    MetadataLoader(int brokerId, ZkClient zk, MetadataCache cache, ReplicaManager replicas, Controller controller) {
        this.brokerId = brokerId;
        this.zk = zk;
        this.cache = cache;
        this.replicas = replicas;
        this.controller = controller;
    }

    void start() {
        zk.watchChildren(ZkPaths.BROKER_IDS, this::onBrokers);
        zk.watchData(ZkPaths.CONTROLLER, this::onController);
        zk.watchChildren(ZkPaths.TOPICS, this::onTopics);
    }

    private void onBrokers(List<String> ids) {
        Map<Integer, Node> next = new HashMap<>();
        for (String idText : ids) {
            int id;
            try {
                id = Integer.parseInt(idText);
            } catch (NumberFormatException exception) {
                continue;
            }
            byte[] data = zk.getData(ZkPaths.broker(id));
            if (data == null) {
                continue;
            }
            try {
                next.put(id, ZkMetadata.parseBroker(id, data));
            } catch (RuntimeException exception) {
                LOG.warn("Ignoring broker {} with unreadable registration", id, exception);
            }
        }
        cache.setBrokers(next);
        LOG.info("Broker {} sees {} live brokers", brokerId, next.size());
        if (controller.isActive()) {
            controller.reconcile();
        }
    }

    private void onController(byte[] data) {
        if (data == null) {
            cache.setControllerId(-1);
            if (controller.isActive()) {
                controller.resign();
            }
            controller.elect();
            return;
        }
        int id = ZkMetadata.parseController(data);
        cache.setControllerId(id);
        if (id != brokerId) {
            controller.resign();
        }
    }

    private void onTopics(List<String> names) {
        for (String name : names) {
            if (watchedTopics.add(name)) {
                zk.watchData(ZkPaths.topic(name), data -> onTopic(name, data));
            }
        }
    }

    private void onTopic(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        ZkMetadata.TopicAssignment assignment;
        try {
            assignment = ZkMetadata.parseTopic(topic, data);
        } catch (RuntimeException exception) {
            LOG.warn("Ignoring unreadable topic {}", topic, exception);
            return;
        }
        assignments.put(topic, assignment);
        for (int partition = 0; partition < assignment.partitions(); partition++) {
            TopicPartition tp = new TopicPartition(topic, partition);
            if (watchedStates.add(tp)) {
                int id = partition;
                zk.watchData(ZkPaths.partitionState(topic, partition), bytes -> onState(topic, id, bytes));
            }
        }
        publish(topic);
    }

    private void onState(String topic, int partition, byte[] data) {
        TopicPartition tp = new TopicPartition(topic, partition);
        if (data == null || data.length == 0) {
            states.remove(tp);
        } else {
            try {
                states.put(tp, ZkMetadata.parseState(data));
            } catch (RuntimeException exception) {
                LOG.warn("Ignoring unreadable state for {}", tp, exception);
                return;
            }
        }
        publish(topic);
        apply(topic, partition);
    }

    private void publish(String topic) {
        ZkMetadata.TopicAssignment assignment = assignments.get(topic);
        if (assignment == null) {
            return;
        }
        Map<Integer, PartitionInfo> partitions = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : assignment.replicas().entrySet()) {
            ZkMetadata.PartitionState state = states.get(new TopicPartition(topic, entry.getKey()));
            int leader = state == null ? -1 : state.leader();
            List<Integer> isr = state == null ? List.of() : state.isr();
            partitions.put(entry.getKey(), new PartitionInfo(entry.getKey(), leader, entry.getValue(), isr));
        }
        cache.putTopic(new TopicInfo(topic, (short) 0, partitions));
    }

    private void apply(String topic, int partition) {
        ZkMetadata.TopicAssignment assignment = assignments.get(topic);
        if (assignment == null) {
            return;
        }
        List<Integer> replicasForPartition = assignment.replicas().get(partition);
        if (replicasForPartition == null || !replicasForPartition.contains(brokerId)) {
            return;
        }
        ZkMetadata.PartitionState state = states.get(new TopicPartition(topic, partition));
        int leader = state == null ? -1 : state.leader();
        List<Integer> isr = state == null || state.isr().isEmpty() ? replicasForPartition : state.isr();
        replicas.onPartitionUpdated(topic, partition, leader, replicasForPartition, isr);
    }
}
