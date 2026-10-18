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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;
import com.simplekafka.common.Utils;
import com.simplekafka.zookeeper.ZkClient;
import com.simplekafka.zookeeper.ZkMetadata;
import com.simplekafka.zookeeper.ZkPaths;

/**
 * Elects a single controller and, while it holds that role, places partitions.
 *
 * <p>Election is the ZooKeeper recipe: create an ephemeral {@code /controller} node.
 * The broker that creates it wins. Everyone else watches the node and tries again
 * when it disappears. The controller does not serve records itself. It only writes
 * the leader and in-sync set, and every broker applies that state from its own watch.
 */
public final class Controller {
    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    private static final int MAX_PARTITIONS = 1024;

    private final int brokerId;
    private final ZkClient zk;
    private final MetadataCache cache;
    private volatile boolean active;

    public Controller(int brokerId, ZkClient zk, MetadataCache cache) {
        this.brokerId = brokerId;
        this.zk = zk;
        this.cache = cache;
    }

    public boolean isActive() {
        return active;
    }

    public void resign() {
        active = false;
    }

    /**
     * Tries to become the controller. Safe to call more than once.
     */
    public synchronized void elect() {
        if (active) {
            reconcile();
            return;
        }
        boolean won = zk.createEphemeral(ZkPaths.CONTROLLER, ZkMetadata.controller(brokerId));
        if (won) {
            active = true;
            cache.setControllerId(brokerId);
            LOG.info("Broker {} elected controller", brokerId);
            reconcile();
            return;
        }
        active = false;
        byte[] data = zk.getData(ZkPaths.CONTROLLER);
        if (data != null) {
            cache.setControllerId(ZkMetadata.parseController(data));
            LOG.info("Broker {} sees controller {}", brokerId, cache.controllerId());
        }
    }

    public synchronized void createTopic(String name, int partitions, short replicationFactor) {
        Utils.requireTopicName(name);
        if (!active || !stillController()) {
            active = false;
            throw new KafkaException(Errors.NOT_CONTROLLER);
        }
        if (partitions < 1 || partitions > MAX_PARTITIONS) {
            throw new KafkaException(Errors.INVALID_PARTITIONS,
                    "partitions must be between 1 and " + MAX_PARTITIONS);
        }
        List<Integer> live = cache.liveBrokerIds();
        if (replicationFactor < 1 || replicationFactor > live.size()) {
            throw new KafkaException(Errors.INVALID_REPLICATION_FACTOR,
                    "replication factor " + replicationFactor + " is not in [1, " + live.size() + "]");
        }
        if (zk.exists(ZkPaths.topic(name))) {
            throw new KafkaException(Errors.TOPIC_ALREADY_EXISTS, "topic already exists: " + name);
        }
        Map<Integer, List<Integer>> assignment = assign(partitions, replicationFactor, live);
        zk.setPersistent(ZkPaths.topic(name), ZkMetadata.topic(partitions, replicationFactor, assignment));
        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            int leader = entry.getValue().get(0);
            zk.setPersistent(ZkPaths.partitionState(name, entry.getKey()),
                    ZkMetadata.state(leader, entry.getValue()));
        }
        LOG.info("Created topic {} with {} partitions, replication factor {}", name, partitions, replicationFactor);
    }

    /**
     * Gives every partition that lost its leader a new one from the remaining live replicas.
     */
    public synchronized void reconcile() {
        if (!active || !stillController()) {
            active = false;
            return;
        }
        for (TopicInfo topic : cache.topics()) {
            for (PartitionInfo partition : topic.partitions()) {
                if (partition.leader() >= 0 && cache.isLive(partition.leader())) {
                    continue;
                }
                int leader = firstLive(partition.isr());
                if (leader < 0) {
                    leader = firstLive(partition.replicas());
                }
                if (leader < 0) {
                    LOG.warn("No live replica for {}-{}", topic.name(), partition.partition());
                    continue;
                }
                List<Integer> isr = new ArrayList<>();
                for (int replica : partition.isr().isEmpty() ? partition.replicas() : partition.isr()) {
                    if (cache.isLive(replica)) {
                        isr.add(replica);
                    }
                }
                if (!isr.contains(leader)) {
                    isr.add(leader);
                }
                zk.setPersistent(ZkPaths.partitionState(topic.name(), partition.partition()),
                        ZkMetadata.state(leader, isr));
                LOG.info("Elected broker {} leader for {}-{}", leader, topic.name(), partition.partition());
            }
        }
    }

    private boolean stillController() {
        byte[] data = zk.getData(ZkPaths.CONTROLLER);
        return data != null && ZkMetadata.parseController(data) == brokerId;
    }

    private int firstLive(List<Integer> candidates) {
        for (int candidate : candidates) {
            if (cache.isLive(candidate)) {
                return candidate;
            }
        }
        return -1;
    }

    static Map<Integer, List<Integer>> assign(int partitions, int replicationFactor, List<Integer> brokers) {
        Map<Integer, List<Integer>> assignment = new LinkedHashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            List<Integer> replicas = new ArrayList<>(replicationFactor);
            int start = partition % brokers.size();
            for (int replica = 0; replica < replicationFactor; replica++) {
                replicas.add(brokers.get((start + replica) % brokers.size()));
            }
            assignment.put(partition, replicas);
        }
        return assignment;
    }
}
