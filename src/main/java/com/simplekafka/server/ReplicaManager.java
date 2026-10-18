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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.ByteReader;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.LogRecord;
import com.simplekafka.common.Node;
import com.simplekafka.common.TopicPartition;
import com.simplekafka.network.FramedClient;
import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.Protocol;
import com.simplekafka.protocol.RequestHeader;
import com.simplekafka.protocol.ResponseHeader;
import com.simplekafka.storage.PartitionLog;
import com.simplekafka.zookeeper.ZkClient;
import com.simplekafka.zookeeper.ZkMetadata;
import com.simplekafka.zookeeper.ZkPaths;

/**
 * Local logs plus leader-follower replication.
 *
 * <p>Followers pull from the leader, the same direction Kafka uses. The leader's high
 * watermark advances to the minimum log end of the in-sync replicas, so a record is
 * visible to consumers only after those replicas have stored it. {@code acks=1} returns
 * as soon as the leader has the record. {@code acks=-1} waits for the watermark.
 */
final class ReplicaManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaManager.class);

    private final int brokerId;
    private final BrokerConfig config;
    private final MetadataCache cache;
    private final ZkClient zk;
    private final FramedClient client = new FramedClient();
    private final Map<TopicPartition, Replica> replicas = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread fetcher;

    ReplicaManager(int brokerId, BrokerConfig config, MetadataCache cache, ZkClient zk) {
        this.brokerId = brokerId;
        this.config = config;
        this.cache = cache;
        this.zk = zk;
    }

    void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        fetcher = new Thread(this::fetchLoop, "simplekafka-fetcher-" + brokerId);
        fetcher.setDaemon(true);
        fetcher.start();
    }

    void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (fetcher != null) {
            fetcher.interrupt();
            try {
                fetcher.join(2_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        for (Replica replica : replicas.values()) {
            replica.log.close();
        }
        client.close();
    }

    void onPartitionUpdated(String topic, int partition, int leader, List<Integer> replicaIds, List<Integer> isr) {
        if (!replicaIds.contains(brokerId)) {
            return;
        }
        Replica replica = open(topic, partition);
        synchronized (replica) {
            replica.replicas = List.copyOf(replicaIds);
            if (leader == brokerId) {
                if (replica.role != Role.LEADER) {
                    // A follower's log is a prefix of the previous leader. Records past the
                    // local watermark were still fetched from that leader, and acks=-1 can
                    // return as soon as the leader observes the fetch. Keep them.
                    replica.log.updateHighWatermark(replica.log.logEndOffset());
                    replica.role = Role.LEADER;
                    replica.leaderId = brokerId;
                    replica.isr = isr.isEmpty() ? List.of(brokerId) : List.copyOf(isr);
                    replica.leaderSinceMs = System.currentTimeMillis();
                    replica.followers.clear();
                    recompute(replica);
                    LOG.info("Broker {} is leader for {}-{}", brokerId, topic, partition);
                }
                return;
            }
            if (replica.role != Role.FOLLOWER || replica.leaderId != leader) {
                replica.role = leader < 0 ? Role.OFFLINE : Role.FOLLOWER;
                replica.leaderId = leader;
                replica.isr = List.copyOf(isr);
                LOG.info("Broker {} is {} for {}-{} (leader {})", brokerId, replica.role, topic, partition, leader);
            }
        }
    }

    long produce(String topic, int partition, List<LogRecord> records, short acks, int timeoutMs) {
        if (acks != -1 && acks != 0 && acks != 1) {
            throw new KafkaException(Errors.INVALID_REQUIRED_ACKS, "acks must be -1, 0, or 1");
        }
        Replica replica = requireLeader(topic, partition);
        long base;
        synchronized (replica) {
            if (replica.role != Role.LEADER) {
                throw notLeader(replica);
            }
            base = replica.log.appendAsLeader(records);
            recompute(replica);
        }
        if (acks == -1) {
            boolean committed = replica.log.awaitHighWatermark(base + records.size(), Math.max(0, timeoutMs));
            if (!committed) {
                throw new KafkaException(Errors.NOT_ENOUGH_REPLICAS,
                        "timed out waiting for in-sync replicas");
            }
        }
        return base;
    }

    FetchResult fetch(String topic, int partition, long offset, int maxBytes, int maxWaitMs, int replicaId) {
        Replica replica = replicas.get(new TopicPartition(topic, partition));
        if (replica == null) {
            if (cache.topic(topic) == null) {
                throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
            throw new KafkaException(Errors.NOT_LEADER_OR_FOLLOWER);
        }
        synchronized (replica) {
            if (replica.role != Role.LEADER) {
                throw notLeader(replica);
            }
            if (replicaId >= 0) {
                noteFollower(replica, replicaId, offset);
                recompute(replica);
            }
        }
        long logEnd = replica.log.logEndOffset();
        long watermark = replica.log.highWatermark();
        if (offset < replica.log.logStartOffset() || offset > logEnd) {
            return new FetchResult(Errors.OFFSET_OUT_OF_RANGE, watermark, logEnd, List.of());
        }
        long visibleEnd = replicaId >= 0 ? logEnd : watermark;
        List<LogRecord> records = replica.log.read(offset, maxBytes, visibleEnd);
        if (records.isEmpty() && maxWaitMs > 0) {
            if (replicaId >= 0) {
                replica.log.awaitLogEnd(offset, maxWaitMs);
            } else {
                replica.log.awaitHighWatermark(offset + 1, maxWaitMs);
            }
            synchronized (replica) {
                if (replica.role != Role.LEADER) {
                    throw notLeader(replica);
                }
            }
            logEnd = replica.log.logEndOffset();
            watermark = replica.log.highWatermark();
            visibleEnd = replicaId >= 0 ? logEnd : watermark;
            records = replica.log.read(offset, maxBytes, visibleEnd);
        }
        return new FetchResult(Errors.NONE, replica.log.highWatermark(), replica.log.logEndOffset(), records);
    }

    private Replica requireLeader(String topic, int partition) {
        Replica replica = replicas.get(new TopicPartition(topic, partition));
        if (replica == null) {
            if (cache.topic(topic) == null) {
                throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
            throw new KafkaException(Errors.NOT_LEADER_OR_FOLLOWER);
        }
        return replica;
    }

    private static KafkaException notLeader(Replica replica) {
        if (replica.leaderId < 0) {
            return new KafkaException(Errors.LEADER_NOT_AVAILABLE);
        }
        return new KafkaException(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    private Replica open(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return replicas.computeIfAbsent(tp, key -> {
            Path dir = config.logDir().resolve(topic + "-" + partition);
            return new Replica(key, PartitionLog.open(dir, config.logConfig()));
        });
    }

    private void fetchLoop() {
        while (running.get()) {
            boolean progressed = false;
            for (Replica replica : List.copyOf(replicas.values())) {
                if (!running.get()) {
                    return;
                }
                try {
                    if (fetchOnce(replica)) {
                        progressed = true;
                    }
                } catch (Exception exception) {
                    LOG.debug("Replica fetch failed for {}", replica.tp, exception);
                }
            }
            if (!progressed) {
                try {
                    Thread.sleep(config.replicaFetchMaxWaitMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private boolean fetchOnce(Replica replica) {
        int leaderId;
        synchronized (replica) {
            if (replica.role != Role.FOLLOWER || replica.leaderId < 0 || replica.leaderId == brokerId) {
                return false;
            }
            leaderId = replica.leaderId;
        }
        Node leader = cache.broker(leaderId);
        if (leader == null) {
            return false;
        }
        long offset = replica.log.logEndOffset();
        int wait = config.replicaFetchMaxWaitMs();
        FetchRequest request = new FetchRequest(replica.tp.topic(), replica.tp.partition(), offset,
                1024 * 1024, wait, brokerId);
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, correlation(), "replica-" + brokerId);
        byte[] responseBytes = client.send(leader, Protocol.request(header, request.body()), wait + 2_000);
        ByteReader reader = new ByteReader(responseBytes);
        ResponseHeader responseHeader = ResponseHeader.read(reader);
        FetchResponse response = FetchResponse.read(reader);
        if (responseHeader.error() == Errors.NOT_LEADER_OR_FOLLOWER
                || responseHeader.error() == Errors.LEADER_NOT_AVAILABLE) {
            return false;
        }
        synchronized (replica) {
            if (replica.role != Role.FOLLOWER || replica.leaderId != leaderId) {
                return false;
            }
            if (responseHeader.error() == Errors.OFFSET_OUT_OF_RANGE) {
                if (replica.log.logEndOffset() > response.logEndOffset()) {
                    replica.log.truncateTo(response.highWatermark());
                }
                return false;
            }
            if (responseHeader.error() != Errors.NONE) {
                return false;
            }
            if (!response.records().isEmpty()) {
                replica.log.appendAsFollower(response.records());
            }
            replica.log.updateHighWatermark(Math.min(response.highWatermark(), replica.log.logEndOffset()));
        }
        return !response.records().isEmpty();
    }

    private void noteFollower(Replica replica, int replicaId, long fetchOffset) {
        if (replicaId == brokerId) {
            return;
        }
        replica.followers.put(replicaId, new FollowerState(fetchOffset, System.currentTimeMillis()));
    }

    private void recompute(Replica replica) {
        long now = System.currentTimeMillis();
        List<Integer> nextIsr = new ArrayList<>();
        long minEnd = replica.log.logEndOffset();
        List<Integer> members = new ArrayList<>(replica.isr);
        if (!members.contains(brokerId)) {
            members.add(brokerId);
        }
        for (int replicaId : members) {
            if (replicaId == brokerId) {
                nextIsr.add(brokerId);
                continue;
            }
            if (!cache.isLive(replicaId)) {
                continue;
            }
            FollowerState state = replica.followers.get(replicaId);
            if (state == null) {
                if (now - replica.leaderSinceMs <= config.replicaLagTimeMaxMs()) {
                    nextIsr.add(replicaId);
                    minEnd = 0L;
                }
                continue;
            }
            if (now - state.lastFetchMs > config.replicaLagTimeMaxMs()) {
                continue;
            }
            nextIsr.add(replicaId);
            minEnd = Math.min(minEnd, state.leo);
        }
        for (int replicaId : replica.replicas) {
            if (nextIsr.contains(replicaId) || replicaId == brokerId || !cache.isLive(replicaId)) {
                continue;
            }
            FollowerState state = replica.followers.get(replicaId);
            if (state != null
                    && now - state.lastFetchMs <= config.replicaLagTimeMaxMs()
                    && state.leo >= replica.log.logEndOffset()) {
                nextIsr.add(replicaId);
                minEnd = Math.min(minEnd, state.leo);
            }
        }
        nextIsr.sort(Integer::compareTo);
        boolean changed = !nextIsr.equals(replica.isr);
        replica.isr = List.copyOf(nextIsr);
        replica.log.updateHighWatermark(minEnd);
        if (changed) {
            writeIsr(replica, nextIsr);
        }
    }

    private void writeIsr(Replica replica, List<Integer> isr) {
        String path = ZkPaths.partitionState(replica.tp.topic(), replica.tp.partition());
        Stat stat = new Stat();
        byte[] current = zk.getData(path, stat);
        if (current == null) {
            return;
        }
        try {
            if (ZkMetadata.parseState(current).leader() != brokerId) {
                return;
            }
        } catch (RuntimeException exception) {
            LOG.warn("Not updating unreadable state for {}", replica.tp);
            return;
        }
        boolean updated = zk.setData(path, ZkMetadata.state(brokerId, isr), stat.getVersion());
        if (updated) {
            LOG.info("ISR for {} is now {}", replica.tp, isr);
        }
    }

    private static int correlation() {
        return (int) (System.nanoTime() & Integer.MAX_VALUE);
    }

    enum Role {
        OFFLINE,
        FOLLOWER,
        LEADER
    }

    private static final class FollowerState {
        private final long leo;
        private final long lastFetchMs;

        private FollowerState(long leo, long lastFetchMs) {
            this.leo = leo;
            this.lastFetchMs = lastFetchMs;
        }
    }

    private static final class Replica {
        private final TopicPartition tp;
        private final PartitionLog log;
        private Role role = Role.OFFLINE;
        private int leaderId = -1;
        private List<Integer> replicas = List.of();
        private List<Integer> isr = List.of();
        private final Map<Integer, FollowerState> followers = new ConcurrentHashMap<>();
        private long leaderSinceMs;

        private Replica(TopicPartition tp, PartitionLog log) {
            this.tp = tp;
            this.log = log;
        }
    }

    static final class FetchResult {
        private final Errors error;
        private final long highWatermark;
        private final long logEndOffset;
        private final List<LogRecord> records;

        FetchResult(Errors error, long highWatermark, long logEndOffset, List<LogRecord> records) {
            this.error = error;
            this.highWatermark = highWatermark;
            this.logEndOffset = logEndOffset;
            this.records = records;
        }

        Errors error() {
            return error;
        }

        long highWatermark() {
            return highWatermark;
        }

        long logEndOffset() {
            return logEndOffset;
        }

        List<LogRecord> records() {
            return records;
        }
    }
}
