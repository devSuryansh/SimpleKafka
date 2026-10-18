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

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.zookeeper.ZkClient;
import com.simplekafka.zookeeper.ZkMetadata;
import com.simplekafka.zookeeper.ZkPaths;

/**
 * One broker: a socket server, a replica manager, and a candidate controller.
 *
 * <p>Startup binds the port before registering with ZooKeeper, so the ephemeral
 * node advertises the port the process actually received. Client traffic is
 * accepted only after that registration and the initial metadata load.
 */
public final class BrokerServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerServer.class);

    private final BrokerConfig config;
    private final Object lifecycle = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final CountDownLatch shutdown = new CountDownLatch(1);

    private ZkClient zk;
    private MetadataCache cache;
    private Controller controller;
    private ReplicaManager replicas;
    private SocketServer sockets;
    private int port;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
    }

    public void start() {
        synchronized (lifecycle) {
            if (stopped.get()) {
                throw new IllegalStateException("broker has been shut down");
            }
            if (!running.compareAndSet(false, true)) {
                throw new IllegalStateException("broker is already running");
            }
            try {
                cache = new MetadataCache();
                sockets = new SocketServer(config.host());
                sockets.bind(config.port());
                port = sockets.port();
                zk = new ZkClient(config.zkConnect(), config.zkSessionTimeoutMs());
                zk.start();
                zk.ensurePath(ZkPaths.BROKER_IDS);
                zk.ensurePath(ZkPaths.TOPICS);
                if (!ensureRegistration()) {
                    throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR,
                            "broker id " + config.brokerId() + " is already registered");
                }
                zk.onReconnect(this::reregisterBroker, this::onSessionRestored);
                controller = new Controller(config.brokerId(), zk, cache);
                replicas = new ReplicaManager(config.brokerId(), config, cache, zk);
                sockets.setApis(new KafkaApis(replicas, controller, cache));
                replicas.start();
                new MetadataLoader(config.brokerId(), zk, cache, replicas, controller).start();
                controller.elect();
                sockets.start();
                LOG.info("SimpleKafka broker {} listening on {}:{}", config.brokerId(), config.host(), port);
            } catch (Exception exception) {
                cleanup();
                running.set(false);
                stopped.set(true);
                shutdown.countDown();
                if (exception instanceof KafkaException kafkaException) {
                    throw kafkaException;
                }
                throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "failed to start broker", exception);
            }
        }
    }

    public int brokerId() {
        return config.brokerId();
    }

    public int port() {
        return port;
    }

    public boolean isController() {
        return controller != null && controller.isActive();
    }

    public int liveBrokerCount() {
        return cache == null ? 0 : cache.brokerCount();
    }

    /**
     * Leader broker id for a partition, or {@code -1} when the broker does not know it yet.
     */
    public int leader(String topic, int partition) {
        if (cache == null) {
            return -1;
        }
        PartitionInfo info = cache.partition(topic, partition);
        return info == null ? -1 : info.leader();
    }

    public void awaitShutdown() throws InterruptedException {
        shutdown.await();
    }

    @Override
    public void close() {
        synchronized (lifecycle) {
            if (!stopped.compareAndSet(false, true)) {
                return;
            }
            running.set(false);
            cleanup();
            shutdown.countDown();
            LOG.info("SimpleKafka broker {} stopped", config.brokerId());
        }
    }

    private void cleanup() {
        if (sockets != null) {
            sockets.close();
            sockets = null;
        }
        if (replicas != null) {
            replicas.shutdown();
            replicas = null;
        }
        if (zk != null) {
            zk.close();
            zk = null;
        }
    }

    private void reregisterBroker() {
        if (!ensureRegistration()) {
            LOG.error("Broker id {} is already registered with different data", config.brokerId());
        }
    }

    /**
     * @return {@code false} when another process already owns this broker id
     */
    private boolean ensureRegistration() {
        byte[] data = ZkMetadata.broker(config.host(), port);
        String path = ZkPaths.broker(config.brokerId());
        if (zk.createEphemeral(path, data)) {
            LOG.info("Registered broker {} at {}:{}", config.brokerId(), config.host(), port);
            return true;
        }
        byte[] existing = zk.getData(path);
        if (existing != null && Arrays.equals(existing, data)) {
            LOG.info("Broker {} is already registered at {}:{}", config.brokerId(), config.host(), port);
            return true;
        }
        return false;
    }

    private void onSessionRestored() {
        if (controller != null) {
            controller.resign();
            controller.elect();
        }
    }
}
