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

package com.simplekafka.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;

/**
 * A small ZooKeeper client with one-shot watches re-armed after every event.
 *
 * <p>Callbacks never run on the ZooKeeper event thread. Doing synchronous ZooKeeper
 * work there deadlocks the client, so every notification is handed to a single
 * executor. Session expiry reconnects and replays the registered watches.
 */
public final class ZkClient implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);

    private final String connectString;
    private final int sessionTimeoutMs;
    private final ExecutorService callbacks = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "simplekafka-zk");
        thread.setDaemon(true);
        return thread;
    });
    private final List<Runnable> registrations = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object connectMonitor = new Object();

    private ZooKeeper zooKeeper;
    private volatile boolean connected;
    private Runnable beforeReplay = () -> {
    };
    private Runnable afterReplay = () -> {
    };

    public ZkClient(String connectString, int sessionTimeoutMs) {
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public void start() {
        connect();
    }

    /**
     * {@code beforeReplay} runs after the session is back and before watches fire,
     * so ephemeral registration is visible to the replay. {@code afterReplay} runs
     * once the caches have observed that registration.
     */
    public void onReconnect(Runnable beforeReplay, Runnable afterReplay) {
        this.beforeReplay = beforeReplay;
        this.afterReplay = afterReplay;
    }

    public void ensurePath(String path) {
        createNodes(path, new byte[0], false);
    }

    public void setPersistent(String path, byte[] data) {
        createNodes(path, data, true);
    }

    /**
     * Creates an ephemeral node. Returns {@code false} when the node already exists.
     */
    public boolean createEphemeral(String path, byte[] data) {
        try {
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException.NodeExistsException exists) {
            return false;
        } catch (KeeperException | InterruptedException exception) {
            throw failure("create " + path, exception);
        }
    }

    public boolean exists(String path) {
        try {
            return zooKeeper.exists(path, false) != null;
        } catch (KeeperException | InterruptedException exception) {
            throw failure("check " + path, exception);
        }
    }

    public byte[] getData(String path) {
        return getData(path, null);
    }

    public byte[] getData(String path, Stat stat) {
        try {
            return zooKeeper.getData(path, false, stat);
        } catch (KeeperException.NoNodeException missing) {
            return null;
        } catch (KeeperException | InterruptedException exception) {
            throw failure("read " + path, exception);
        }
    }

    /**
     * Conditional write. Returns {@code false} when the znode version moved.
     */
    public boolean setData(String path, byte[] data, int version) {
        try {
            zooKeeper.setData(path, data, version);
            return true;
        } catch (KeeperException.BadVersionException stale) {
            return false;
        } catch (KeeperException | InterruptedException exception) {
            throw failure("update " + path, exception);
        }
    }

    public List<String> getChildren(String path) {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException missing) {
            return List.of();
        } catch (KeeperException | InterruptedException exception) {
            throw failure("list " + path, exception);
        }
    }

    /**
     * Invokes {@code listener} with the current children, then again whenever they change.
     * The first call is synchronous.
     */
    public void watchChildren(String path, Consumer<List<String>> listener) {
        ChildWatch watch = new ChildWatch(path, listener);
        register(() -> watch.arm());
    }

    /**
     * Invokes {@code listener} with the current bytes, or {@code null} when the node is absent.
     */
    public void watchData(String path, Consumer<byte[]> listener) {
        DataWatch watch = new DataWatch(path, listener);
        register(() -> watch.arm());
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() != Event.EventType.None) {
            return;
        }
        Event.KeeperState state = event.getState();
        if (state == Event.KeeperState.SyncConnected) {
            synchronized (connectMonitor) {
                connected = true;
                connectMonitor.notifyAll();
            }
            LOG.info("Connected to ZooKeeper at {}", connectString);
        } else if (state == Event.KeeperState.Disconnected) {
            connected = false;
            LOG.warn("Disconnected from ZooKeeper at {}", connectString);
        } else if (state == Event.KeeperState.Expired) {
            connected = false;
            LOG.warn("ZooKeeper session expired, reconnecting");
            callbacks.execute(this::reconnect);
        } else if (state == Event.KeeperState.AuthFailed) {
            LOG.error("ZooKeeper authentication failed");
        } else {
            LOG.info("ZooKeeper state changed to {}", state);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        callbacks.shutdownNow();
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void register(Runnable registration) {
        synchronized (registrations) {
            registrations.add(registration);
        }
        registration.run();
    }

    private void connect() {
        try {
            synchronized (connectMonitor) {
                connected = false;
            }
            zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, this);
            awaitConnected();
        } catch (IOException exception) {
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "failed to connect to " + connectString, exception);
        }
    }

    private void reconnect() {
        if (closed.get()) {
            return;
        }
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            connect();
            beforeReplay.run();
            List<Runnable> snapshot;
            synchronized (registrations) {
                snapshot = new ArrayList<>(registrations);
            }
            for (Runnable registration : snapshot) {
                registration.run();
            }
            afterReplay.run();
        } catch (RuntimeException exception) {
            LOG.error("Failed to reconnect to ZooKeeper", exception);
        }
    }

    private void awaitConnected() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sessionTimeoutMs);
        synchronized (connectMonitor) {
            while (!connected) {
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0) {
                    throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR,
                            "timed out connecting to ZooKeeper at " + connectString);
                }
                try {
                    connectMonitor.wait(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "interrupted connecting to ZooKeeper",
                            interrupted);
                }
            }
        }
    }

    private void createNodes(String path, byte[] leafData, boolean overwriteLeaf) {
        String[] parts = path.split("/");
        StringBuilder current = new StringBuilder();
        try {
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].isEmpty()) {
                    continue;
                }
                current.append('/').append(parts[i]);
                String node = current.toString();
                boolean leaf = node.equals(normalize(path));
                byte[] payload = leaf ? leafData : new byte[0];
                Stat stat = zooKeeper.exists(node, false);
                if (stat == null) {
                    try {
                        zooKeeper.create(node, payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException exists) {
                        if (leaf && overwriteLeaf) {
                            zooKeeper.setData(node, payload, -1);
                        }
                    }
                } else if (leaf && overwriteLeaf) {
                    zooKeeper.setData(node, payload, -1);
                }
            }
        } catch (KeeperException | InterruptedException exception) {
            throw failure("write " + path, exception);
        }
    }

    private static String normalize(String path) {
        if (path.endsWith("/") && path.length() > 1) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private void readData(String path, Consumer<byte[]> listener, Watcher watcher) {
        if (closed.get()) {
            return;
        }
        try {
            listener.accept(zooKeeper.getData(path, watcher, null));
        } catch (KeeperException.NoNodeException missing) {
            try {
                // The node can appear between the failed read and this exists() call.
                // exists() does not fire for a node that is already there, so read it now.
                if (zooKeeper.exists(path, watcher) != null) {
                    try {
                        listener.accept(zooKeeper.getData(path, watcher, null));
                    } catch (KeeperException.NoNodeException gone) {
                        listener.accept(null);
                    }
                } else {
                    listener.accept(null);
                }
            } catch (KeeperException | InterruptedException exception) {
                LOG.warn("Failed to watch for creation of {}", path, exception);
            }
        } catch (KeeperException | InterruptedException exception) {
            LOG.warn("Failed to read {}", path, exception);
        }
    }

    private void readChildren(String path, Consumer<List<String>> listener, Watcher watcher) {
        if (closed.get()) {
            return;
        }
        try {
            listener.accept(zooKeeper.getChildren(path, watcher));
        } catch (KeeperException.NoNodeException missing) {
            listener.accept(List.of());
            try {
                zooKeeper.exists(path, event -> {
                    if (event.getType() == Event.EventType.NodeCreated) {
                        callbacks.execute(() -> readChildren(path, listener, watcher));
                    }
                });
            } catch (KeeperException | InterruptedException exception) {
                LOG.warn("Failed to watch for creation of {}", path, exception);
            }
        } catch (KeeperException | InterruptedException exception) {
            LOG.warn("Failed to list {}", path, exception);
        }
    }

    private static KafkaException failure(String action, Exception exception) {
        if (exception instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "ZooKeeper " + action + " failed", exception);
    }

    private final class DataWatch implements Watcher {
        private final String path;
        private final Consumer<byte[]> listener;

        private DataWatch(String path, Consumer<byte[]> listener) {
            this.path = path;
            this.listener = listener;
        }

        @Override
        public void process(WatchedEvent event) {
            if (!closed.get()) {
                callbacks.execute(this::arm);
            }
        }

        private void arm() {
            readData(path, listener, this);
        }
    }

    private final class ChildWatch implements Watcher {
        private final String path;
        private final Consumer<List<String>> listener;

        private ChildWatch(String path, Consumer<List<String>> listener) {
            this.path = path;
            this.listener = listener;
        }

        @Override
        public void process(WatchedEvent event) {
            if (!closed.get()) {
                callbacks.execute(this::arm);
            }
        }

        private void arm() {
            readChildren(path, listener, this);
        }
    }
}
