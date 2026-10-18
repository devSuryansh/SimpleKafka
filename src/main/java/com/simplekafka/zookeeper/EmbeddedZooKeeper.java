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
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;

/**
 * An in-process ZooKeeper for local development and tests.
 *
 * <p>The admin server is disabled so several instances can share a machine without
 * fighting over port 8080.
 */
public final class EmbeddedZooKeeper implements Closeable {
    static {
        System.setProperty("zookeeper.admin.enableServer", "false");
        System.setProperty("zookeeper.snapshot.trust.empty", "true");
    }

    private final ServerCnxnFactory factory;
    private final int port;

    public EmbeddedZooKeeper(Path dataDir, int port, int tickTime) {
        try {
            Files.createDirectories(dataDir);
            ZooKeeperServer server = new ZooKeeperServer(dataDir.toFile(), dataDir.toFile(), tickTime);
            this.factory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", port), 1000);
            this.factory.startup(server);
            this.port = factory.getLocalPort();
        } catch (IOException | InterruptedException exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "failed to start ZooKeeper", exception);
        }
    }

    public int port() {
        return port;
    }

    public String connectString() {
        return "127.0.0.1:" + port;
    }

    @Override
    public void close() {
        if (factory != null) {
            factory.shutdown();
        }
    }
}
