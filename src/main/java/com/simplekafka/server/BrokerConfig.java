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
import java.util.Properties;

import com.simplekafka.storage.LogConfig;

/**
 * Static configuration for one broker process.
 *
 * <p>{@code port} may be {@code 0}, in which case the operating system assigns a port
 * and that port is what gets registered in ZooKeeper.
 */
public final class BrokerConfig {
    private final int brokerId;
    private final String host;
    private final int port;
    private final String zkConnect;
    private final Path logDir;
    private final LogConfig logConfig;
    private final int replicaLagTimeMaxMs;
    private final int replicaFetchMaxWaitMs;
    private final int zkSessionTimeoutMs;

    private BrokerConfig(Builder builder) {
        this.brokerId = builder.brokerId;
        this.host = builder.host;
        this.port = builder.port;
        this.zkConnect = builder.zkConnect;
        this.logDir = builder.logDir;
        this.logConfig = builder.logConfig;
        this.replicaLagTimeMaxMs = builder.replicaLagTimeMaxMs;
        this.replicaFetchMaxWaitMs = builder.replicaFetchMaxWaitMs;
        this.zkSessionTimeoutMs = builder.zkSessionTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BrokerConfig fromProperties(Properties properties) {
        Builder builder = builder();
        builder.brokerId(integer(properties, "broker.id", -1));
        builder.host(value(properties, "host.name", "127.0.0.1"));
        builder.port(integer(properties, "port", 9092));
        builder.zkConnect(value(properties, "zookeeper.connect", "localhost:2181"));
        String logDir = properties.getProperty("log.dirs");
        if (logDir != null) {
            builder.logDir(Path.of(logDir));
        }
        builder.logConfig(new LogConfig(
                integer(properties, "log.segment.bytes", LogConfig.DEFAULT_SEGMENT_BYTES),
                integer(properties, "log.index.interval.bytes", LogConfig.DEFAULT_INDEX_INTERVAL_BYTES)));
        builder.replicaLagTimeMaxMs(integer(properties, "replica.lag.time.max.ms", 10_000));
        builder.replicaFetchMaxWaitMs(integer(properties, "replica.fetch.max.wait.ms", 500));
        builder.zkSessionTimeoutMs(integer(properties, "zookeeper.session.timeout.ms", 10_000));
        return builder.build();
    }

    public int brokerId() {
        return brokerId;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String zkConnect() {
        return zkConnect;
    }

    public Path logDir() {
        return logDir;
    }

    public LogConfig logConfig() {
        return logConfig;
    }

    public int replicaLagTimeMaxMs() {
        return replicaLagTimeMaxMs;
    }

    public int replicaFetchMaxWaitMs() {
        return replicaFetchMaxWaitMs;
    }

    public int zkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    private static String value(Properties properties, String key, String fallback) {
        String value = properties.getProperty(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int integer(Properties properties, String key, int fallback) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value.trim());
    }

    /**
     * Builds a {@link BrokerConfig}. {@code brokerId}, {@code zkConnect}, and {@code logDir} are required.
     */
    public static final class Builder {
        private int brokerId = -1;
        private String host = "127.0.0.1";
        private int port = 9092;
        private String zkConnect;
        private Path logDir;
        private LogConfig logConfig = LogConfig.defaults();
        private int replicaLagTimeMaxMs = 10_000;
        private int replicaFetchMaxWaitMs = 500;
        private int zkSessionTimeoutMs = 10_000;

        public Builder brokerId(int brokerId) {
            this.brokerId = brokerId;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder zkConnect(String zkConnect) {
            this.zkConnect = zkConnect;
            return this;
        }

        public Builder logDir(Path logDir) {
            this.logDir = logDir;
            return this;
        }

        public Builder logConfig(LogConfig logConfig) {
            this.logConfig = logConfig;
            return this;
        }

        public Builder replicaLagTimeMaxMs(int replicaLagTimeMaxMs) {
            this.replicaLagTimeMaxMs = replicaLagTimeMaxMs;
            return this;
        }

        public Builder replicaFetchMaxWaitMs(int replicaFetchMaxWaitMs) {
            this.replicaFetchMaxWaitMs = replicaFetchMaxWaitMs;
            return this;
        }

        public Builder zkSessionTimeoutMs(int zkSessionTimeoutMs) {
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            return this;
        }

        public BrokerConfig build() {
            if (brokerId < 0) {
                throw new IllegalArgumentException("broker.id is required");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host.name is required");
            }
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port is out of range");
            }
            if (zkConnect == null || zkConnect.isBlank()) {
                throw new IllegalArgumentException("zookeeper.connect is required");
            }
            if (logDir == null) {
                throw new IllegalArgumentException("log.dirs is required");
            }
            if (replicaLagTimeMaxMs <= 0 || replicaFetchMaxWaitMs < 0 || zkSessionTimeoutMs <= 0) {
                throw new IllegalArgumentException("timeouts must be positive");
            }
            return new BrokerConfig(this);
        }
    }
}
