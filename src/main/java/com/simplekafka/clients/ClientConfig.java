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

package com.simplekafka.clients;

import java.util.ArrayList;
import java.util.List;

import com.simplekafka.common.Node;

/**
 * Settings for a producer, consumer, or admin client.
 *
 * <p>Bootstrap servers are {@code host:port} pairs separated by commas. Any of them
 * can answer a metadata request.
 */
public final class ClientConfig {
    private final List<Node> bootstrapServers;
    private final String clientId;
    private final short acks;
    private final int retries;
    private final int retryBackoffMs;
    private final int requestTimeoutMs;

    private ClientConfig(List<Node> bootstrapServers, String clientId, short acks, int retries,
                         int retryBackoffMs, int requestTimeoutMs) {
        this.bootstrapServers = List.copyOf(bootstrapServers);
        this.clientId = clientId;
        this.acks = acks;
        this.retries = retries;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public static ClientConfig bootstrap(String servers) {
        return new ClientConfig(parse(servers), "simplekafka", (short) 1, 5, 100, 10_000);
    }

    public ClientConfig withClientId(String clientId) {
        return new ClientConfig(bootstrapServers, clientId, acks, retries, retryBackoffMs, requestTimeoutMs);
    }

    public ClientConfig withAcks(short acks) {
        return new ClientConfig(bootstrapServers, clientId, acks, retries, retryBackoffMs, requestTimeoutMs);
    }

    public ClientConfig withRetries(int retries) {
        return new ClientConfig(bootstrapServers, clientId, acks, retries, retryBackoffMs, requestTimeoutMs);
    }

    public ClientConfig withRequestTimeoutMs(int requestTimeoutMs) {
        return new ClientConfig(bootstrapServers, clientId, acks, retries, retryBackoffMs, requestTimeoutMs);
    }

    public List<Node> bootstrapServers() {
        return bootstrapServers;
    }

    public String clientId() {
        return clientId;
    }

    public short acks() {
        return acks;
    }

    public int retries() {
        return retries;
    }

    public int retryBackoffMs() {
        return retryBackoffMs;
    }

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public static List<Node> parse(String servers) {
        if (servers == null || servers.isBlank()) {
            throw new IllegalArgumentException("bootstrap servers are required");
        }
        List<Node> nodes = new ArrayList<>();
        for (String raw : servers.split(",")) {
            String entry = raw.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int colon = entry.lastIndexOf(':');
            if (colon <= 0 || colon == entry.length() - 1) {
                throw new IllegalArgumentException("expected host:port but got " + entry);
            }
            String host = entry.substring(0, colon);
            int port = Integer.parseInt(entry.substring(colon + 1));
            nodes.add(new Node(-1, host, port));
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("bootstrap servers are required");
        }
        return nodes;
    }
}
