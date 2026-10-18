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

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.ByteReader;
import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.Node;
import com.simplekafka.network.FramedClient;
import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.MetadataRequest;
import com.simplekafka.protocol.MetadataResponse;
import com.simplekafka.protocol.Protocol;
import com.simplekafka.protocol.RequestHeader;
import com.simplekafka.protocol.ResponseHeader;

/**
 * Bootstrap connection, metadata cache, and correlation ids for one client.
 */
final class Metadata implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Metadata.class);
    private static final long MAX_AGE_MS = 30_000L;

    private final ClientConfig config;
    private final FramedClient client = new FramedClient();
    private final AtomicInteger correlation = new AtomicInteger();
    private volatile ClusterSnapshot snapshot = ClusterSnapshot.empty();
    private volatile long refreshedAt;

    Metadata(ClientConfig config) {
        this.config = config;
    }

    ClusterSnapshot snapshot() {
        return snapshot;
    }

    void refresh(List<String> topics) {
        KafkaException last = null;
        MetadataRequest request = new MetadataRequest(topics);
        for (Node bootstrap : config.bootstrapServers()) {
            Node target = preferKnown(bootstrap);
            try {
                Rpc rpc = send(target, ApiKeys.METADATA, request.body(), config.requestTimeoutMs());
                if (rpc.header.error() != Errors.NONE) {
                    throw new KafkaException(rpc.header.error());
                }
                snapshot = MetadataResponse.read(rpc.body).cluster();
                refreshedAt = System.currentTimeMillis();
                return;
            } catch (KafkaException exception) {
                last = exception;
                LOG.debug("Metadata request to {} failed", target, exception);
            }
        }
        if (last == null) {
            last = new KafkaException(Errors.BROKER_NOT_AVAILABLE, "no bootstrap server configured");
        }
        throw last;
    }

    void refreshIfStale(List<String> topics) {
        if (System.currentTimeMillis() - refreshedAt > MAX_AGE_MS || snapshot.nodes().isEmpty()) {
            refresh(topics);
        }
    }

    Rpc send(Node node, ApiKeys apiKey, byte[] body, int timeoutMs) {
        int correlationId = correlation.incrementAndGet();
        RequestHeader header = new RequestHeader(apiKey, correlationId, config.clientId());
        byte[] response = client.send(node, Protocol.request(header, body), timeoutMs);
        ByteReader reader = new ByteReader(response);
        ResponseHeader decoded = ResponseHeader.read(reader);
        if (decoded.correlationId() != correlationId) {
            throw new KafkaException(Errors.NETWORK_EXCEPTION, "correlation id mismatch from " + node);
        }
        return new Rpc(decoded, reader);
    }

    @Override
    public void close() {
        client.close();
    }

    private Node preferKnown(Node bootstrap) {
        for (Node node : snapshot.nodes()) {
            if (node.host().equals(bootstrap.host()) && node.port() == bootstrap.port()) {
                return node;
            }
        }
        return bootstrap;
    }

    record Rpc(ResponseHeader header, ByteReader body) {
    }
}
