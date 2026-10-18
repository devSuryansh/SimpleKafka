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

package com.simplekafka.network;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.Limits;
import com.simplekafka.common.Node;
import com.simplekafka.common.Utils;

/**
 * Blocking, length-prefixed connections to brokers.
 *
 * <p>One connection is kept per broker. A failed request is retried once on a fresh
 * socket so a broker restart does not stick the caller to a dead connection.
 * Calls for the same broker are serialized. Different brokers proceed in parallel.
 */
public final class FramedClient implements Closeable {
    private final Map<String, Object> locks = new HashMap<>();
    private final Map<String, Connection> connections = new HashMap<>();
    private boolean closed;

    public byte[] send(Node node, byte[] payload, int timeoutMs) {
        if (payload.length > Limits.MAX_FRAME_BYTES) {
            throw new KafkaException(Errors.MESSAGE_TOO_LARGE, "request is larger than the maximum frame");
        }
        Object lock = lockFor(node);
        synchronized (lock) {
            if (closed) {
                throw new KafkaException(Errors.NETWORK_EXCEPTION, "client is closed");
            }
            try {
                return connection(node, timeoutMs).send(payload, timeoutMs);
            } catch (IOException exception) {
                // The broker may already have applied the request, so this is not resent.
                drop(node);
                throw new KafkaException(Errors.NETWORK_EXCEPTION,
                        "request to " + node + " failed", exception);
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        for (Connection connection : connections.values()) {
            Utils.closeQuietly(connection);
        }
        connections.clear();
    }

    private Connection connection(Node node, int timeoutMs) throws IOException {
        String key = key(node);
        Connection current = connections.get(key);
        if (current == null || !current.matches(node)) {
            Utils.closeQuietly(current);
            current = Connection.open(node, timeoutMs);
            connections.put(key, current);
        }
        return current;
    }

    private void drop(Node node) {
        Utils.closeQuietly(connections.remove(key(node)));
    }

    private Object lockFor(Node node) {
        synchronized (locks) {
            return locks.computeIfAbsent(key(node), ignored -> new Object());
        }
    }

    private static String key(Node node) {
        return node.id() >= 0 ? "id:" + node.id() : node.host() + ":" + node.port();
    }

    private static final class Connection implements Closeable {
        private final Node node;
        private final Socket socket;
        private final DataInputStream input;
        private final DataOutputStream output;

        private Connection(Node node, Socket socket, DataInputStream input, DataOutputStream output) {
            this.node = node;
            this.socket = socket;
            this.input = input;
            this.output = output;
        }

        static Connection open(Node node, int timeoutMs) throws IOException {
            Socket socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(node.host(), node.port()), Math.max(1, timeoutMs));
            socket.setSoTimeout(Math.max(1, timeoutMs));
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            return new Connection(node, socket, input, output);
        }

        boolean matches(Node other) {
            return node.host().equals(other.host()) && node.port() == other.port();
        }

        byte[] send(byte[] payload, int timeoutMs) throws IOException {
            socket.setSoTimeout(Math.max(1, timeoutMs));
            output.writeInt(payload.length);
            output.write(payload);
            output.flush();
            int length;
            try {
                length = input.readInt();
            } catch (EOFException eof) {
                throw new IOException("broker closed the connection", eof);
            }
            if (length <= 0 || length > Limits.MAX_FRAME_BYTES) {
                throw new IOException("invalid response frame length " + length);
            }
            byte[] body = new byte[length];
            input.readFully(body);
            return body;
        }

        @Override
        public void close() {
            Utils.closeQuietly(socket);
        }
    }
}
