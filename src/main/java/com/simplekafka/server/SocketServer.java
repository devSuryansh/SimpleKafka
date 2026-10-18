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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.Limits;
import com.simplekafka.common.Utils;

/**
 * Accepts broker connections and reads one length-prefixed frame at a time.
 *
 * <p>Each connection has its own thread. Produce calls block until replicas catch up,
 * so a fixed pool could deadlock a follower fetch behind those waits. A cached pool
 * lets the fetch proceed on another thread.
 */
final class SocketServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

    private final String host;
    private final AtomicInteger threadIds = new AtomicInteger();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private ServerSocket serverSocket;
    private ExecutorService workers;
    private Thread acceptor;
    private volatile boolean running;
    private volatile KafkaApis apis;
    private int port;

    SocketServer(String host) {
        this.host = host;
    }

    void setApis(KafkaApis apis) {
        this.apis = apis;
    }

    void bind(int port) throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(host, port), 128);
        this.port = serverSocket.getLocalPort();
    }

    int port() {
        return port;
    }

    void start() {
        running = true;
        workers = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "simplekafka-request-" + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        acceptor = new Thread(this::acceptLoop, "simplekafka-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                sockets.add(socket);
                workers.submit(() -> handle(socket));
            } catch (IOException exception) {
                if (running) {
                    LOG.warn("Failed to accept a connection", exception);
                }
            }
        }
    }

    private void handle(Socket socket) {
        try (Socket client = socket) {
            DataInputStream input = new DataInputStream(client.getInputStream());
            DataOutputStream output = new DataOutputStream(client.getOutputStream());
            while (running) {
                int length;
                try {
                    length = input.readInt();
                } catch (EOFException eof) {
                    return;
                }
                if (length <= 0 || length > Limits.MAX_FRAME_BYTES) {
                    LOG.warn("Closing connection after invalid frame length {}", length);
                    return;
                }
                byte[] request = new byte[length];
                input.readFully(request);
                byte[] response = apis.handle(request);
                output.writeInt(response.length);
                output.write(response);
                output.flush();
            }
        } catch (EOFException eof) {
            // The peer closed the connection between frames.
        } catch (IOException exception) {
            if (running) {
                LOG.debug("Connection closed", exception);
            }
        } finally {
            sockets.remove(socket);
        }
    }

    @Override
    public void close() {
        running = false;
        if (serverSocket != null) {
            Utils.closeQuietly(serverSocket);
        }
        for (Socket socket : sockets) {
            Utils.closeQuietly(socket);
        }
        if (workers != null) {
            workers.shutdownNow();
            try {
                workers.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (acceptor != null) {
            try {
                acceptor.join(1_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
