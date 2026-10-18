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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.clients.Admin;
import com.simplekafka.clients.ClientConfig;
import com.simplekafka.clients.Consumer;
import com.simplekafka.clients.ConsumerRecord;
import com.simplekafka.clients.Producer;
import com.simplekafka.clients.SendResult;
import com.simplekafka.zookeeper.EmbeddedZooKeeper;

class ClusterTest {
    @TempDir
    Path directory;

    @Test
    @Timeout(60)
    void produceAndConsumeOnOneBroker() throws Exception {
        try (EmbeddedZooKeeper zk = new EmbeddedZooKeeper(directory.resolve("zk"), 0, 2000)) {
            BrokerServer broker = start(zk, 0);
            try {
                waitFor(() -> broker.liveBrokerCount() == 1 && broker.isController());
                String bootstrap = "127.0.0.1:" + broker.port();
                try (Admin admin = new Admin(bootstrap)) {
                    admin.createTopic("orders", 1, (short) 1);
                }
                try (Producer producer = new Producer(bootstrap)) {
                    SendResult first = producer.send("orders", "alpha");
                    SendResult second = producer.send("orders", "beta");
                    assertEquals(0L, first.offset());
                    assertEquals(1L, second.offset());
                }
                try (Consumer consumer = new Consumer(bootstrap)) {
                    consumer.assign("orders", 0);
                    List<String> values = collect(consumer, 2);
                    assertEquals(List.of("alpha", "beta"), values);
                }
            } finally {
                broker.close();
            }
        }
    }

    @Test
    @Timeout(90)
    void replicationSurvivesLeaderFailure() throws Exception {
        try (EmbeddedZooKeeper zk = new EmbeddedZooKeeper(directory.resolve("zk"), 0, 2000)) {
            BrokerServer first = start(zk, 0);
            BrokerServer second = start(zk, 1);
            try {
                waitFor(() -> first.liveBrokerCount() == 2 && second.liveBrokerCount() == 2);
                String bootstrap = "127.0.0.1:" + first.port();
                try (Admin admin = new Admin(bootstrap)) {
                    admin.createTopic("events", 1, (short) 2);
                }
                waitFor(() -> first.leader("events", 0) >= 0 && second.leader("events", 0) >= 0);
                try (Producer producer = new Producer(ClientConfig.bootstrap(bootstrap).withAcks((short) -1))) {
                    producer.send("events", "one");
                    producer.send("events", "two");
                }
                int leader = first.leader("events", 0);
                BrokerServer failed = leader == first.brokerId() ? first : second;
                BrokerServer survivor = failed == first ? second : first;
                failed.close();
                waitFor(() -> survivor.leader("events", 0) == survivor.brokerId() && survivor.isController());
                String survivorBootstrap = "127.0.0.1:" + survivor.port();
                try (Producer producer = new Producer(ClientConfig.bootstrap(survivorBootstrap).withAcks((short) -1))) {
                    producer.send("events", "three");
                }
                try (Consumer consumer = new Consumer(survivorBootstrap)) {
                    consumer.assign("events", 0);
                    List<String> values = collect(consumer, 3);
                    assertEquals(List.of("one", "two", "three"), values);
                }
            } finally {
                first.close();
                second.close();
            }
        }
    }

    @Test
    @Timeout(60)
    void keyedRecordsStayOnTheSamePartition() throws Exception {
        try (EmbeddedZooKeeper zk = new EmbeddedZooKeeper(directory.resolve("zk"), 0, 2000)) {
            BrokerServer broker = start(zk, 0);
            try {
                waitFor(() -> broker.isController());
                String bootstrap = "127.0.0.1:" + broker.port();
                try (Admin admin = new Admin(bootstrap)) {
                    admin.createTopic("users", 4, (short) 1);
                }
                try (Producer producer = new Producer(bootstrap)) {
                    SendResult first = producer.send("users", "alice", "a1");
                    SendResult second = producer.send("users", "alice", "a2");
                    assertEquals(first.partition(), second.partition());
                    assertTrue(first.partition() >= 0 && first.partition() < 4);
                }
            } finally {
                broker.close();
            }
        }
    }

    private BrokerServer start(EmbeddedZooKeeper zk, int id) {
        BrokerConfig config = BrokerConfig.builder()
                .brokerId(id)
                .host("127.0.0.1")
                .port(0)
                .zkConnect(zk.connectString())
                .logDir(directory.resolve("broker-" + id))
                .zkSessionTimeoutMs(8_000)
                .replicaLagTimeMaxMs(8_000)
                .replicaFetchMaxWaitMs(200)
                .build();
        BrokerServer server = new BrokerServer(config);
        server.start();
        return server;
    }

    private static List<String> collect(Consumer consumer, int count) {
        List<String> values = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (values.size() < count && System.nanoTime() < deadline) {
            for (ConsumerRecord record : consumer.poll(500)) {
                values.add(record.valueAsString());
            }
        }
        assertEquals(count, values.size(), "records: " + values);
        return values;
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met");
    }
}
