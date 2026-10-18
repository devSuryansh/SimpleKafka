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

package com.simplekafka.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.clients.Admin;
import com.simplekafka.clients.ClientConfig;
import com.simplekafka.clients.Consumer;
import com.simplekafka.clients.ConsumerRecord;
import com.simplekafka.clients.Producer;
import com.simplekafka.clients.SendResult;
import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Node;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;
import com.simplekafka.server.BrokerConfig;
import com.simplekafka.server.BrokerServer;
import com.simplekafka.zookeeper.EmbeddedZooKeeper;

/**
 * Command line entry point for a local ZooKeeper, a broker, and the clients.
 */
public final class SimpleKafka {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKafka.class);

    private SimpleKafka() {
    }

    public static void main(String[] args) {
        int status;
        try {
            status = run(args);
        } catch (IllegalArgumentException exception) {
            System.err.println("error: " + exception.getMessage());
            System.err.println();
            System.err.print(usage());
            status = 2;
        } catch (Exception exception) {
            LOG.error("Command failed", exception);
            System.err.println("error: " + exception.getMessage());
            status = 1;
        }
        System.exit(status);
    }

    static int run(String[] args) throws Exception {
        CommandLine line = CommandLine.parse(args);
        return switch (line.command()) {
            case "zk" -> zk(line);
            case "broker" -> broker(line);
            case "topic" -> topic(line);
            case "produce" -> produce(line);
            case "consume" -> consume(line);
            case "help", "" -> {
                System.out.print(usage());
                yield 0;
            }
            default -> throw new IllegalArgumentException("unknown command " + line.command());
        };
    }

    private static int zk(CommandLine line) throws Exception {
        int port = line.optionInt("port", 2181);
        int tick = line.optionInt("tick-time", 2000);
        Path dataDir = Path.of(line.option("data-dir", "data/zookeeper"));
        EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper(dataDir, port, tick);
        LOG.info("ZooKeeper listening on {}", zooKeeper.connectString());
        Runtime.getRuntime().addShutdownHook(new Thread(zooKeeper::close, "simplekafka-zk-shutdown"));
        Thread.currentThread().join();
        return 0;
    }

    private static int broker(CommandLine line) throws Exception {
        Properties properties = new Properties();
        if (line.has("config")) {
            try (var input = Files.newInputStream(Path.of(line.required("config")))) {
                properties.load(input);
            }
        }
        override(properties, line, "id", "broker.id");
        override(properties, line, "host", "host.name");
        override(properties, line, "port", "port");
        override(properties, line, "zk", "zookeeper.connect");
        override(properties, line, "log-dir", "log.dirs");
        if (properties.getProperty("log.dirs") == null && properties.getProperty("broker.id") != null) {
            properties.setProperty("log.dirs", "data/broker-" + properties.getProperty("broker.id"));
        }
        BrokerConfig config = BrokerConfig.fromProperties(properties);
        BrokerServer server = new BrokerServer(config);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "simplekafka-shutdown"));
        server.awaitShutdown();
        return 0;
    }

    private static int topic(CommandLine line) throws Exception {
        String bootstrap = line.required("bootstrap");
        try (Admin admin = new Admin(bootstrap)) {
            if ("create".equals(line.action())) {
                admin.createTopic(line.required("topic"), line.optionInt("partitions", 1),
                        (short) line.optionInt("replication-factor", 1));
                System.out.println("created topic " + line.required("topic"));
                return 0;
            }
            if ("describe".equals(line.action()) || line.action().isEmpty()) {
                ClusterSnapshot cluster = admin.describeCluster();
                printCluster(cluster, line.has("topic") ? line.required("topic") : null);
                return 0;
            }
        }
        throw new IllegalArgumentException("unknown topic action " + line.action());
    }

    private static int produce(CommandLine line) throws Exception {
        String topic = line.required("topic");
        Integer partition = line.has("partition") ? line.optionInt("partition", 0) : null;
        short acks = (short) line.optionInt("acks", 1);
        ClientConfig clientConfig = ClientConfig.bootstrap(line.required("bootstrap"))
                .withAcks(acks)
                .withClientId("simplekafka-produce");
        try (Producer producer = new Producer(clientConfig)) {
            if (line.has("message")) {
                int count = line.optionInt("count", 1);
                for (int i = 0; i < count; i++) {
                    String payload = count == 1 ? line.required("message") : line.required("message") + "-" + i;
                    SendResult result = producer.send(topic, partition, null, payload.getBytes(StandardCharsets.UTF_8));
                    System.out.printf("topic=%s partition=%d offset=%d%n", result.topic(), result.partition(),
                            result.offset());
                }
                return 0;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String text;
                while ((text = reader.readLine()) != null) {
                    SendResult result = producer.send(topic, partition, null, text.getBytes(StandardCharsets.UTF_8));
                    System.out.printf("topic=%s partition=%d offset=%d%n", result.topic(), result.partition(),
                            result.offset());
                }
            }
        }
        return 0;
    }

    private static int consume(CommandLine line) throws Exception {
        String topic = line.required("topic");
        int partition = line.optionInt("partition", 0);
        int maxMessages = line.optionInt("max-messages", -1);
        try (Consumer consumer = new Consumer(line.required("bootstrap"))) {
            consumer.assign(topic, partition);
            if (line.has("offset")) {
                consumer.seek(line.optionInt("offset", 0));
            }
            int seen = 0;
            while (maxMessages < 0 || seen < maxMessages) {
                List<ConsumerRecord> records = consumer.poll(1000);
                for (ConsumerRecord record : records) {
                    String key = record.keyAsString() == null ? "" : record.keyAsString();
                    System.out.printf("%d\t%s\t%s%n", record.offset(), key, record.valueAsString());
                    seen++;
                    if (maxMessages >= 0 && seen >= maxMessages) {
                        return 0;
                    }
                }
            }
        }
        return 0;
    }

    private static void printCluster(ClusterSnapshot cluster, String onlyTopic) {
        System.out.println("controller: " + cluster.controllerId());
        List<Node> nodes = new ArrayList<>(cluster.nodes());
        nodes.sort(Comparator.comparingInt(Node::id));
        for (Node node : nodes) {
            System.out.println("broker " + node.id() + " " + node.host() + ":" + node.port());
        }
        List<TopicInfo> topics = new ArrayList<>(cluster.topics());
        topics.sort(Comparator.comparing(TopicInfo::name));
        for (TopicInfo topic : topics) {
            if (onlyTopic != null && !onlyTopic.equals(topic.name())) {
                continue;
            }
            System.out.println("topic " + topic.name());
            List<PartitionInfo> partitions = new ArrayList<>(topic.partitions());
            partitions.sort(Comparator.comparingInt(PartitionInfo::partition));
            for (PartitionInfo partition : partitions) {
                System.out.println("  partition " + partition.partition()
                        + " leader=" + partition.leader()
                        + " replicas=" + partition.replicas()
                        + " isr=" + partition.isr());
            }
        }
    }

    private static void override(Properties properties, CommandLine line, String flag, String key) {
        if (line.has(flag)) {
            properties.setProperty(key, line.required(flag));
        }
    }

    private static String usage() {
        return """
                usage: simplekafka <command> [options]

                zk
                    --port <port>          client port (default 2181)
                    --data-dir <dir>       data directory (default data/zookeeper)
                    --tick-time <ms>       tick time (default 2000)

                broker
                    --config <file>        broker properties file
                    --id <id>              broker id
                    --host <host>          advertised host (default 127.0.0.1)
                    --port <port>          listen port (default 9092)
                    --zk <host:port>       ZooKeeper connect string
                    --log-dir <dir>        log directory

                topic create
                    --bootstrap <host:port>
                    --topic <name>
                    --partitions <n>               (default 1)
                    --replication-factor <n>       (default 1)

                topic describe
                    --bootstrap <host:port>
                    --topic <name>                 optional

                produce
                    --bootstrap <host:port>
                    --topic <name>
                    --partition <n>                optional, otherwise partitioned
                    --acks <1|-1>                  (default 1)
                    --message <text> --count <n>   send generated records
                    without --message, each stdin line is one record

                consume
                    --bootstrap <host:port>
                    --topic <name>
                    --partition <n>                (default 0)
                    --offset <n>                   starting offset (default 0)
                    --max-messages <n>             stop after n records
                """;
    }
}
