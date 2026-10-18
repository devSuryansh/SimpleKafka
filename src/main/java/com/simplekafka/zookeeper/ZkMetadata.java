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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.Node;

/**
 * Text form of the znodes SimpleKafka stores in ZooKeeper.
 *
 * <p>The format is line-oriented {@code key=value} so a running cluster can be
 * inspected with the ZooKeeper shell. Lists are comma-separated.
 */
public final class ZkMetadata {
    private ZkMetadata() {
    }

    public static byte[] broker(String host, int port) {
        return text("host=" + host + "\nport=" + port + "\n");
    }

    public static Node parseBroker(int id, byte[] data) {
        Map<String, String> values = parse(data);
        return new Node(id, required(values, "host"), Integer.parseInt(required(values, "port")));
    }

    public static byte[] controller(int brokerId) {
        return text("id=" + brokerId + "\n");
    }

    public static int parseController(byte[] data) {
        return Integer.parseInt(required(parse(data), "id"));
    }

    public static byte[] topic(int partitions, short replicationFactor, Map<Integer, List<Integer>> replicas) {
        StringBuilder builder = new StringBuilder();
        builder.append("partitions=").append(partitions).append('\n');
        builder.append("replication-factor=").append(replicationFactor).append('\n');
        for (Map.Entry<Integer, List<Integer>> entry : replicas.entrySet()) {
            builder.append("replica.").append(entry.getKey()).append('=');
            builder.append(join(entry.getValue())).append('\n');
        }
        return text(builder.toString());
    }

    public static TopicAssignment parseTopic(String name, byte[] data) {
        Map<String, String> values = parse(data);
        int partitions = Integer.parseInt(required(values, "partitions"));
        short replicationFactor = Short.parseShort(required(values, "replication-factor"));
        Map<Integer, List<Integer>> replicas = new LinkedHashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            replicas.put(partition, integers(required(values, "replica." + partition)));
        }
        return new TopicAssignment(name, partitions, replicationFactor, replicas);
    }

    public static byte[] state(int leader, List<Integer> isr) {
        return text("leader=" + leader + "\nisr=" + join(isr) + "\n");
    }

    public static PartitionState parseState(byte[] data) {
        Map<String, String> values = parse(data);
        return new PartitionState(Integer.parseInt(required(values, "leader")), integers(required(values, "isr")));
    }

    public static Map<String, String> parse(byte[] data) {
        if (data == null) {
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "missing znode data");
        }
        Map<String, String> values = new LinkedHashMap<>();
        String text = new String(data, StandardCharsets.UTF_8);
        for (String rawLine : text.split("\n")) {
            String line = rawLine.trim();
            if (line.isEmpty()) {
                continue;
            }
            int separator = line.indexOf('=');
            if (separator <= 0) {
                throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "bad metadata line: " + line);
            }
            values.put(line.substring(0, separator).trim(), line.substring(separator + 1).trim());
        }
        return values;
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "metadata is missing " + key);
        }
        return value;
    }

    private static List<Integer> integers(String value) {
        if (value.isEmpty()) {
            return List.of();
        }
        List<Integer> numbers = new ArrayList<>();
        for (String part : value.split(",")) {
            if (!part.isBlank()) {
                numbers.add(Integer.parseInt(part.trim()));
            }
        }
        return numbers;
    }

    private static String join(List<Integer> values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(values.get(i));
        }
        return builder.toString();
    }

    private static byte[] text(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Replica placement for a topic, as stored on the topic znode.
     */
    public record TopicAssignment(String name, int partitions, short replicationFactor,
                                  Map<Integer, List<Integer>> replicas) {
    }

    /**
     * Leader and in-sync set for one partition.
     */
    public record PartitionState(int leader, List<Integer> isr) {
    }
}
