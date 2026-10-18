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

/**
 * ZooKeeper layout for broker membership, controller election, and partition state.
 *
 * <pre>
 * /brokers/ids/{id}                         ephemeral broker registration
 * /controller                               ephemeral controller election
 * /brokers/topics/{topic}                   partition assignment
 * /brokers/topics/{topic}/partitions/{id}/state
 * </pre>
 */
public final class ZkPaths {
    public static final String BROKERS = "/brokers";
    public static final String BROKER_IDS = "/brokers/ids";
    public static final String TOPICS = "/brokers/topics";
    public static final String CONTROLLER = "/controller";

    private ZkPaths() {
    }

    public static String broker(int id) {
        return BROKER_IDS + "/" + id;
    }

    public static String topic(String topic) {
        return TOPICS + "/" + topic;
    }

    public static String partitionState(String topic, int partition) {
        return topic(topic) + "/partitions/" + partition + "/state";
    }
}
