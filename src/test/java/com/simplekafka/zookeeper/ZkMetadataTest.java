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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.simplekafka.common.Node;

class ZkMetadataTest {
    @Test
    void brokerAndControllerRoundTrip() {
        Node broker = ZkMetadata.parseBroker(2, ZkMetadata.broker("127.0.0.1", 9094));
        assertEquals(new Node(2, "127.0.0.1", 9094), broker);
        assertEquals(7, ZkMetadata.parseController(ZkMetadata.controller(7)));
    }

    @Test
    void topicAndStateRoundTrip() {
        Map<Integer, List<Integer>> replicas = Map.of(0, List.of(1, 0));
        ZkMetadata.TopicAssignment topic = ZkMetadata.parseTopic("orders",
                ZkMetadata.topic(1, (short) 2, replicas));
        assertEquals(1, topic.partitions());
        assertEquals((short) 2, topic.replicationFactor());
        assertEquals(List.of(1, 0), topic.replicas().get(0));

        ZkMetadata.PartitionState state = ZkMetadata.parseState(ZkMetadata.state(1, List.of(1, 0)));
        assertEquals(1, state.leader());
        assertEquals(List.of(0, 1), state.isr());
    }
}
