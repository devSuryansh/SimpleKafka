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

package com.simplekafka.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable description of a topic and its partitions.
 */
public final class TopicInfo {
    private final String name;
    private final short error;
    private final Map<Integer, PartitionInfo> partitions;

    public TopicInfo(String name, short error, Map<Integer, PartitionInfo> partitions) {
        this.name = name;
        this.error = error;
        this.partitions = Map.copyOf(partitions);
    }

    public static TopicInfo missing(String name) {
        return new TopicInfo(name, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Map.of());
    }

    public static TopicInfo of(String name, Collection<PartitionInfo> partitions) {
        Map<Integer, PartitionInfo> byId = new HashMap<>();
        for (PartitionInfo info : partitions) {
            byId.put(info.partition(), info);
        }
        return new TopicInfo(name, Errors.NONE.code(), byId);
    }

    public String name() {
        return name;
    }

    public short error() {
        return error;
    }

    public int partitionCount() {
        return partitions.size();
    }

    public PartitionInfo partition(int partition) {
        return partitions.get(partition);
    }

    public Collection<PartitionInfo> partitions() {
        return partitions.values();
    }
}
