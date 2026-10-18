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

import java.util.List;

/**
 * Leadership and replica placement for a single partition.
 *
 * <p>A leader of {@code -1} means the partition is offline.
 */
public final class PartitionInfo {
    private final int partition;
    private final int leader;
    private final List<Integer> replicas;
    private final List<Integer> isr;

    public PartitionInfo(int partition, int leader, List<Integer> replicas, List<Integer> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = List.copyOf(replicas);
        this.isr = List.copyOf(isr);
    }

    public int partition() {
        return partition;
    }

    public int leader() {
        return leader;
    }

    public List<Integer> replicas() {
        return replicas;
    }

    public List<Integer> isr() {
        return isr;
    }
}
