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
import java.util.concurrent.TimeUnit;

import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.Node;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;
import com.simplekafka.common.Utils;
import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.CreateTopicRequest;
import com.simplekafka.protocol.CreateTopicResponse;

/**
 * Topic administration. Create requests are sent to the controller, then this client
 * waits until metadata shows a leader for every partition.
 */
public final class Admin implements Closeable {
    private final ClientConfig config;
    private final Metadata metadata;

    public Admin(String bootstrapServers) {
        this(ClientConfig.bootstrap(bootstrapServers).withClientId("simplekafka-admin"));
    }

    public Admin(ClientConfig config) {
        this.config = config;
        this.metadata = new Metadata(config);
    }

    public void createTopic(String topic, int partitions, short replicationFactor) {
        Utils.requireTopicName(topic);
        KafkaException last = new KafkaException(Errors.REQUEST_TIMED_OUT, "timed out creating " + topic);
        for (int attempt = 0; attempt < 10; attempt++) {
            metadata.refresh(List.of());
            Node controller = metadata.snapshot().controller();
            if (controller == null) {
                sleep(config.retryBackoffMs());
                continue;
            }
            CreateTopicRequest request = new CreateTopicRequest(topic, partitions, replicationFactor);
            Metadata.Rpc rpc = metadata.send(controller, ApiKeys.CREATE_TOPIC, request.body(),
                    config.requestTimeoutMs());
            if (rpc.header().error() == Errors.NONE) {
                CreateTopicResponse.read(rpc.body());
                awaitLeaders(topic, partitions);
                return;
            }
            if (rpc.header().error() == Errors.NOT_CONTROLLER || rpc.header().error().retriable()) {
                last = new KafkaException(rpc.header().error());
                sleep(config.retryBackoffMs());
                continue;
            }
            throw new KafkaException(rpc.header().error());
        }
        throw last;
    }

    public ClusterSnapshot describeCluster() {
        metadata.refresh(List.of());
        return metadata.snapshot();
    }

    @Override
    public void close() {
        metadata.close();
    }

    private void awaitLeaders(String topic, int partitions) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            metadata.refresh(List.of(topic));
            TopicInfo info = metadata.snapshot().topic(topic);
            if (info != null && info.error() == Errors.NONE.code() && info.partitionCount() == partitions) {
                boolean ready = true;
                for (PartitionInfo partition : info.partitions()) {
                    if (partition.leader() < 0) {
                        ready = false;
                        break;
                    }
                }
                if (ready) {
                    return;
                }
            }
            sleep(100);
        }
        throw new KafkaException(Errors.LEADER_NOT_AVAILABLE, "timed out waiting for leaders of " + topic);
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaException(Errors.REQUEST_TIMED_OUT, "interrupted");
        }
    }
}
