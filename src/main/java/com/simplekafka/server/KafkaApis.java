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

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simplekafka.common.ByteReader;
import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.Limits;
import com.simplekafka.common.Node;
import com.simplekafka.common.TopicInfo;
import com.simplekafka.common.Utils;
import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.CreateTopicRequest;
import com.simplekafka.protocol.CreateTopicResponse;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.MetadataRequest;
import com.simplekafka.protocol.MetadataResponse;
import com.simplekafka.protocol.ProduceRequest;
import com.simplekafka.protocol.ProduceResponse;
import com.simplekafka.protocol.Protocol;
import com.simplekafka.protocol.RequestHeader;
import com.simplekafka.protocol.ResponseHeader;

/**
 * Decodes one framed request and builds the matching response.
 *
 * <p>Every api key has a fixed response body, including failures, so a client that
 * shares a connection can always read the next frame.
 */
final class KafkaApis {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaApis.class);

    private final ReplicaManager replicas;
    private final Controller controller;
    private final MetadataCache cache;

    KafkaApis(ReplicaManager replicas, Controller controller, MetadataCache cache) {
        this.replicas = replicas;
        this.controller = controller;
        this.cache = cache;
    }

    byte[] handle(byte[] frame) {
        int correlationId = 0;
        ApiKeys apiKey = null;
        try {
            ByteReader reader = new ByteReader(frame);
            RequestHeader header = RequestHeader.read(reader);
            correlationId = header.correlationId();
            apiKey = header.apiKey();
            if (apiKey == ApiKeys.PRODUCE) {
                return produce(header, reader);
            }
            if (apiKey == ApiKeys.FETCH) {
                return fetch(header, reader);
            }
            if (apiKey == ApiKeys.METADATA) {
                return metadata(header, reader);
            }
            if (apiKey == ApiKeys.CREATE_TOPIC) {
                return createTopic(header, reader);
            }
            return error(apiKey, correlationId, Errors.INVALID_REQUEST);
        } catch (KafkaException exception) {
            LOG.debug("Request {} failed: {}", apiKey, exception.getMessage());
            return error(apiKey, correlationId, exception.error());
        } catch (Exception exception) {
            LOG.error("Unexpected failure handling {}", apiKey, exception);
            return error(apiKey, correlationId, Errors.UNKNOWN_SERVER_ERROR);
        }
    }

    private byte[] produce(RequestHeader header, ByteReader reader) {
        ProduceRequest request = ProduceRequest.read(reader);
        validateTopicPartition(request.topic(), request.partition());
        if (request.records().isEmpty()) {
            throw new KafkaException(Errors.INVALID_REQUEST, "produce batch is empty");
        }
        long offset = replicas.produce(request.topic(), request.partition(), request.records(),
                request.acks(), request.timeoutMs());
        return ok(header, new ProduceResponse(offset).body());
    }

    private byte[] fetch(RequestHeader header, ByteReader reader) {
        FetchRequest request = FetchRequest.read(reader);
        validateTopicPartition(request.topic(), request.partition());
        if (request.maxBytes() <= 0 || request.maxBytes() > Limits.MAX_FRAME_BYTES) {
            throw new KafkaException(Errors.INVALID_REQUEST, "invalid max bytes");
        }
        if (request.offset() < 0) {
            throw new KafkaException(Errors.OFFSET_OUT_OF_RANGE);
        }
        ReplicaManager.FetchResult result = replicas.fetch(request.topic(), request.partition(), request.offset(),
                request.maxBytes(), Math.max(0, request.maxWaitMs()), request.replicaId());
        FetchResponse body = new FetchResponse(result.highWatermark(), result.logEndOffset(), result.records());
        return respond(header.correlationId(), result.error(), body.body());
    }

    private byte[] metadata(RequestHeader header, ByteReader reader) {
        MetadataRequest request = MetadataRequest.read(reader);
        ClusterSnapshot full = cache.snapshot();
        Map<String, TopicInfo> topics = new LinkedHashMap<>();
        if (request.topics().isEmpty()) {
            for (TopicInfo topic : full.topics()) {
                topics.put(topic.name(), topic);
            }
        } else {
            for (String name : request.topics()) {
                TopicInfo topic = full.topic(name);
                topics.put(name, topic == null ? TopicInfo.missing(name) : topic);
            }
        }
        ClusterSnapshot filtered = new ClusterSnapshot(full.controllerId(), nodes(full), topics);
        return ok(header, new MetadataResponse(filtered).body());
    }

    private byte[] createTopic(RequestHeader header, ByteReader reader) {
        CreateTopicRequest request = CreateTopicRequest.read(reader);
        Utils.requireTopicName(request.topic());
        controller.createTopic(request.topic(), request.partitions(), request.replicationFactor());
        return ok(header, new CreateTopicResponse(request.topic()).body());
    }

    private static void validateTopicPartition(String topic, int partition) {
        Utils.requireTopicName(topic);
        if (partition < 0) {
            throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "negative partition");
        }
    }

    private static Map<Integer, Node> nodes(ClusterSnapshot snapshot) {
        Map<Integer, Node> nodes = new LinkedHashMap<>();
        for (Node node : snapshot.nodes()) {
            nodes.put(node.id(), node);
        }
        return nodes;
    }

    private static byte[] ok(RequestHeader header, byte[] body) {
        return respond(header.correlationId(), Errors.NONE, body);
    }

    private byte[] error(ApiKeys apiKey, int correlationId, Errors error) {
        return respond(correlationId, error, emptyBody(apiKey));
    }

    private static byte[] respond(int correlationId, Errors error, byte[] body) {
        return Protocol.response(new ResponseHeader(correlationId, error), body);
    }

    private static byte[] emptyBody(ApiKeys apiKey) {
        if (apiKey == ApiKeys.PRODUCE) {
            return new ProduceResponse(-1L).body();
        }
        if (apiKey == ApiKeys.FETCH) {
            return FetchResponse.empty().body();
        }
        if (apiKey == ApiKeys.METADATA) {
            return new MetadataResponse(ClusterSnapshot.empty()).body();
        }
        if (apiKey == ApiKeys.CREATE_TOPIC) {
            return new CreateTopicResponse("").body();
        }
        return new byte[0];
    }
}
