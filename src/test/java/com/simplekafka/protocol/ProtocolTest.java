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

package com.simplekafka.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.ByteReader;
import com.simplekafka.common.ClusterSnapshot;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.LogRecord;
import com.simplekafka.common.Node;
import com.simplekafka.common.PartitionInfo;
import com.simplekafka.common.TopicInfo;

class ProtocolTest {
    @Test
    void produceRoundTrip() {
        LogRecord record = new LogRecord(-1L, 42L, "k".getBytes(StandardCharsets.UTF_8),
                "hello".getBytes(StandardCharsets.UTF_8));
        ProduceRequest request = new ProduceRequest("orders", 1, (short) -1, 1500, List.of(record));
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, 7, "producer");
        byte[] frame = Protocol.request(header, request.body());

        ByteReader reader = new ByteReader(frame);
        RequestHeader decodedHeader = RequestHeader.read(reader);
        ProduceRequest decoded = ProduceRequest.read(reader);
        assertEquals(ApiKeys.PRODUCE, decodedHeader.apiKey());
        assertEquals(7, decodedHeader.correlationId());
        assertEquals("producer", decodedHeader.clientId());
        assertEquals("orders", decoded.topic());
        assertEquals(1, decoded.partition());
        assertEquals((short) -1, decoded.acks());
        assertEquals(1500, decoded.timeoutMs());
        assertEquals(1, decoded.records().size());
        assertArrayEquals(record.value(), decoded.records().get(0).value());
        assertEquals(0, reader.remaining());
    }

    @Test
    void fetchRoundTripKeepsOffsets() {
        LogRecord record = new LogRecord(4L, 9L, null, new byte[] {1, 2, 3});
        FetchResponse response = new FetchResponse(5L, 8L, List.of(record));
        ResponseHeader header = new ResponseHeader(3, Errors.NONE);
        byte[] frame = Protocol.response(header, response.body());

        ByteReader reader = new ByteReader(frame);
        ResponseHeader decodedHeader = ResponseHeader.read(reader);
        FetchResponse decoded = FetchResponse.read(reader);
        assertEquals(Errors.NONE, decodedHeader.error());
        assertEquals(5L, decoded.highWatermark());
        assertEquals(8L, decoded.logEndOffset());
        assertEquals(record, decoded.records().get(0));
        assertNull(decoded.records().get(0).key());
    }

    @Test
    void metadataRoundTrip() {
        Node node = new Node(1, "127.0.0.1", 9092);
        PartitionInfo partition = new PartitionInfo(0, 1, List.of(1, 2), List.of(1));
        TopicInfo topic = TopicInfo.of("events", List.of(partition));
        ClusterSnapshot snapshot = new ClusterSnapshot(1, Map.of(1, node), Map.of("events", topic));
        byte[] body = new MetadataResponse(snapshot).body();
        ClusterSnapshot decoded = MetadataResponse.read(new ByteReader(body)).cluster();
        assertEquals(1, decoded.controllerId());
        assertEquals(node, decoded.node(1));
        assertEquals(1, decoded.topic("events").partition(0).leader());
        assertEquals(List.of(1, 2), decoded.topic("events").partition(0).replicas());
    }

    @Test
    void unknownApiKeyIsRejected() {
        ByteBuf buf = new ByteBuf();
        buf.writeShort(99);
        buf.writeShort(0);
        buf.writeInt(1);
        buf.writeString("x");
        KafkaException error = assertThrows(KafkaException.class,
                () -> RequestHeader.read(new ByteReader(buf.toByteArray())));
        assertEquals(Errors.INVALID_REQUEST, error.error());
    }

    @Test
    void truncatedFrameIsRejected() {
        KafkaException error = assertThrows(KafkaException.class,
                () -> ProduceResponse.read(new ByteReader(new byte[] {1, 2})));
        assertEquals(Errors.INVALID_REQUEST, error.error());
    }
}
