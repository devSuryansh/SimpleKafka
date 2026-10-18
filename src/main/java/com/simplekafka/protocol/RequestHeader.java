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

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.ByteReader;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;

/**
 * Common prefix of every request.
 *
 * <pre>
 * int16 apiKey
 * int16 apiVersion
 * int32 correlationId
 * string clientId
 * </pre>
 */
public final class RequestHeader {
    public static final short CURRENT_VERSION = 0;

    private final ApiKeys apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;

    public RequestHeader(ApiKeys apiKey, int correlationId, String clientId) {
        this(apiKey, CURRENT_VERSION, correlationId, clientId);
    }

    public RequestHeader(ApiKeys apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId == null ? "" : clientId;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public int correlationId() {
        return correlationId;
    }

    public String clientId() {
        return clientId;
    }

    public void write(ByteBuf buf) {
        buf.writeShort(apiKey.id());
        buf.writeShort(apiVersion);
        buf.writeInt(correlationId);
        buf.writeString(clientId);
    }

    public static RequestHeader read(ByteReader reader) {
        short apiId = reader.readShort();
        short version = reader.readShort();
        int correlationId = reader.readInt();
        String clientId = reader.readString();
        ApiKeys apiKey = ApiKeys.fromId(apiId);
        if (apiKey == null) {
            throw new KafkaException(Errors.INVALID_REQUEST, "unknown api key " + apiId);
        }
        if (version != CURRENT_VERSION) {
            throw new KafkaException(Errors.INVALID_REQUEST, "unsupported api version " + version);
        }
        return new RequestHeader(apiKey, version, correlationId, clientId);
    }
}
