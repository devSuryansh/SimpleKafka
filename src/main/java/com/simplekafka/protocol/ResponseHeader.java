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

/**
 * Common prefix of every response. The body that follows has a fixed shape per api key,
 * including when {@code error} is not {@link Errors#NONE}.
 *
 * <pre>
 * int32 correlationId
 * int16 errorCode
 * </pre>
 */
public final class ResponseHeader {
    private final int correlationId;
    private final Errors error;

    public ResponseHeader(int correlationId, Errors error) {
        this.correlationId = correlationId;
        this.error = error;
    }

    public int correlationId() {
        return correlationId;
    }

    public Errors error() {
        return error;
    }

    public void write(ByteBuf buf) {
        buf.writeInt(correlationId);
        buf.writeShort(error.code());
    }

    public static ResponseHeader read(ByteReader reader) {
        int correlationId = reader.readInt();
        Errors error = Errors.forCode(reader.readShort());
        return new ResponseHeader(correlationId, error);
    }
}
