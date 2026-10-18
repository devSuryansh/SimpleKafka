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

/**
 * The offset of the first record in a successful produce, or {@code -1} on error.
 */
public final class ProduceResponse {
    private final long baseOffset;

    public ProduceResponse(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public byte[] body() {
        ByteBuf buf = new ByteBuf(8);
        write(buf);
        return buf.toByteArray();
    }

    public void write(ByteBuf buf) {
        buf.writeLong(baseOffset);
    }

    public static ProduceResponse read(ByteReader reader) {
        return new ProduceResponse(reader.readLong());
    }
}
