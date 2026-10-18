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

package com.simplekafka.storage;

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.LogRecord;

/**
 * On-disk record layout.
 *
 * <pre>
 * int32 length          bytes after this field
 * int8  magic           {@link #MAGIC}
 * int32 crc             CRC32 of the body
 * int64 offset
 * int64 timestamp
 * int32 keyLength       -1 when the key is null
 * bytes key
 * int32 valueLength     -1 when the value is null
 * bytes value
 * </pre>
 */
final class RecordFormat {
    static final byte MAGIC = 1;

    /** length + magic + crc + offset + timestamp + keyLength + valueLength. */
    static final int OVERHEAD = 4 + 1 + 4 + 8 + 8 + 4 + 4;

    private RecordFormat() {
    }

    static int encodedSize(LogRecord record) {
        return OVERHEAD + byteLength(record.key()) + byteLength(record.value());
    }

    static byte[] crcBody(LogRecord record) {
        ByteBuf buf = new ByteBuf(encodedSize(record));
        buf.writeLong(record.offset());
        buf.writeLong(record.timestamp());
        buf.writeNullableBytes(record.key());
        buf.writeNullableBytes(record.value());
        return buf.toByteArray();
    }

    private static int byteLength(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }
}
