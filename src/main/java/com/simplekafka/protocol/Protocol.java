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

/**
 * Joins a header and a body into the payload that sits inside a length-prefixed frame.
 *
 * <p>The 4-byte frame length is added by the socket layer, not here, so codecs can be
 * tested without a network.
 */
public final class Protocol {
    private Protocol() {
    }

    public static byte[] request(RequestHeader header, byte[] body) {
        ByteBuf buf = new ByteBuf(32 + body.length);
        header.write(buf);
        buf.writeBytes(body);
        return buf.toByteArray();
    }

    public static byte[] response(ResponseHeader header, byte[] body) {
        ByteBuf buf = new ByteBuf(8 + body.length);
        header.write(buf);
        buf.writeBytes(body);
        return buf.toByteArray();
    }
}
