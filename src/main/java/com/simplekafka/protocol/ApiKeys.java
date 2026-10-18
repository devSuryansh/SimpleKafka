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

/**
 * Request types on the SimpleKafka wire protocol.
 *
 * <p>The protocol is intentionally not compatible with Apache Kafka. It keeps
 * the same ideas: a numeric api key, a version, a correlation id, and a body.
 */
public enum ApiKeys {
    PRODUCE(0),
    FETCH(1),
    METADATA(2),
    CREATE_TOPIC(3);

    private final short id;

    ApiKeys(int id) {
        this.id = (short) id;
    }

    public short id() {
        return id;
    }

    public static ApiKeys fromId(short id) {
        for (ApiKeys key : values()) {
            if (key.id == id) {
                return key;
            }
        }
        return null;
    }
}
