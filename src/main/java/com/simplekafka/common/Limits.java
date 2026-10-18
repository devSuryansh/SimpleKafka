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

/**
 * Hard bounds that protect the broker from a corrupt or hostile frame.
 */
public final class Limits {
    /** Largest request or response body accepted on a connection. */
    public static final int MAX_FRAME_BYTES = 8 * 1024 * 1024;

    /** Largest single record, including its key and value. */
    public static final int MAX_MESSAGE_BYTES = 1024 * 1024;

    /** Largest number of records accepted in one produce or fetch. */
    public static final int MAX_RECORD_BATCH = 10_000;

    private Limits() {
    }
}
