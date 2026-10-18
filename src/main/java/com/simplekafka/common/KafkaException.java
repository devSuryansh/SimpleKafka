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
 * A failure that has a protocol {@link Errors} code.
 *
 * <p>Request handlers turn this into a response. Callers outside the broker
 * can read {@link #error()} to decide whether to retry.
 */
public class KafkaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final Errors error;

    public KafkaException(Errors error) {
        super(error.message() == null ? error.name() : error.message());
        this.error = error;
    }

    public KafkaException(Errors error, String message) {
        super(message);
        this.error = error;
    }

    public KafkaException(Errors error, String message, Throwable cause) {
        super(message, cause);
        this.error = error;
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
        this.error = Errors.UNKNOWN_SERVER_ERROR;
    }

    public Errors error() {
        return error;
    }
}
