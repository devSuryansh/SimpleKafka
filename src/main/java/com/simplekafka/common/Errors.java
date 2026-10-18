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
 * Error codes carried in every response header.
 *
 * <p>The numeric values match the well-known Kafka error codes for the cases
 * this broker actually produces, so operators can read them without a second
 * table. Codes that SimpleKafka does not use are simply never written.
 */
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error", true),
    NONE(0, null, false),
    OFFSET_OUT_OF_RANGE(1, "The requested offset is outside the log", false),
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host the topic-partition", true),
    LEADER_NOT_AVAILABLE(5, "There is no leader for this partition", true),
    NOT_LEADER_OR_FOLLOWER(6, "This server is not the leader for that topic-partition", true),
    REQUEST_TIMED_OUT(7, "The request timed out", true),
    BROKER_NOT_AVAILABLE(8, "The broker is not available", true),
    MESSAGE_TOO_LARGE(10, "The record is larger than the maximum allowed size", false),
    NETWORK_EXCEPTION(13, "The network connection was disconnected before the response arrived", true),
    INVALID_TOPIC_EXCEPTION(17, "The request attempted to use an invalid topic name", false),
    NOT_ENOUGH_REPLICAS(19, "Messages are rejected because there are fewer in-sync replicas than required", true),
    INVALID_REQUIRED_ACKS(21, "The requested acks value is not supported", false),
    TOPIC_ALREADY_EXISTS(36, "The topic already exists", false),
    INVALID_PARTITIONS(37, "The number of partitions is invalid", false),
    INVALID_REPLICATION_FACTOR(38, "The replication factor is invalid", false),
    NOT_CONTROLLER(41, "This is not the controller", true),
    INVALID_REQUEST(42, "The request is invalid", false);

    private final short code;
    private final String message;
    private final boolean retriable;

    Errors(int code, String message, boolean retriable) {
        this.code = (short) code;
        this.message = message;
        this.retriable = retriable;
    }

    public short code() {
        return code;
    }

    /**
     * Returns the default explanation, or {@code null} for {@link #NONE}.
     */
    public String message() {
        return message;
    }

    /**
     * Whether a client should refresh metadata and try the request again.
     */
    public boolean retriable() {
        return retriable;
    }

    /**
     * Maps a wire code back to a known error. Unrecognized codes become
     * {@link #UNKNOWN_SERVER_ERROR} so a newer broker cannot crash an older client.
     */
    public static Errors forCode(short code) {
        for (Errors error : values()) {
            if (error.code == code) {
                return error;
            }
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
