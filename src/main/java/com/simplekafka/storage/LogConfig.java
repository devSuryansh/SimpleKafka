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

/**
 * On-disk layout settings for a partition log.
 *
 * @param segmentBytes maximum size of an active segment before a new one is rolled
 * @param indexIntervalBytes minimum distance between sparse index entries
 */
public record LogConfig(int segmentBytes, int indexIntervalBytes) {
    public static final int DEFAULT_SEGMENT_BYTES = 1024 * 1024;
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;

    public LogConfig {
        if (segmentBytes < 64) {
            throw new IllegalArgumentException("segmentBytes must be at least 64");
        }
        if (indexIntervalBytes < 1) {
            throw new IllegalArgumentException("indexIntervalBytes must be positive");
        }
    }

    public static LogConfig defaults() {
        return new LogConfig(DEFAULT_SEGMENT_BYTES, DEFAULT_INDEX_INTERVAL_BYTES);
    }
}
