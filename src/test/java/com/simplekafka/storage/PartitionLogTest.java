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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.LogRecord;

class PartitionLogTest {
    @TempDir
    Path directory;

    @Test
    void appendReadAndRecover() throws Exception {
        LogConfig config = new LogConfig(180, 64);
        List<LogRecord> written;
        try (PartitionLog log = PartitionLog.open(directory, config)) {
            written = messages(30);
            assertEquals(0L, log.appendAsLeader(written));
            assertTrue(log.segmentCount() > 1);
            log.updateHighWatermark(log.logEndOffset());
            assertEquals(values(written), values(log.read(0L, 1024 * 1024, log.highWatermark())));
        }
        try (PartitionLog reopened = PartitionLog.open(directory, config)) {
            assertEquals(written.size(), reopened.logEndOffset());
            assertEquals(written.size(), reopened.highWatermark());
            assertEquals(values(written), values(reopened.read(0L, 1024 * 1024, reopened.highWatermark())));
        }
    }

    @Test
    void sparseIndexStillReturnsTheLastRecord() {
        LogConfig config = new LogConfig(1024 * 1024, 10_000);
        try (PartitionLog log = PartitionLog.open(directory, config)) {
            log.appendAsLeader(messages(15));
            log.updateHighWatermark(15);
            List<LogRecord> tail = log.read(14L, 1024, log.highWatermark());
            assertEquals(1, tail.size());
            assertEquals("m-14", text(tail.get(0)));
        }
    }

    @Test
    void readStopsAtTheHighWatermark() {
        try (PartitionLog log = PartitionLog.open(directory, LogConfig.defaults())) {
            log.appendAsLeader(messages(5));
            log.updateHighWatermark(3);
            assertEquals(List.of("m-0", "m-1", "m-2"), values(log.read(0L, 1024 * 1024, log.highWatermark())));
            assertEquals(5L, log.logEndOffset());
        }
    }

    @Test
    void tornTailIsTruncatedOnRecovery() throws Exception {
        LogConfig config = LogConfig.defaults();
        try (PartitionLog log = PartitionLog.open(directory, config)) {
            log.appendAsLeader(messages(2));
            log.updateHighWatermark(2);
        }
        Path segment = directory.resolve(LogSegment.logName(0));
        Files.write(segment, new byte[] {0, 0, 0, 20, 1, 2, 3, 4}, java.nio.file.StandardOpenOption.APPEND);
        try (PartitionLog reopened = PartitionLog.open(directory, config)) {
            assertEquals(2L, reopened.logEndOffset());
            assertEquals(List.of("m-0", "m-1"), values(reopened.read(0L, 1024 * 1024, 2L)));
        }
    }

    @Test
    void followerAppendPreservesOffsetsAndSkipsDuplicates() {
        try (PartitionLog log = PartitionLog.open(directory, LogConfig.defaults())) {
            log.appendAsFollower(List.of(record(0, "a"), record(1, "b")));
            log.appendAsFollower(List.of(record(1, "b"), record(2, "c")));
            assertEquals(3L, log.logEndOffset());
            assertEquals(List.of("a", "b", "c"), values(log.read(0L, 1024 * 1024, 3L)));
            assertEquals(Errors.OFFSET_OUT_OF_RANGE,
                    assertThrows(KafkaException.class, () -> log.appendAsFollower(List.of(record(4, "gap")))).error());
        }
    }

    @Test
    void truncateDropsTheUncommittedTail() {
        try (PartitionLog log = PartitionLog.open(directory, LogConfig.defaults())) {
            log.appendAsLeader(messages(8));
            log.updateHighWatermark(5);
            log.truncateTo(log.highWatermark());
            assertEquals(5L, log.logEndOffset());
            assertEquals(5, log.read(0L, 1024 * 1024, log.logEndOffset()).size());
        }
    }

    private static List<LogRecord> messages(int count) {
        List<LogRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new LogRecord(-1L, 1_000L + i, null, ("m-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return records;
    }

    private static LogRecord record(long offset, String value) {
        return new LogRecord(offset, 10L, null, value.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> values(List<LogRecord> records) {
        List<String> values = new ArrayList<>();
        for (LogRecord record : records) {
            values.add(text(record));
        }
        return values;
    }

    private static String text(LogRecord record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }
}
