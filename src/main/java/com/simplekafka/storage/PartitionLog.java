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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.Limits;
import com.simplekafka.common.LogRecord;

/**
 * The append-only log for one partition.
 *
 * <p>Records are stored in segments named by their base offset. The high watermark is
 * the first offset that is not yet committed: consumers read offsets below it, while
 * the log end may sit further ahead when replicas have not caught up. Both values are
 * recovered from disk, and a torn write at the end of the active segment is truncated
 * on open.
 */
public final class PartitionLog implements Closeable {
    private static final Pattern SEGMENT_NAME = Pattern.compile("\\d{20}\\.log");
    private static final String HIGH_WATERMARK_FILE = "high-watermark";

    private final Path directory;
    private final LogConfig config;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantLock awaitLock = new ReentrantLock();
    private final Condition dataAvailable = awaitLock.newCondition();
    private final Object watermarkLock = new Object();
    private final AtomicLong nextOffset = new AtomicLong();
    private final AtomicLong highWatermark = new AtomicLong();
    private List<LogSegment> segments;
    private LogSegment active;
    private boolean closed;

    private PartitionLog(Path directory, LogConfig config, List<LogSegment> segments) {
        this.directory = directory;
        this.config = config;
        this.segments = segments;
        this.active = segments.get(segments.size() - 1);
        this.nextOffset.set(active.endOffset());
    }

    /**
     * Opens an existing partition directory, or creates an empty log when none is there.
     */
    public static PartitionLog open(Path directory, LogConfig config) {
        try {
            Files.createDirectories(directory);
            List<Path> paths = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
                for (Path path : stream) {
                    if (SEGMENT_NAME.matcher(path.getFileName().toString()).matches()) {
                        paths.add(path);
                    }
                }
            }
            paths.sort(Comparator.comparingLong(path -> LogSegment.parseBaseOffset(path.getFileName().toString())));
            List<LogSegment> opened = new ArrayList<>();
            for (Path path : paths) {
                opened.add(LogSegment.open(path, config.indexIntervalBytes()));
            }
            if (opened.isEmpty()) {
                opened.add(LogSegment.create(directory, 0L, config.indexIntervalBytes()));
            }
            PartitionLog log = new PartitionLog(directory, config, opened);
            log.loadHighWatermark();
            return log;
        } catch (IOException exception) {
            throw disk("open partition log " + directory, exception);
        }
    }

    /**
     * Assigns offsets and appends. Returns the offset of the first record.
     * A timestamp of {@code 0} is replaced with the current time.
     */
    public long appendAsLeader(List<LogRecord> records) {
        if (records.isEmpty()) {
            throw new KafkaException(Errors.INVALID_REQUEST, "produce batch is empty");
        }
        lock.writeLock().lock();
        try {
            long now = System.currentTimeMillis();
            List<LogRecord> assigned = new ArrayList<>(records.size());
            long offset = nextOffset.get();
            long base = offset;
            for (LogRecord record : records) {
                long timestamp = record.timestamp() > 0 ? record.timestamp() : now;
                assigned.add(new LogRecord(offset, timestamp, record.key(), record.value()));
                offset++;
            }
            appendAll(assigned);
            return base;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends records that already carry leader-assigned offsets. A prefix below the
     * local log end is skipped so a retried fetch is safe.
     */
    public void appendAsFollower(List<LogRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            List<LogRecord> fresh = new ArrayList<>();
            long expected = nextOffset.get();
            for (LogRecord record : records) {
                if (record.offset() < expected) {
                    continue;
                }
                if (record.offset() != expected) {
                    throw new KafkaException(Errors.OFFSET_OUT_OF_RANGE,
                            "expected offset " + expected + " but received " + record.offset());
                }
                fresh.add(record);
                expected++;
            }
            if (!fresh.isEmpty()) {
                appendAll(fresh);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reads records in {@code [offset, endExclusive)} up to about {@code maxBytes}.
     * The first record is returned even when it is larger than {@code maxBytes}, so a
     * single large record cannot stall a consumer.
     */
    public List<LogRecord> read(long offset, int maxBytes, long endExclusive) {
        lock.readLock().lock();
        try {
            if (maxBytes <= 0 || offset >= endExclusive || offset >= nextOffset.get()) {
                return List.of();
            }
            long end = Math.min(endExclusive, nextOffset.get());
            List<LogRecord> records = new ArrayList<>();
            int used = 0;
            for (LogSegment segment : segments) {
                if (segment.baseOffset() >= end) {
                    break;
                }
                if (segment.endOffset() <= offset) {
                    continue;
                }
                int budget = records.isEmpty() ? Math.max(maxBytes, 1) : maxBytes - used;
                if (budget <= 0) {
                    break;
                }
                for (LogRecord record : segment.read(offset, budget, end)) {
                    int encoded = RecordFormat.encodedSize(record);
                    if (!records.isEmpty() && used + encoded > maxBytes) {
                        return records;
                    }
                    records.add(record);
                    used += encoded;
                    if (used >= maxBytes) {
                        return records;
                    }
                }
            }
            return records;
        } catch (IOException exception) {
            throw disk("read partition log", exception);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Drops every record at or after {@code offset} and moves the log end there.
     * The high watermark is pulled back when it would point past the new end.
     */
    public void truncateTo(long offset) {
        lock.writeLock().lock();
        try {
            if (offset >= nextOffset.get()) {
                return;
            }
            long target = Math.max(0L, offset);
            List<LogSegment> kept = new ArrayList<>();
            for (LogSegment segment : segments) {
                if (segment.baseOffset() >= target) {
                    segment.delete();
                } else {
                    kept.add(segment);
                }
            }
            if (kept.isEmpty()) {
                active = LogSegment.create(directory, target, config.indexIntervalBytes());
                segments = new ArrayList<>();
                segments.add(active);
            } else {
                segments = kept;
                active = kept.get(kept.size() - 1);
                active.truncateTo(target);
            }
            nextOffset.set(target);
            synchronized (watermarkLock) {
                if (highWatermark.get() > target) {
                    highWatermark.set(target);
                    persistHighWatermark(target);
                }
            }
            signal();
        } catch (IOException exception) {
            throw disk("truncate partition log", exception);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Moves the high watermark forward. The value is capped at the log end and ignored
     * when it is not ahead of the current watermark.
     */
    public void updateHighWatermark(long candidate) {
        synchronized (watermarkLock) {
            long capped = Math.min(candidate, nextOffset.get());
            if (capped <= highWatermark.get()) {
                return;
            }
            try {
                persistHighWatermark(capped);
            } catch (IOException exception) {
                throw disk("persist high watermark", exception);
            }
            highWatermark.set(capped);
        }
        signal();
    }

    public long logEndOffset() {
        return nextOffset.get();
    }

    public long highWatermark() {
        return highWatermark.get();
    }

    public long logStartOffset() {
        lock.readLock().lock();
        try {
            return segments.get(0).baseOffset();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int segmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Waits until the log end is strictly greater than {@code offset}, or until the timeout.
     *
     * @return the log end offset observed when the wait finished
     */
    public long awaitLogEnd(long offset, long timeoutMs) {
        return await(timeoutMs, () -> nextOffset.get() > offset, nextOffset);
    }

    /**
     * Waits until the high watermark reaches {@code target}. {@code target} is exclusive,
     * matching the watermark itself: a watermark of {@code n} means offsets {@code [0, n)} are committed.
     *
     * @return {@code true} when the watermark reached the target
     */
    public boolean awaitHighWatermark(long target, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMs));
        awaitLock.lock();
        try {
            while (highWatermark.get() < target) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return highWatermark.get() >= target;
                }
                try {
                    dataAvailable.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            awaitLock.unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            for (LogSegment segment : segments) {
                segment.close();
            }
        } catch (IOException exception) {
            throw disk("close partition log", exception);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void appendAll(List<LogRecord> records) {
        try {
            for (LogRecord record : records) {
                int encoded = RecordFormat.encodedSize(record);
                if (encoded > Limits.MAX_MESSAGE_BYTES) {
                    throw new KafkaException(Errors.MESSAGE_TOO_LARGE);
                }
                if (active.size() > 0 && active.size() + encoded > config.segmentBytes()) {
                    active.force();
                    active = LogSegment.create(directory, record.offset(), config.indexIntervalBytes());
                    segments.add(active);
                }
                active.append(record);
            }
            active.force();
            nextOffset.set(records.get(records.size() - 1).offset() + 1);
            signal();
        } catch (IOException exception) {
            throw disk("append to partition log", exception);
        }
    }

    private long await(long timeoutMs, BooleanSupplier ready, AtomicLong observed) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMs));
        awaitLock.lock();
        try {
            while (!ready.getAsBoolean()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return observed.get();
                }
                try {
                    dataAvailable.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return observed.get();
                }
            }
            return observed.get();
        } finally {
            awaitLock.unlock();
        }
    }

    private void signal() {
        awaitLock.lock();
        try {
            dataAvailable.signalAll();
        } finally {
            awaitLock.unlock();
        }
    }

    private void loadHighWatermark() throws IOException {
        Path path = directory.resolve(HIGH_WATERMARK_FILE);
        if (!Files.exists(path)) {
            highWatermark.set(0L);
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read < 0) {
                    highWatermark.set(0L);
                    return;
                }
            }
            buffer.flip();
            long stored = buffer.getLong();
            if (stored < 0 || stored > nextOffset.get()) {
                stored = Math.max(0L, Math.min(stored, nextOffset.get()));
            }
            highWatermark.set(stored);
        }
    }

    private void persistHighWatermark(long value) throws IOException {
        Path path = directory.resolve(HIGH_WATERMARK_FILE);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        buffer.flip();
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private static KafkaException disk(String action, IOException exception) {
        return new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "failed to " + action, exception);
    }
}
