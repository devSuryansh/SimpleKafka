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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.CRC32;

import com.simplekafka.common.ByteBuf;
import com.simplekafka.common.ByteReader;
import com.simplekafka.common.Errors;
import com.simplekafka.common.KafkaException;
import com.simplekafka.common.LogRecord;

/**
 * One pair of {@code .log} and {@code .index} files.
 *
 * <p>The log is append-only. The index is a sparse map from relative offset to file
 * position, stored as pairs of 4-byte integers. Recovery scans the log, drops a torn
 * tail, and rewrites the index so a crash cannot leave the two files disagreeing.
 *
 * <p>Reads use absolute file positions, so concurrent readers do not share a cursor.
 * Callers still serialize readers against writers with the partition lock.
 */
final class LogSegment implements Closeable {
    private final long baseOffset;
    private final Path logPath;
    private final Path indexPath;
    private final FileChannel channel;
    private final int indexIntervalBytes;
    private final List<IndexEntry> index = new ArrayList<>();
    private long size;
    private long endOffset;
    private int lastIndexPosition = -1;

    private LogSegment(long baseOffset, Path logPath, Path indexPath, FileChannel channel, int indexIntervalBytes)
            throws IOException {
        this.baseOffset = baseOffset;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.channel = channel;
        this.indexIntervalBytes = indexIntervalBytes;
        this.size = channel.size();
        recover();
    }

    static LogSegment create(Path directory, long baseOffset, int indexIntervalBytes) throws IOException {
        Path logPath = directory.resolve(logName(baseOffset));
        Path indexPath = directory.resolve(indexName(baseOffset));
        FileChannel channel = FileChannel.open(logPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new LogSegment(baseOffset, logPath, indexPath, channel, indexIntervalBytes);
    }

    static LogSegment open(Path logPath, int indexIntervalBytes) throws IOException {
        long baseOffset = parseBaseOffset(logPath.getFileName().toString());
        Path indexPath = logPath.resolveSibling(indexName(baseOffset));
        FileChannel channel = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new LogSegment(baseOffset, logPath, indexPath, channel, indexIntervalBytes);
    }

    static String logName(long offset) {
        return String.format(Locale.ROOT, "%020d.log", offset);
    }

    static String indexName(long offset) {
        return String.format(Locale.ROOT, "%020d.index", offset);
    }

    static long parseBaseOffset(String fileName) {
        int dot = fileName.indexOf('.');
        if (dot != 20) {
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "not a segment file: " + fileName);
        }
        return Long.parseLong(fileName.substring(0, dot));
    }

    long baseOffset() {
        return baseOffset;
    }

    long endOffset() {
        return endOffset;
    }

    long size() {
        return size;
    }

    void append(LogRecord record) throws IOException {
        byte[] body = RecordFormat.crcBody(record);
        CRC32 crc = new CRC32();
        crc.update(body);
        ByteBuf frame = new ByteBuf(4 + 1 + 4 + body.length);
        frame.writeInt(1 + 4 + body.length);
        frame.writeByte(RecordFormat.MAGIC);
        frame.writeInt((int) crc.getValue());
        frame.writeBytes(body);
        byte[] bytes = frame.toByteArray();
        if (size > Integer.MAX_VALUE - bytes.length) {
            throw new KafkaException(Errors.MESSAGE_TOO_LARGE, "segment exceeded 2 GiB");
        }
        int position = (int) size;
        writeFully(channel, ByteBuffer.wrap(bytes), position);
        size += bytes.length;
        endOffset = record.offset() + 1;
        maybeIndex(record.offset(), position);
    }

    List<LogRecord> read(long startOffset, int maxBytes, long endExclusive) throws IOException {
        if (maxBytes <= 0 || startOffset >= endOffset || startOffset >= endExclusive) {
            return List.of();
        }
        int position = positionFor(startOffset);
        List<LogRecord> records = new ArrayList<>();
        int used = 0;
        while (position < size) {
            ScanHit hit = readAt(position);
            if (hit.record == null) {
                break;
            }
            if (hit.record.offset() >= endExclusive) {
                break;
            }
            if (hit.record.offset() >= startOffset) {
                int encoded = hit.next - position;
                if (!records.isEmpty() && used + encoded > maxBytes) {
                    break;
                }
                records.add(hit.record);
                used += encoded;
                if (used >= maxBytes) {
                    break;
                }
            }
            position = hit.next;
        }
        return records;
    }

    void truncateTo(long offset) throws IOException {
        if (offset >= endOffset) {
            return;
        }
        int position = 0;
        long expected = baseOffset;
        if (offset > baseOffset) {
            position = positionFor(offset);
            while (position < size) {
                ScanHit hit = readAt(position);
                if (hit.record == null || hit.record.offset() >= offset) {
                    break;
                }
                expected = hit.record.offset() + 1;
                position = hit.next;
            }
        }
        applyTruncation(position, offset < baseOffset ? baseOffset : Math.max(expected, Math.min(offset, endOffset)));
        if (offset <= baseOffset) {
            endOffset = baseOffset;
        } else {
            endOffset = offset;
        }
    }

    void force() throws IOException {
        channel.force(true);
        writeIndex();
    }

    void delete() throws IOException {
        close();
        Files.deleteIfExists(logPath);
        Files.deleteIfExists(indexPath);
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
            writeIndex();
            channel.close();
        }
    }

    private void recover() throws IOException {
        index.clear();
        lastIndexPosition = -1;
        long position = 0;
        long expected = baseOffset;
        boolean torn = false;
        while (position < size) {
            ScanHit hit = readAt(position);
            if (hit.record == null) {
                torn = true;
                break;
            }
            if (hit.record.offset() != expected) {
                torn = true;
                break;
            }
            maybeIndex(hit.record.offset(), (int) position);
            expected = hit.record.offset() + 1;
            position = hit.next;
        }
        if (torn || position != size) {
            applyTruncation(position, expected);
        }
        endOffset = expected;
        writeIndex();
    }

    private void applyTruncation(long position, long newEnd) throws IOException {
        channel.truncate(position);
        size = position;
        index.removeIf(entry -> entry.position >= position);
        if (index.isEmpty()) {
            lastIndexPosition = -1;
        } else {
            lastIndexPosition = index.get(index.size() - 1).position;
        }
        endOffset = newEnd;
    }

    private void maybeIndex(long offset, int position) {
        long relative = offset - baseOffset;
        if (relative < 0 || relative > Integer.MAX_VALUE) {
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "offset does not fit in the segment index");
        }
        if (index.isEmpty() || position - lastIndexPosition >= indexIntervalBytes) {
            index.add(new IndexEntry((int) relative, position));
            lastIndexPosition = position;
        }
    }

    private int positionFor(long offset) {
        if (index.isEmpty() || offset <= baseOffset) {
            return 0;
        }
        int relative = (int) Math.min(offset - baseOffset, Integer.MAX_VALUE);
        int low = 0;
        int high = index.size() - 1;
        int found = 0;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (index.get(mid).relativeOffset <= relative) {
                found = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return index.get(found).position;
    }

    private ScanHit readAt(long position) throws IOException {
        if (position > size || size - position < 4) {
            return ScanHit.end();
        }
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        readFully(channel, lengthBuffer, position);
        int length = lengthBuffer.getInt(0);
        int minimum = RecordFormat.OVERHEAD - 4;
        if (length < minimum || position + 4L + length > size) {
            return ScanHit.end();
        }
        ByteBuffer payload = ByteBuffer.allocate(length);
        readFully(channel, payload, position + 4);
        payload.flip();
        byte magic = payload.get();
        int crc = payload.getInt();
        byte[] body = new byte[payload.remaining()];
        payload.get(body);
        if (magic != RecordFormat.MAGIC) {
            return ScanHit.end();
        }
        CRC32 checksum = new CRC32();
        checksum.update(body);
        if ((int) checksum.getValue() != crc) {
            return ScanHit.end();
        }
        try {
            ByteReader reader = new ByteReader(body);
            long offset = reader.readLong();
            long timestamp = reader.readLong();
            byte[] key = reader.readNullableBytes();
            byte[] value = reader.readNullableBytes();
            if (reader.remaining() != 0) {
                return ScanHit.end();
            }
            int next = (int) (position + 4 + length);
            return ScanHit.record(new LogRecord(offset, timestamp, key, value), next);
        } catch (KafkaException exception) {
            return ScanHit.end();
        }
    }

    private void writeIndex() throws IOException {
        ByteBuf buf = new ByteBuf(Math.max(8, index.size() * 8));
        for (IndexEntry entry : index) {
            buf.writeInt(entry.relativeOffset);
            buf.writeInt(entry.position);
        }
        byte[] bytes = buf.toByteArray();
        try (FileChannel indexChannel = FileChannel.open(indexPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            writeFully(indexChannel, ByteBuffer.wrap(bytes), 0);
            indexChannel.force(true);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long cursor = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, cursor);
            if (read < 0) {
                throw new EOFException("short read at " + cursor);
            }
            cursor += read;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long cursor = position;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, cursor);
            if (written < 0) {
                throw new EOFException("short write at " + cursor);
            }
            cursor += written;
        }
    }

    private record IndexEntry(int relativeOffset, int position) {
    }

    private static final class ScanHit {
        private final LogRecord record;
        private final int next;

        private ScanHit(LogRecord record, int next) {
            this.record = record;
            this.next = next;
        }

        static ScanHit end() {
            return new ScanHit(null, -1);
        }

        static ScanHit record(LogRecord record, int next) {
            return new ScanHit(record, next);
        }
    }
}
