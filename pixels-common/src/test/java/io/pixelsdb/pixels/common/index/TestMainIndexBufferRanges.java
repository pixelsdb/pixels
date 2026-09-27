/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.Test;
import java.util.Random;
import java.util.TreeMap;
import static org.junit.jupiter.api.Assertions.*;

class TestMainIndexBufferRanges {
    private static final long FILE_ID = 91;
    private static final int RANDOM_OPERATIONS = 1000;
    private static final int RANDOM_ROW_DOMAIN = 512;
    private static final int MAX_RANGE_ROWS = 32;
    private static final long RANDOM_SEED = 1741;

    @Test
    void joinsBothNeighborsAndKeepsRowGroupBoundaries() throws Exception {
        try (MainIndexBuffer buffer = new MainIndexBuffer(new MainIndexCache())) {
            assertTrue(buffer.insertRange(range(20, 30, 0, 20)));
            assertTrue(buffer.insertRange(range(0, 10, 0, 0)));
            assertTrue(buffer.insertRange(range(10, 20, 0, 10)));
            assertTrue(buffer.insertRange(range(30, 40, 1, 0)));
            MainIndexBuffer.FlushSnapshot snapshot = buffer.snapshotForFlush(FILE_ID);
            assertEquals(40, snapshot.getEntryCount());
            assertEquals(2, snapshot.getRowIdRanges().size());
            assertRange(range(0, 30, 0, 0), snapshot.getRowIdRanges().get(0));
            assertRange(range(30, 40, 1, 0), snapshot.getRowIdRanges().get(1));
            assertEquals(location(1, 9), buffer.lookup(39));
            assertNull(buffer.lookup(40));
            assertFalse(buffer.insert(15, location(2, 0)));
            assertFalse(buffer.insertRange(range(5, 35, 2, 0)));
            assertEquals(location(0, 15), buffer.lookup(15));
            buffer.discardFlushed(snapshot);
            assertTrue(buffer.cachedFileIds().isEmpty());
            assertNull(buffer.lookup(15));
        }
    }

    @Test
    void mixesPointsAndRangesAndRejectsStaleFlushSnapshot() throws Exception {
        try (MainIndexBuffer buffer = new MainIndexBuffer(new MainIndexCache())) {
            assertTrue(buffer.insert(10, location(0, 10)));
            assertFalse(buffer.insertRange(range(0, 20, 0, 0)));
            assertNull(buffer.lookup(0));
            assertTrue(buffer.insertRange(range(0, 10, 0, 0)));
            assertTrue(buffer.insertRange(range(11, 20, 0, 11)));
            MainIndexBuffer.FlushSnapshot snapshot = buffer.snapshotForFlush(FILE_ID);
            assertEquals(20, snapshot.getEntryCount());
            assertEquals(1, snapshot.getRowIdRanges().size());
            assertRange(range(0, 20, 0, 0), snapshot.getRowIdRanges().get(0));
            assertTrue(buffer.insertRange(range(20, 30, 0, 20)));
            assertThrows(MainIndexException.class, () -> buffer.discardFlushed(snapshot));
            assertEquals(location(0, 25), buffer.lookup(25));
            MainIndexBuffer.FlushSnapshot retry = buffer.snapshotForFlush(FILE_ID);
            assertEquals(30, retry.getEntryCount());
            assertRange(range(0, 30, 0, 0), retry.getRowIdRanges().get(0));
        }
    }

    @Test
    void randomMixedWritesMatchPointMapAndFlushCoverage() throws Exception {
        Random random = new Random(RANDOM_SEED);
        TreeMap<Long, IndexProto.RowLocation> expected = new TreeMap<>();
        try (MainIndexBuffer buffer = new MainIndexBuffer(new MainIndexCache())) {
            for (int operation = 0; operation < RANDOM_OPERATIONS; operation++) {
                long start = random.nextInt(RANDOM_ROW_DOMAIN);
                int count = 1 + random.nextInt(MAX_RANGE_ROWS);
                int group = random.nextInt(MAX_RANGE_ROWS);
                if (random.nextBoolean()) {
                    IndexProto.RowLocation point = location(group, (int) start);
                    assertEquals(!expected.containsKey(start), buffer.insert(start, point));
                    expected.putIfAbsent(start, point);
                } else {
                    boolean overlap = !expected.subMap(start, start + count).isEmpty();
                    assertEquals(!overlap, buffer.insertRange(range(start, start + count, group, (int) start)));
                    if (!overlap) {
                        for (long id = start; id < start + count; id++)
                            expected.put(id, location(group, (int) id));
                    }
                }
            }
            for (long id = 0; id < RANDOM_ROW_DOMAIN + MAX_RANGE_ROWS; id++)
                assertEquals(expected.get(id), buffer.lookup(id));
            MainIndexBuffer.FlushSnapshot snapshot = buffer.snapshotForFlush(FILE_ID);
            assertEquals(expected.size(), snapshot.getEntryCount());
            TreeMap<Long, IndexProto.RowLocation> flushed = new TreeMap<>();
            for (RowIdRange range : snapshot.getRowIdRanges()) {
                for (long id = range.getRowIdStart(); id < range.getRowIdEnd(); id++)
                    assertNull(flushed.put(id, location(range.getRgId(),
                            range.getRgRowOffsetStart() + (int) (id - range.getRowIdStart()))));
            }
            assertEquals(expected, flushed);
        }
    }

    private RowIdRange range(long start, long end, int group, int offset) {
        return new RowIdRange(start, end, FILE_ID, group, offset, offset + (int) (end - start));
    }

    private IndexProto.RowLocation location(int group, int offset) {
        return IndexProto.RowLocation.newBuilder().setFileId(FILE_ID).setRgId(group)
                .setRgRowOffset(offset).build();
    }

    private void assertRange(RowIdRange expected, RowIdRange actual) {
        assertEquals(expected.getRowIdStart(), actual.getRowIdStart());
        assertEquals(expected.getRowIdEnd(), actual.getRowIdEnd());
        assertEquals(expected.getFileId(), actual.getFileId());
        assertEquals(expected.getRgId(), actual.getRgId());
        assertEquals(expected.getRgRowOffsetStart(), actual.getRgRowOffsetStart());
        assertEquals(expected.getRgRowOffsetEnd(), actual.getRgRowOffsetEnd());
    }
}
