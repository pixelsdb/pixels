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
package io.pixelsdb.pixels.index.main.sqlite;

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestSqliteMainIndex
{
    private static long nextTableId = 100L;
    long tableId;
    String sqlitePath;
    MainIndex mainIndex;

    @BeforeEach
    public void setUp() throws MainIndexException
    {
        tableId = nextTableId++;
        // Create SQLite Directory
        try
        {
            sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
            FileUtils.forceMkdir(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to create SQLite test directory: " + e.getMessage());
        }

        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        MainIndexFactory.Instance().closeIndex(tableId, true);

        // Clear SQLite Directory
        try
        {
            FileUtils.deleteDirectory(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to clean up SQLite test directory: " + e.getMessage());
        }
    }

    @Test
    public void testFlushCacheMissingFileIsNoop() throws MainIndexException
    {
        Assertions.assertTrue(mainIndex.flushCache(987654321L));
    }

    @Test
    public void testFlushCacheAcceptsMatchingCommittedMarker() throws Exception
    {
        long fileId = 42L;
        RowIdRange firstRange = new RowIdRange(5000L, 5002L, fileId, 0, 0, 2);
        RowIdRange secondRange = new RowIdRange(5010L, 5011L, fileId, 1, 0, 1);
        List<RowIdRange> ranges = new ArrayList<>();
        ranges.add(firstRange);
        ranges.add(secondRange);
        putMainIndexEntry(5000L, fileId, 0, 0);
        putMainIndexEntry(5001L, fileId, 0, 1);
        putMainIndexEntry(5010L, fileId, 1, 0);

        insertRange(firstRange);
        insertRange(secondRange);
        insertFlushMarker(fileId, 3, ranges);
        Assertions.assertEquals(2, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));

        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(2, countRangesForFile(fileId));
        assertLocation(5000L, fileId, 0, 0);
        assertLocation(5001L, fileId, 0, 1);
        assertLocation(5010L, fileId, 1, 0);

        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(2, countRangesForFile(fileId));
    }

    @Test
    public void testFlushCacheConflictingMarkerKeepsBufferRetryable() throws Exception
    {
        long fileId = 43L;
        putMainIndexEntry(6000L, fileId, 0, 0);
        putMainIndexEntry(6001L, fileId, 0, 1);
        putMainIndexEntry(6010L, fileId, 1, 0);

        insertFlushMarker(fileId, 3, new ArrayList<>());

        Assertions.assertThrows(MainIndexException.class, () -> mainIndex.flushCache(fileId));
        Assertions.assertEquals(0, countExactRanges(6010L, 6011L));
        assertLocation(6000L, fileId, 0, 0);
        assertLocation(6010L, fileId, 1, 0);

        deleteFlushMarker(fileId);
        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(2, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
        assertLocation(6000L, fileId, 0, 0);
        assertLocation(6010L, fileId, 1, 0);
    }

    @Test
    public void testFlushCacheRangeWithoutMarkerFailsAndKeepsBufferRetryable() throws Exception
    {
        long fileId = 44L;
        putMainIndexEntry(7000L, fileId, 0, 0);
        putMainIndexEntry(7001L, fileId, 0, 1);
        putMainIndexEntry(7010L, fileId, 1, 0);

        insertRange(new RowIdRange(7000L, 7002L, fileId, 0, 0, 2));

        Assertions.assertThrows(MainIndexException.class, () -> mainIndex.flushCache(fileId));
        Assertions.assertEquals(0, countExactRanges(7010L, 7011L));
        Assertions.assertEquals(0, countFlushMarkersForFile(fileId));
        assertLocation(7000L, fileId, 0, 0);
        assertLocation(7010L, fileId, 1, 0);

        deleteExactRange(7000L, 7002L);
        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(2, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
    }

    @Test
    public void testFlushCacheRejectsFlushMarkerMetadataMismatches() throws Exception
    {
        long fileId = 45L;
        putMainIndexEntry(8000L, fileId, 0, 0);
        putMainIndexEntry(8001L, fileId, 0, 1);

        List<RowIdRange> ranges = Arrays.asList(new RowIdRange(8000L, 8002L, fileId, 0, 0, 2));
        byte[] rangeHash = buildRangeHash(ranges);

        insertFlushMarker(fileId, 1, ranges.size(), rangeHash);
        assertFlushFailsAndBufferSurvives(fileId, 8000L, 8001L);

        deleteFlushMarker(fileId);
        insertFlushMarker(fileId, 2, ranges.size() + 1, rangeHash);
        assertFlushFailsAndBufferSurvives(fileId, 8000L, 8001L);

        deleteFlushMarker(fileId);
        byte[] badHash = rangeHash.clone();
        badHash[0] = (byte) (badHash[0] ^ 0x7f);
        insertFlushMarker(fileId, 2, ranges.size(), badHash);
        assertFlushFailsAndBufferSurvives(fileId, 8000L, 8001L);

        deleteFlushMarker(fileId);
        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(1, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
    }

    @Test
    public void testFlushCacheRollsBackRangesWhenMarkerInsertFails() throws Exception
    {
        long fileId = 46L;
        putMainIndexEntry(9000L, fileId, 0, 0);
        putMainIndexEntry(9001L, fileId, 0, 1);
        putMainIndexEntry(9010L, fileId, 1, 0);

        createFailingFlushMarkerTrigger(fileId);
        Assertions.assertThrows(MainIndexException.class, () -> mainIndex.flushCache(fileId));
        Assertions.assertEquals(0, countRangesForFile(fileId));
        Assertions.assertEquals(0, countFlushMarkersForFile(fileId));
        assertLocation(9000L, fileId, 0, 0);
        assertLocation(9010L, fileId, 1, 0);

        dropFailingFlushMarkerTrigger();
        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(2, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
    }

    @Test
    public void testFlushCacheConvergesAfterUnknownCommittedStateWithOutOfOrderBuffer() throws Exception
    {
        long fileId = 48L;
        List<RowIdRange> committedRanges = Arrays.asList(
                new RowIdRange(11000L, 11003L, fileId, 0, 0, 3),
                new RowIdRange(11010L, 11012L, fileId, 1, 7, 9));

        putMainIndexEntry(11002L, fileId, 0, 2);
        putMainIndexEntry(11000L, fileId, 0, 0);
        putMainIndexEntry(11010L, fileId, 1, 7);
        putMainIndexEntry(11001L, fileId, 0, 1);
        putMainIndexEntry(11011L, fileId, 1, 8);

        for (RowIdRange range : committedRanges)
        {
            insertRange(range);
        }
        insertFlushMarker(fileId, 5, committedRanges);

        Assertions.assertTrue(mainIndex.flushCache(fileId));
        Assertions.assertEquals(2, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
        assertNoInvalidRanges(fileId);
        assertLocation(11000L, fileId, 0, 0);
        assertLocation(11002L, fileId, 0, 2);
        assertLocation(11011L, fileId, 1, 8);
    }

    @Test
    public void testFlushCacheFailureForOneFileDoesNotDiscardOtherFileBuffers() throws Exception
    {
        long failingFileId = 49L;
        long healthyFileId = 50L;
        putMainIndexEntry(12000L, failingFileId, 0, 0);
        putMainIndexEntry(12001L, failingFileId, 0, 1);
        putMainIndexEntry(12100L, healthyFileId, 0, 0);
        putMainIndexEntry(12101L, healthyFileId, 0, 1);

        createFailingFlushMarkerTrigger(failingFileId);
        Assertions.assertThrows(MainIndexException.class, () -> mainIndex.flushCache(failingFileId));
        Assertions.assertEquals(0, countRangesForFile(failingFileId));
        Assertions.assertEquals(0, countFlushMarkersForFile(failingFileId));
        assertLocation(12000L, failingFileId, 0, 0);

        Assertions.assertTrue(mainIndex.flushCache(healthyFileId));
        Assertions.assertEquals(1, countRangesForFile(healthyFileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(healthyFileId));
        assertLocation(12101L, healthyFileId, 0, 1);

        dropFailingFlushMarkerTrigger();
        Assertions.assertTrue(mainIndex.flushCache(failingFileId));
        Assertions.assertEquals(1, countRangesForFile(failingFileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(failingFileId));
    }

    @Test
    public void testPutEntriesFlushesDurableRangesAndLocations() throws Exception
    {
        long fileId = 51L;
        List<IndexProto.PrimaryIndexEntry> entries = Arrays.asList(
                primaryEntry(13002L, fileId, 0, 2),
                primaryEntry(13000L, fileId, 0, 0),
                primaryEntry(13001L, fileId, 0, 1),
                primaryEntry(13020L, fileId, 2, 4),
                primaryEntry(13021L, fileId, 2, 5));

        assertAllTrue(mainIndex.putEntries(entries));
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(2, ranges.size());
        assertRange(ranges.get(0), 13000L, 13003L, fileId, 0, 0, 3);
        assertRange(ranges.get(1), 13020L, 13022L, fileId, 2, 4, 6);
        assertNoInvalidRanges(fileId);

        List<IndexProto.RowLocation> locations = mainIndex.getLocations(Arrays.asList(13000L, 13002L, 13021L));
        Assertions.assertEquals(3, locations.size());
        Assertions.assertEquals(0, locations.get(0).getRgRowOffset());
        Assertions.assertEquals(2, locations.get(1).getRgRowOffset());
        Assertions.assertEquals(5, locations.get(2).getRgRowOffset());
    }

    @Test
    public void testCloseConvergesWhenPreviousFlushCommittedButBufferSurvived() throws Exception
    {
        long fileId = 52L;
        RowIdRange committedRange = new RowIdRange(14000L, 14002L, fileId, 0, 0, 2);
        putMainIndexEntry(14000L, fileId, 0, 0);
        putMainIndexEntry(14001L, fileId, 0, 1);

        insertRange(committedRange);
        insertFlushMarker(fileId, 2, Arrays.asList(committedRange));

        MainIndexFactory.Instance().closeIndex(tableId, false);
        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);

        Assertions.assertEquals(1, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
        assertLocation(14000L, fileId, 0, 0);
        assertLocation(14001L, fileId, 0, 1);
    }

    @Test
    public void testDeleteRowIdRangeRemovesExactRangeWithoutInvalidResidue() throws Exception
    {
        long fileId = 53L;
        putContiguousEntries(fileId, 0, 15000L, 15004L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(15000L, 15004L, fileId, 0, 0, 4)));

        Assertions.assertEquals(0, countRangesForFile(fileId));
        assertNoInvalidRanges(fileId);
        for (long rowId = 15000L; rowId < 15004L; rowId++)
        {
            assertLocationMissing(rowId);
        }
    }

    @Test
    public void testDeleteRowIdRangeSplitsMiddleRangeForRecoveryCleanup() throws Exception
    {
        long fileId = 54L;
        putContiguousEntries(fileId, 0, 16000L, 16010L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(16003L, 16007L, fileId, 0, 3, 7)));

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(2, ranges.size());
        assertRange(ranges.get(0), 16000L, 16003L, fileId, 0, 0, 3);
        assertRange(ranges.get(1), 16007L, 16010L, fileId, 0, 7, 10);
        assertNoInvalidRanges(fileId);
        assertLocation(16002L, fileId, 0, 2);
        assertLocationMissing(16003L);
        assertLocationMissing(16006L);
        assertLocation(16007L, fileId, 0, 7);
    }

    @Test
    public void testDeleteRowIdRangeTrimsBordersAndDeletesCoveredRanges() throws Exception
    {
        long fileId = 55L;
        putContiguousEntries(fileId, 0, 17000L, 17005L, 0);
        putContiguousEntries(fileId, 1, 17010L, 17015L, 0);
        putContiguousEntries(fileId, 2, 17020L, 17025L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(17003L, 17022L, fileId, 0, 3, 22)));

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(2, ranges.size());
        assertRange(ranges.get(0), 17000L, 17003L, fileId, 0, 0, 3);
        assertRange(ranges.get(1), 17022L, 17025L, fileId, 2, 2, 5);
        assertNoInvalidRanges(fileId);
        assertLocation(17002L, fileId, 0, 2);
        assertLocationMissing(17010L);
        assertLocationMissing(17021L);
        assertLocation(17022L, fileId, 2, 2);
    }

    @Test
    public void testDeleteRowIdRangeLeftAlignedTrimsLeadingPortionOfSingleRange() throws Exception
    {
        long fileId = 60L;
        putContiguousEntries(fileId, 0, 21000L, 21010L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        // Delete [21000, 21003) which shares its left edge with the existing range [21000, 21010).
        // Expected to trim the leading portion and keep [21003, 21010).
        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(21000L, 21003L, fileId, 0, 0, 3)));

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(1, ranges.size());
        assertRange(ranges.get(0), 21003L, 21010L, fileId, 0, 3, 10);
        assertNoInvalidRanges(fileId);
        assertLocationMissing(21000L);
        assertLocationMissing(21002L);
        assertLocation(21003L, fileId, 0, 3);
        assertLocation(21009L, fileId, 0, 9);
    }

    @Test
    public void testDeleteRowIdRangeRightAlignedTrimsTrailingPortionOfSingleRange() throws Exception
    {
        long fileId = 61L;
        putContiguousEntries(fileId, 0, 22000L, 22010L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        // Delete [22007, 22010) which shares its right edge with the existing range [22000, 22010).
        // Expected to trim the trailing portion and keep [22000, 22007).
        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(22007L, 22010L, fileId, 0, 7, 10)));

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(1, ranges.size());
        assertRange(ranges.get(0), 22000L, 22007L, fileId, 0, 0, 7);
        assertNoInvalidRanges(fileId);
        assertLocation(22000L, fileId, 0, 0);
        assertLocation(22006L, fileId, 0, 6);
        assertLocationMissing(22007L);
        assertLocationMissing(22009L);
    }

    @Test
    public void testDeleteRowIdRangeFullyContainsSingleRangeRemovesItWithoutResidue() throws Exception
    {
        long fileId = 62L;
        // Single committed range [23000, 23004) sitting in isolation.
        putContiguousEntries(fileId, 0, 23000L, 23004L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        // Delete [22990, 23010) which strictly contains the entire range.
        // No border range is partially overlapped, so the bulk DELETE clause should remove the range
        // and leave no residue or split-out ranges.
        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(22990L, 23010L, fileId, 0, 0, 20)));

        Assertions.assertEquals(0, countRangesForFile(fileId));
        assertNoInvalidRanges(fileId);
        for (long rowId = 23000L; rowId < 23004L; rowId++)
        {
            assertLocationMissing(rowId);
        }
    }

    @Test
    public void testDeleteRowIdRangeMissingAllRangesIsNoop() throws Exception
    {
        long fileId = 63L;
        // Persist a single range [24000, 24004) so the table is non-empty.
        putContiguousEntries(fileId, 0, 24000L, 24004L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        // Delete a row id window that does not overlap any committed range; should be a no-op.
        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(30000L, 30010L, fileId, 0, 0, 10)));

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(1, ranges.size());
        assertRange(ranges.get(0), 24000L, 24004L, fileId, 0, 0, 4);
        assertNoInvalidRanges(fileId);
        assertLocation(24000L, fileId, 0, 0);
        assertLocation(24003L, fileId, 0, 3);
        // Row ids inside the deleted (but never committed) window remain unknown.
        assertLocationMissing(30000L);
        assertLocationMissing(30009L);
    }

    @Test
    public void testDeleteRowIdRangeRollsBackSplitWhenRightRangeInsertFails() throws Exception
    {
        long fileId = 57L;
        putContiguousEntries(fileId, 0, 19000L, 19010L, 0);
        Assertions.assertTrue(mainIndex.flushCache(fileId));

        createFailingRangeInsertTrigger(19007L);
        Assertions.assertThrows(MainIndexException.class,
                () -> mainIndex.deleteRowIdRange(new RowIdRange(19003L, 19007L, fileId, 0, 3, 7)));
        dropFailingRangeInsertTrigger();

        List<RowIdRange> ranges = listRangesForFile(fileId);
        Assertions.assertEquals(1, ranges.size());
        assertRange(ranges.get(0), 19000L, 19010L, fileId, 0, 0, 10);
        assertNoInvalidRanges(fileId);
        assertLocation(19003L, fileId, 0, 3);
        assertLocation(19007L, fileId, 0, 7);
    }

    @Test
    public void testDeleteRowIdRangeRejectsEmptyOrReversedRange() throws Exception
    {
        Assertions.assertThrows(MainIndexException.class,
                () -> mainIndex.deleteRowIdRange(new RowIdRange(20000L, 20000L, 58L, 0, 0, 0)));
        Assertions.assertThrows(MainIndexException.class,
                () -> mainIndex.deleteRowIdRange(new RowIdRange(20001L, 20000L, 58L, 0, 1, 0)));
    }

    @Test
    public void testCloseFlushesCacheWithMarkerAndReopenReadsRows() throws Exception
    {
        long fileId = 47L;
        putMainIndexEntry(10000L, fileId, 0, 0);
        putMainIndexEntry(10001L, fileId, 0, 1);

        MainIndexFactory.Instance().closeIndex(tableId, false);
        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);

        Assertions.assertEquals(1, countRangesForFile(fileId));
        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
        assertLocation(10000L, fileId, 0, 0);
        assertLocation(10001L, fileId, 0, 1);
    }

    @Test
    public void testPutAndGetLocation() throws MainIndexException
    {
        long rowId = 1000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(1).setRgId(1).setRgRowOffset(0).build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        IndexProto.RowLocation fetched = mainIndex.getLocation(rowId);
        Assertions.assertNotNull(fetched);
        Assertions.assertEquals(1, fetched.getFileId());
        Assertions.assertEquals(1, fetched.getRgId());
        Assertions.assertEquals(0, fetched.getRgRowOffset());
    }

    @Test
    public void testFlushCacheAndDeleteEntry() throws Exception
    {
        long rowId = 2000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(2).setRgId(2).setRgRowOffset(0).build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId, rowId + 1,
                2, 2, 0, 1)));
        assertLocationMissing(rowId);
        Assertions.assertEquals(0, countRangesForFile(2));

        location = location.toBuilder().setFileId(3).build();
        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(3));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId - 1, rowId + 1,
                3, 2, 0, 2)));
        assertLocationMissing(rowId);
        Assertions.assertEquals(0, countRangesForFile(3));

        location = location.toBuilder().setFileId(4).build();
        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(4));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId - 1, rowId,
                4, 2, 0, 1)));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
    }

    @Test
    @Tag("performance")
    public void testFlushCachePerformanceSmoke() throws Exception
    {
        int entryCount = Integer.getInteger("sqlite.main.index.perf.smoke.entries", 50_000);
        long timeoutSeconds = Long.getLong("sqlite.main.index.perf.smoke.timeout.sec", 30L);
        long fileId = 56L;
        long rowIdBase = 18000L;
        long[] elapsedMs = new long[4];

        Assertions.assertTimeout(Duration.ofSeconds(timeoutSeconds), () -> {
            IndexProto.RowLocation.Builder locationBuilder = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId).setRgId(0);
            long start = System.nanoTime();
            for (int i = 0; i < entryCount; i++)
            {
                Assertions.assertTrue(mainIndex.putEntry(rowIdBase + i,
                        locationBuilder.setRgRowOffset(i).build()));
            }
            elapsedMs[0] = nanosToMillis(System.nanoTime() - start);

            start = System.nanoTime();
            Assertions.assertTrue(mainIndex.flushCache(fileId));
            elapsedMs[1] = nanosToMillis(System.nanoTime() - start);

            start = System.nanoTime();
            int sampleStep = Math.max(1, entryCount / 100);
            for (int i = 0; i < entryCount; i += sampleStep)
            {
                IndexProto.RowLocation location = mainIndex.getLocation(rowIdBase + i);
                Assertions.assertEquals(fileId, location.getFileId());
                Assertions.assertEquals(i, location.getRgRowOffset());
            }
            elapsedMs[2] = nanosToMillis(System.nanoTime() - start);

            start = System.nanoTime();
            Assertions.assertTrue(mainIndex.flushCache(fileId));
            Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(
                    rowIdBase, rowIdBase + entryCount, fileId, 0, 0, entryCount)));
            elapsedMs[3] = nanosToMillis(System.nanoTime() - start);
        });

        Assertions.assertEquals(1, countFlushMarkersForFile(fileId));
        Assertions.assertEquals(0, countRangesForFile(fileId));
        System.out.println("sqlite main index perf smoke entries=" + entryCount +
                ", putMs=" + elapsedMs[0] +
                ", flushMs=" + elapsedMs[1] +
                ", sampledGetMs=" + elapsedMs[2] +
                ", idempotentFlushAndDeleteMs=" + elapsedMs[3]);
    }

    @Test
    @Disabled("Manual performance smoke test; not a correctness gate.")
    @Tag("performance")
    public void testPutAndGetPerformance() throws MainIndexException
    {
        final long rowIdBase = 0L;
        final int entryCount = Integer.getInteger("sqlite.main.index.perf.entries", 10_000_000);
        IndexProto.RowLocation.Builder locationBuilder = IndexProto.RowLocation.newBuilder()
                .setFileId(1L).setRgId(0);
        long start = System.currentTimeMillis();
        for (int i = 0; i < entryCount; i++)
        {
            mainIndex.putEntry(rowIdBase + i, locationBuilder.setRgRowOffset(i).build());
        }
        System.out.println("put " + entryCount + " entries in " + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        for (int i = 0; i < entryCount; i++)
        {
            mainIndex.getLocation(rowIdBase + i);
        }
        System.out.println("get " + entryCount + " entries in " + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        mainIndex.flushCache(1);
        System.out.println("flush cache in " + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        mainIndex.deleteRowIdRange(new RowIdRange(
                0L, entryCount, 1L, 0, 0, entryCount));
        System.out.println("delete all entries in " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void testConcurrentAccess() throws Exception
    {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try
                {
                    long rowId = 3000L;

                    // test putEntry()
                    IndexProto.RowLocation dummyLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(100 + threadNum).setRgId(10).setRgRowOffset(0).build();
                    Assertions.assertTrue(mainIndex.putEntry(rowId + threadNum, dummyLocation));

                    // test getLocation()
                    IndexProto.RowLocation fetched = mainIndex.getLocation(rowId + threadNum);
                    Assertions.assertNotNull(fetched);
                    Assertions.assertEquals(100 + threadNum, fetched.getFileId());
                }
                finally
                {
                    latch.countDown();
                }
                return null;
            }));
        }

        latch.await();
        for (Future<Void> future : futures)
        {
            future.get();
        }
        executor.shutdown();
    }

    @Test
    public void testConcurrentPutAndDeleteRowIds() throws Exception
    {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);

        CountDownLatch putLatch = new CountDownLatch(threadCount);
        CountDownLatch deleteLatch = new CountDownLatch(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        // Create RowIdRange for every thread
        List<RowIdRange> ranges = new ArrayList<>();
        // create 10 row id ranges of the same file
        for (int i = 0; i < threadCount; i++)
        {
            long base = 10_000L + i * 100;
            ranges.add(new RowIdRange(base, base + 4, i, i, 0, 4));
        }

        // Concurrent putRowIdsOfRg
        for (int i = 0; i < threadCount; i++)
        {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try
                {
                    RowIdRange range = ranges.get(threadNum);
                    int offset = 0;
                    for (long id = range.getRowIdStart(); id < range.getRowIdEnd(); id++)
                    {
                        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                                .setFileId(range.getFileId()).setRgId(range.getRgId()).setRgRowOffset(offset++).build();
                        Assertions.assertTrue(mainIndex.putEntry(id, location));
                    }

                    for (long id = range.getRowIdStart(); id < range.getRowIdEnd(); id++)
                    {
                        IndexProto.RowLocation loc = mainIndex.getLocation(id);
                        Assertions.assertNotNull(loc);
                        Assertions.assertEquals(threadNum, loc.getFileId());
                        Assertions.assertEquals(threadNum, loc.getRgId());
                    }
                }
                finally
                {
                    putLatch.countDown();
                }
                return null;
            }));
        }

        // Wait for put method complete
        putLatch.await();

        // Concurrent deleteRowIdRange
        for (int i = 0; i < threadCount; i++)
        {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try
                {
                    mainIndex.flushCache(threadNum);
                    RowIdRange range = ranges.get(threadNum);
                    mainIndex.deleteRowIdRange(range);
                    for (long id = range.getRowIdStart(); id < range.getRowIdEnd(); id++)
                    {
                        assertLocationMissing(id);
                    }
                }
                finally
                {
                    deleteLatch.countDown();
                }
                return null;
            }));
        }

        deleteLatch.await();
        for (Future<Void> future : futures)
        {
            future.get();
        }
        executor.shutdown();
    }

    private void putMainIndexEntry(long rowId, long fileId, int rgId, int rgRowOffset)
    {
        Assertions.assertTrue(mainIndex.putEntry(rowId, IndexProto.RowLocation.newBuilder()
                .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffset).build()));
    }

    private void putContiguousEntries(long fileId, int rgId, long rowIdStart, long rowIdEnd, int rgRowOffsetStart)
    {
        int offset = rgRowOffsetStart;
        for (long rowId = rowIdStart; rowId < rowIdEnd; rowId++)
        {
            putMainIndexEntry(rowId, fileId, rgId, offset++);
        }
    }

    private IndexProto.PrimaryIndexEntry primaryEntry(long rowId, long fileId, int rgId, int rgRowOffset)
    {
        return IndexProto.PrimaryIndexEntry.newBuilder()
                .setRowId(rowId)
                .setRowLocation(IndexProto.RowLocation.newBuilder()
                        .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffset).build())
                .build();
    }

    private void assertAllTrue(List<Boolean> results)
    {
        for (Boolean result : results)
        {
            Assertions.assertTrue(result);
        }
    }

    private void assertLocation(long rowId, long fileId, int rgId, int rgRowOffset) throws MainIndexException
    {
        IndexProto.RowLocation location = mainIndex.getLocation(rowId);
        Assertions.assertNotNull(location);
        Assertions.assertEquals(fileId, location.getFileId());
        Assertions.assertEquals(rgId, location.getRgId());
        Assertions.assertEquals(rgRowOffset, location.getRgRowOffset());
    }

    private void assertLocationMissing(long rowId)
    {
        Assertions.assertThrows(MainIndexException.class, () -> mainIndex.getLocation(rowId));
    }

    private void assertFlushFailsAndBufferSurvives(long fileId, long firstRowId, long secondRowId) throws Exception
    {
        Assertions.assertThrows(MainIndexException.class, () -> mainIndex.flushCache(fileId));
        Assertions.assertEquals(0, countRangesForFile(fileId));
        assertLocation(firstRowId, fileId, 0, 0);
        assertLocation(secondRowId, fileId, 0, 1);
    }

    private void assertRange(RowIdRange range, long rowIdStart, long rowIdEnd, long fileId,
                             int rgId, int rgRowOffsetStart, int rgRowOffsetEnd)
    {
        Assertions.assertEquals(rowIdStart, range.getRowIdStart());
        Assertions.assertEquals(rowIdEnd, range.getRowIdEnd());
        Assertions.assertEquals(fileId, range.getFileId());
        Assertions.assertEquals(rgId, range.getRgId());
        Assertions.assertEquals(rgRowOffsetStart, range.getRgRowOffsetStart());
        Assertions.assertEquals(rgRowOffsetEnd, range.getRgRowOffsetEnd());
    }

    private void assertNoInvalidRanges(long fileId) throws Exception
    {
        Assertions.assertEquals(0, countInvalidRangesForFile(fileId));
    }

    private Connection openMainIndexConnection() throws Exception
    {
        String path = sqlitePath.endsWith("/") ? sqlitePath : sqlitePath + "/";
        return DriverManager.getConnection("jdbc:sqlite:" + path + tableId + ".main.index.db");
    }

    private void insertRange(RowIdRange range) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement("INSERT INTO row_id_ranges VALUES(?, ?, ?, ?, ?, ?)"))
        {
            pst.setLong(1, range.getRowIdStart());
            pst.setLong(2, range.getRowIdEnd());
            pst.setLong(3, range.getFileId());
            pst.setInt(4, range.getRgId());
            pst.setInt(5, range.getRgRowOffsetStart());
            pst.setInt(6, range.getRgRowOffsetEnd());
            pst.executeUpdate();
        }
    }

    private void deleteExactRange(long rowIdStart, long rowIdEnd) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "DELETE FROM row_id_ranges WHERE row_id_start = ? AND row_id_end = ?"))
        {
            pst.setLong(1, rowIdStart);
            pst.setLong(2, rowIdEnd);
            pst.executeUpdate();
        }
    }

    private void insertFlushMarker(long fileId, long entryCount, List<RowIdRange> ranges) throws Exception
    {
        insertFlushMarker(fileId, entryCount, ranges.size(), buildRangeHash(ranges));
    }

    private void insertFlushMarker(long fileId, long entryCount, long rangeCount, byte[] rangeHash) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "INSERT INTO row_id_range_flush_markers VALUES(?, ?, ?, ?, ?)"))
        {
            pst.setLong(1, fileId);
            pst.setLong(2, entryCount);
            pst.setLong(3, rangeCount);
            pst.setBytes(4, rangeHash);
            pst.setLong(5, System.currentTimeMillis());
            pst.executeUpdate();
        }
    }

    private void deleteFlushMarker(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "DELETE FROM row_id_range_flush_markers WHERE file_id = ?"))
        {
            pst.setLong(1, fileId);
            pst.executeUpdate();
        }
    }

    private void createFailingFlushMarkerTrigger(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             Statement statement = connection.createStatement())
        {
            statement.executeUpdate("DROP TRIGGER IF EXISTS fail_marker_insert");
            statement.executeUpdate("CREATE TRIGGER fail_marker_insert BEFORE INSERT ON row_id_range_flush_markers " +
                    "WHEN NEW.file_id = " + fileId + " BEGIN SELECT RAISE(ABORT, 'forced marker failure'); END");
        }
    }

    private void dropFailingFlushMarkerTrigger() throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             Statement statement = connection.createStatement())
        {
            statement.executeUpdate("DROP TRIGGER IF EXISTS fail_marker_insert");
        }
    }

    private void createFailingRangeInsertTrigger(long rowIdStart) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             Statement statement = connection.createStatement())
        {
            statement.executeUpdate("DROP TRIGGER IF EXISTS fail_range_insert");
            statement.executeUpdate("CREATE TRIGGER fail_range_insert BEFORE INSERT ON row_id_ranges " +
                    "WHEN NEW.row_id_start = " + rowIdStart + " " +
                    "BEGIN SELECT RAISE(ABORT, 'forced range insert failure'); END");
        }
    }

    private void dropFailingRangeInsertTrigger() throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             Statement statement = connection.createStatement())
        {
            statement.executeUpdate("DROP TRIGGER IF EXISTS fail_range_insert");
        }
    }

    private List<RowIdRange> listRangesForFile(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "SELECT * FROM row_id_ranges WHERE file_id = ? ORDER BY row_id_start"))
        {
            pst.setLong(1, fileId);
            List<RowIdRange> ranges = new ArrayList<>();
            try (ResultSet rs = pst.executeQuery())
            {
                while (rs.next())
                {
                    ranges.add(new RowIdRange(
                            rs.getLong("row_id_start"),
                            rs.getLong("row_id_end"),
                            rs.getLong("file_id"),
                            rs.getInt("rg_id"),
                            rs.getInt("rg_row_offset_start"),
                            rs.getInt("rg_row_offset_end")));
                }
            }
            return ranges;
        }
    }

    private int countRangesForFile(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement("SELECT COUNT(*) FROM row_id_ranges WHERE file_id = ?"))
        {
            pst.setLong(1, fileId);
            try (ResultSet rs = pst.executeQuery())
            {
                Assertions.assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private int countFlushMarkersForFile(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "SELECT COUNT(*) FROM row_id_range_flush_markers WHERE file_id = ?"))
        {
            pst.setLong(1, fileId);
            try (ResultSet rs = pst.executeQuery())
            {
                Assertions.assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private int countExactRanges(long rowIdStart, long rowIdEnd) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "SELECT COUNT(*) FROM row_id_ranges WHERE row_id_start = ? AND row_id_end = ?"))
        {
            pst.setLong(1, rowIdStart);
            pst.setLong(2, rowIdEnd);
            try (ResultSet rs = pst.executeQuery())
            {
                Assertions.assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private int countInvalidRangesForFile(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "SELECT COUNT(*) FROM row_id_ranges WHERE file_id = ? AND " +
                             "(row_id_end <= row_id_start OR " +
                             "(row_id_end - row_id_start) != (rg_row_offset_end - rg_row_offset_start))"))
        {
            pst.setLong(1, fileId);
            try (ResultSet rs = pst.executeQuery())
            {
                Assertions.assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private byte[] buildRangeHash(List<RowIdRange> ranges) throws Exception
    {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (RowIdRange range : ranges)
        {
            updateLong(digest, range.getRowIdStart());
            updateLong(digest, range.getRowIdEnd());
            updateLong(digest, range.getFileId());
            updateInt(digest, range.getRgId());
            updateInt(digest, range.getRgRowOffsetStart());
            updateInt(digest, range.getRgRowOffsetEnd());
        }
        return digest.digest();
    }

    private static void updateLong(MessageDigest digest, long value)
    {
        for (int shift = 56; shift >= 0; shift -= 8)
        {
            digest.update((byte) (value >>> shift));
        }
    }

    private static void updateInt(MessageDigest digest, int value)
    {
        for (int shift = 24; shift >= 0; shift -= 8)
        {
            digest.update((byte) (value >>> shift));
        }
    }

    private long nanosToMillis(long nanos)
    {
        return nanos / 1_000_000L;
    }
}
