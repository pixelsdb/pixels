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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Tag("benchmark")
public class TestSqliteMainIndexBenchmark
{
    private static final String ENABLE_PROPERTY = "pixels.sqlite.main.index.benchmark";
    private static final long NOT_APPLICABLE = -1L;
    private static final int CONTIGUOUS_ROWS = Integer.getInteger(
            "pixels.sqlite.main.index.benchmark.contiguousRows", 1_000_000);
    private static final int FRAGMENTED_ROWS = Integer.getInteger(
            "pixels.sqlite.main.index.benchmark.fragmentedRows", 100_000);
    private static long nextTableId = 900_000L;

    private String sqlitePath;
    private long tableId;
    private MainIndex mainIndex;

    @BeforeEach
    public void setUp()
    {
        Assumptions.assumeTrue(Boolean.getBoolean(ENABLE_PROPERTY),
                "Set -D" + ENABLE_PROPERTY + "=true to run manual sqlite main-index benchmarks.");
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        closeAndRemoveIndex();
    }

    @Test
    public void benchmarkPutGetAndFlushPaths() throws Exception
    {
        System.out.println();
        printBenchmarkParameters();
        List<BenchmarkResult> results = new ArrayList<>();
        results.add(benchmarkHotPutGetPath());
        results.add(benchmarkContiguousFlush());
        results.add(benchmarkFragmentedFlush());
        results.add(benchmarkMarkerHitRetry());
        printBenchmarkSummary(results);
    }

    private BenchmarkResult benchmarkHotPutGetPath() throws Exception
    {
        openFreshIndex();
        long fileId = 1L;
        long rowIdBase = 1_000_000_000L;

        long putNs = elapsedNanos(() -> putContiguousEntries(CONTIGUOUS_ROWS, fileId, rowIdBase));
        long getNs = elapsedNanos(() -> getContiguousEntries(CONTIGUOUS_ROWS, rowIdBase));
        long cleanupFlushNs = elapsedNanos(() -> Assertions.assertTrue(mainIndex.flushCache(fileId)));
        long ranges = countRangesForFile(fileId);
        long markers = countFlushMarkersForFile(fileId);

        closeAndRemoveIndex();
        return new BenchmarkResult("hot put/get path", "contiguous, pre-flush get",
                CONTIGUOUS_ROWS, ranges, markers, putNs, cleanupFlushNs,
                NOT_APPLICABLE, NOT_APPLICABLE, getNs);
    }

    private BenchmarkResult benchmarkContiguousFlush() throws Exception
    {
        openFreshIndex();
        long fileId = 2L;
        long rowIdBase = 2_000_000_000L;

        long putNs = elapsedNanos(() -> putContiguousEntries(CONTIGUOUS_ROWS, fileId, rowIdBase));
        long flushNs = elapsedNanos(() -> Assertions.assertTrue(mainIndex.flushCache(fileId)));
        long getNs = elapsedNanos(() -> getContiguousEntries(CONTIGUOUS_ROWS, rowIdBase));
        long ranges = countRangesForFile(fileId);
        long markers = countFlushMarkersForFile(fileId);

        Assertions.assertEquals(1L, ranges);
        Assertions.assertEquals(1L, markers);
        closeAndRemoveIndex();
        return new BenchmarkResult("contiguous first flush", "contiguous rows -> 1 range",
                CONTIGUOUS_ROWS, ranges, markers, putNs, flushNs,
                NOT_APPLICABLE, NOT_APPLICABLE, getNs);
    }

    private BenchmarkResult benchmarkFragmentedFlush() throws Exception
    {
        openFreshIndex();
        long fileId = 3L;
        long rowIdBase = 3_000_000_000L;

        long putNs = elapsedNanos(() -> putFragmentedEntries(FRAGMENTED_ROWS, fileId, rowIdBase));
        long flushNs = elapsedNanos(() -> Assertions.assertTrue(mainIndex.flushCache(fileId)));
        long getNs = elapsedNanos(() -> getFragmentedEntries(FRAGMENTED_ROWS, rowIdBase));
        long ranges = countRangesForFile(fileId);
        long markers = countFlushMarkersForFile(fileId);

        Assertions.assertEquals(FRAGMENTED_ROWS, ranges);
        Assertions.assertEquals(1L, markers);
        closeAndRemoveIndex();
        return new BenchmarkResult("fragmented first flush", "1-row gaps -> many ranges",
                FRAGMENTED_ROWS, ranges, markers, putNs, flushNs,
                NOT_APPLICABLE, NOT_APPLICABLE, getNs);
    }

    private BenchmarkResult benchmarkMarkerHitRetry() throws Exception
    {
        openFreshIndex();
        long fileId = 4L;
        long rowIdBase = 4_000_000_000L;
        List<RowIdRange> ranges = buildFragmentedRanges(FRAGMENTED_ROWS, fileId, rowIdBase);

        insertRangesAndMarker(fileId, FRAGMENTED_ROWS, ranges);
        long putNs = elapsedNanos(() -> putFragmentedEntries(FRAGMENTED_ROWS, fileId, rowIdBase));
        long markerRetryNs = elapsedNanos(() -> Assertions.assertTrue(mainIndex.flushCache(fileId)));
        long emptyRetryNs = elapsedNanos(() -> Assertions.assertTrue(mainIndex.flushCache(fileId)));
        long getNs = elapsedNanos(() -> getFragmentedEntries(FRAGMENTED_ROWS, rowIdBase));
        long storedRanges = countRangesForFile(fileId);
        long markers = countFlushMarkersForFile(fileId);

        Assertions.assertEquals(FRAGMENTED_ROWS, storedRanges);
        Assertions.assertEquals(1L, markers);
        closeAndRemoveIndex();
        return new BenchmarkResult("marker-hit retry flush", "matching marker already durable",
                FRAGMENTED_ROWS, storedRanges, markers, putNs, NOT_APPLICABLE,
                markerRetryNs, emptyRetryNs, getNs);
    }

    private void openFreshIndex() throws Exception
    {
        closeAndRemoveIndex();
        this.tableId = nextTableId++;
        this.sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
        try
        {
            FileUtils.forceMkdir(new File(sqlitePath));
        }
        catch (IOException e)
        {
            throw new MainIndexException("Failed to create SQLite benchmark directory", e);
        }
        this.mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
    }

    private void closeAndRemoveIndex() throws Exception
    {
        if (this.mainIndex != null)
        {
            MainIndexFactory.Instance().closeIndex(this.tableId, true);
            this.mainIndex = null;
        }
        if (this.sqlitePath != null)
        {
            FileUtils.deleteDirectory(new File(sqlitePath));
        }
    }

    private void putContiguousEntries(int rowCount, long fileId, long rowIdBase)
    {
        IndexProto.RowLocation.Builder locationBuilder = IndexProto.RowLocation.newBuilder()
                .setFileId(fileId).setRgId(0);
        for (int i = 0; i < rowCount; i++)
        {
            Assertions.assertTrue(mainIndex.putEntry(rowIdBase + i, locationBuilder.setRgRowOffset(i).build()));
        }
    }

    private void putFragmentedEntries(int rowCount, long fileId, long rowIdBase)
    {
        IndexProto.RowLocation.Builder locationBuilder = IndexProto.RowLocation.newBuilder()
                .setFileId(fileId).setRgId(0);
        for (int i = 0; i < rowCount; i++)
        {
            Assertions.assertTrue(mainIndex.putEntry(rowIdBase + i * 2L, locationBuilder.setRgRowOffset(i).build()));
        }
    }

    private void getContiguousEntries(int rowCount, long rowIdBase) throws MainIndexException
    {
        for (int i = 0; i < rowCount; i++)
        {
            Assertions.assertNotNull(mainIndex.getLocation(rowIdBase + i));
        }
    }

    private void getFragmentedEntries(int rowCount, long rowIdBase) throws MainIndexException
    {
        for (int i = 0; i < rowCount; i++)
        {
            Assertions.assertNotNull(mainIndex.getLocation(rowIdBase + i * 2L));
        }
    }

    private List<RowIdRange> buildFragmentedRanges(int rowCount, long fileId, long rowIdBase)
    {
        List<RowIdRange> ranges = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            long rowId = rowIdBase + i * 2L;
            ranges.add(new RowIdRange(rowId, rowId + 1, fileId, 0, i, i + 1));
        }
        return ranges;
    }

    private void insertRangesAndMarker(long fileId, long entryCount, List<RowIdRange> ranges) throws Exception
    {
        try (Connection connection = openMainIndexConnection())
        {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try
            {
                try (PreparedStatement pst = connection.prepareStatement("INSERT INTO row_id_ranges VALUES(?, ?, ?, ?, ?, ?)"))
                {
                    for (RowIdRange range : ranges)
                    {
                        pst.setLong(1, range.getRowIdStart());
                        pst.setLong(2, range.getRowIdEnd());
                        pst.setLong(3, range.getFileId());
                        pst.setInt(4, range.getRgId());
                        pst.setInt(5, range.getRgRowOffsetStart());
                        pst.setInt(6, range.getRgRowOffsetEnd());
                        pst.addBatch();
                    }
                    pst.executeBatch();
                }
                try (PreparedStatement pst = connection.prepareStatement(
                        "INSERT INTO row_id_range_flush_markers VALUES(?, ?, ?, ?, ?)"))
                {
                    pst.setLong(1, fileId);
                    pst.setLong(2, entryCount);
                    pst.setLong(3, ranges.size());
                    pst.setBytes(4, buildRangeHash(ranges));
                    pst.setLong(5, System.currentTimeMillis());
                    pst.executeUpdate();
                }
                connection.commit();
            }
            catch (Exception e)
            {
                connection.rollback();
                throw e;
            }
            finally
            {
                connection.setAutoCommit(originalAutoCommit);
            }
        }
    }

    private Connection openMainIndexConnection() throws Exception
    {
        String path = sqlitePath.endsWith("/") ? sqlitePath : sqlitePath + "/";
        return DriverManager.getConnection("jdbc:sqlite:" + path + tableId + ".main.index.db");
    }

    private long countRangesForFile(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement("SELECT COUNT(*) FROM row_id_ranges WHERE file_id = ?"))
        {
            pst.setLong(1, fileId);
            try (ResultSet rs = pst.executeQuery())
            {
                Assertions.assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }

    private long countFlushMarkersForFile(long fileId) throws Exception
    {
        try (Connection connection = openMainIndexConnection();
             PreparedStatement pst = connection.prepareStatement(
                     "SELECT COUNT(*) FROM row_id_range_flush_markers WHERE file_id = ?"))
        {
            pst.setLong(1, fileId);
            try (ResultSet rs = pst.executeQuery())
            {
                Assertions.assertTrue(rs.next());
                return rs.getLong(1);
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

    private long elapsedNanos(ThrowingRunnable runnable) throws Exception
    {
        long start = System.nanoTime();
        runnable.run();
        return System.nanoTime() - start;
    }

    private void printBenchmarkParameters()
    {
        System.out.println("SQLite MainIndex benchmark parameters");
        System.out.println("  -D" + ENABLE_PROPERTY + "=" + Boolean.getBoolean(ENABLE_PROPERTY));
        System.out.println("  -Dpixels.sqlite.main.index.benchmark.contiguousRows=" + CONTIGUOUS_ROWS);
        System.out.println("  -Dpixels.sqlite.main.index.benchmark.fragmentedRows=" + FRAGMENTED_ROWS);
        System.out.println("  index.sqlite.path=" + ConfigFactory.Instance().getProperty("index.sqlite.path"));
        System.out.println("  java.version=" + System.getProperty("java.version"));
        System.out.println("  os.name=" + System.getProperty("os.name"));
        System.out.println("  os.arch=" + System.getProperty("os.arch"));
    }

    private void printBenchmarkSummary(List<BenchmarkResult> results)
    {
        System.out.println();
        System.out.println("SQLite MainIndex benchmark summary");
        System.out.println("rows = logical MainIndex entries; ranges = persisted row_id_ranges.");
        System.out.println("markerRetry = retry when a matching per-file durable marker already exists.");
        System.out.println("emptyRetry = immediate second flush after marker retry discarded the buffer.");
        System.out.println(String.format("%-27s %-31s %12s %10s %7s %10s %13s %10s %16s %15s %13s %10s %13s",
                "workload", "shape", "rows", "ranges", "markers", "put(ms)", "put rows/s",
                "flush(ms)", "flush ranges/s", "markerRetry(ms)", "emptyRetry(ms)", "get(ms)", "get rows/s"));
        for (BenchmarkResult result : results)
        {
            System.out.println(String.format("%-27s %-31s %12s %10s %7s %10s %13s %10s %16s %15s %13s %10s %13s",
                    result.name,
                    result.shape,
                    formatLong(result.rows),
                    formatLong(result.ranges),
                    formatLong(result.markers),
                    formatMillis(result.putNs),
                    formatRate(result.rows, result.putNs),
                    formatMillis(result.flushNs),
                    formatRate(result.ranges, result.flushNs),
                    formatMillis(result.markerRetryNs),
                    formatMillis(result.emptyRetryNs),
                    formatMillis(result.getNs),
                    formatRate(result.rows, result.getNs)));
        }
    }

    private String formatLong(long value)
    {
        return String.format(Locale.US, "%,d", value);
    }

    private String formatMillis(long nanos)
    {
        if (nanos < 0)
        {
            return "-";
        }
        return String.format(Locale.US, "%,.3f", nanos / 1_000_000.0D);
    }

    private String formatRate(long count, long nanos)
    {
        if (nanos <= 0)
        {
            return "-";
        }
        double rate = count * 1_000_000_000.0D / nanos;
        return String.format(Locale.US, "%,.0f", rate);
    }

    private static final class BenchmarkResult
    {
        private final String name;
        private final String shape;
        private final long rows;
        private final long ranges;
        private final long markers;
        private final long putNs;
        private final long flushNs;
        private final long markerRetryNs;
        private final long emptyRetryNs;
        private final long getNs;

        private BenchmarkResult(String name, String shape, long rows, long ranges, long markers,
                                long putNs, long flushNs, long markerRetryNs, long emptyRetryNs, long getNs)
        {
            this.name = name;
            this.shape = shape;
            this.rows = rows;
            this.ranges = ranges;
            this.markers = markers;
            this.putNs = putNs;
            this.flushNs = flushNs;
            this.markerRetryNs = markerRetryNs;
            this.emptyRetryNs = emptyRetryNs;
            this.getNs = getNs;
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }
}
