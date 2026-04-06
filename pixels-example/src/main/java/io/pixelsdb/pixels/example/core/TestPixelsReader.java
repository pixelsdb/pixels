/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Tests for hidden-timestamp-column expose/filter logic in PixelsRecordReaderImpl.
 *
 * Covers:
 *   T1  empty projection + expose only (no filter)
 *   T2  empty projection + filter + expose
 *   T3  with projection + filter + expose
 *   T4  with projection + expose only (no filter)
 *   T5  multi-RG stride correctness (small batch across RG boundaries)
 *   T6  no hidden column + valid transTimestamp (must not crash)
 *   T7  empty projection + filter + no expose (count(*) with timestamp filter)
 *   T8  expose + no-hidden-column file (must throw IOException)
 *   T9  filter + no expose (hiddenColumnVector must be null)
 *   T10 VectorizedRowBatch.applyFilter syncs hiddenColumnVector
 */
public class TestPixelsReader
{
    private static final String TEST_DIR = "/tmp/pixels_test_reader/";
    private static final String FILE_WITH_HIDDEN = TEST_DIR + "with_hidden.pxl";
    private static final String FILE_NO_HIDDEN = TEST_DIR + "no_hidden.pxl";
    private static final String SCHEMA_STR = "struct<x:int,y:int>";

    private static final int ROWS_PER_BATCH = 10;
    private static final int NUM_BATCHES = 3;
    private static final int TOTAL_ROWS = ROWS_PER_BATCH * NUM_BATCHES; // 30

    private static final long FILTER_TIMESTAMP = 150;
    // timestamps are 10,20,...,300 → rows 0-14 have ts <= 150 → 15 rows pass
    private static final int EXPECTED_FILTERED_ROWS = 15;

    private static Storage storage;

    public static void main(String[] args) throws Exception
    {
        storage = StorageFactory.Instance().getStorage("file");
        setup();
        try
        {
            writeFileWithHiddenColumn();
            writeFileWithoutHiddenColumn();

            testT1_EmptyProjection_ExposeOnly();
            testT2_EmptyProjection_FilterAndExpose();
            testT3_WithProjection_FilterAndExpose();
            testT4_WithProjection_ExposeOnly();
            testT5_MultiRG_StrideCorrectness();
            testT6_NoHiddenColumn_ValidTimestamp();
            testT7_EmptyProjection_FilterNoExpose();
            testT8_ExposeNoHiddenColumn_ThrowsIOException();
            testT9_FilterNoExpose_HiddenVectorNull();
            testT10_ApplyFilter_SyncsHiddenColumn();

            System.out.println("\n=== All 10 tests passed! ===");
        } finally
        {
            cleanup();
        }
    }

    // ======================= helpers =======================

    private static void setup() throws IOException
    {
        Files.createDirectories(Paths.get(TEST_DIR));
        Files.deleteIfExists(Paths.get(FILE_WITH_HIDDEN));
        Files.deleteIfExists(Paths.get(FILE_NO_HIDDEN));
    }

    private static void cleanup() throws IOException
    {
        Files.deleteIfExists(Paths.get(FILE_WITH_HIDDEN));
        Files.deleteIfExists(Paths.get(FILE_NO_HIDDEN));
    }

    private static void check(boolean condition, String message)
    {
        if (!condition)
        {
            throw new AssertionError(message);
        }
    }

    private static long expectedTimestamp(int globalRowIdx)
    {
        return (globalRowIdx + 1) * 10L;
    }

    // ======================= write test files =======================

    private static void writeFileWithHiddenColumn() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString(SCHEMA_STR);
        PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setHasHiddenColumn(true)
                .setPixelStride(10)
                .setRowGroupSize(1) // minimal → forces every batch into its own row group
                .setStorage(storage)
                .setPath(FILE_WITH_HIDDEN)
                .setBlockSize(256 * 1024)
                .setReplication((short) 1)
                .setBlockPadding(false)
                .setEncodingLevel(EncodingLevel.EL2)
                .setCompressionBlockSize(1)
                .setNullsPadding(false)
                .build();

        for (int batch = 0; batch < NUM_BATCHES; batch++)
        {
            VectorizedRowBatch rowBatch = schema.createRowBatchWithHiddenColumn();
            LongColumnVector x = (LongColumnVector) rowBatch.cols[0];
            LongColumnVector y = (LongColumnVector) rowBatch.cols[1];
            LongColumnVector hidden = (LongColumnVector) rowBatch.cols[2];

            for (int i = 0; i < ROWS_PER_BATCH; i++)
            {
                int g = batch * ROWS_PER_BATCH + i;
                int row = rowBatch.size++;
                x.vector[row] = g * 100L;
                x.isNull[row] = false;
                y.vector[row] = g * 200L;
                y.isNull[row] = false;
                hidden.vector[row] = expectedTimestamp(g);
                hidden.isNull[row] = false;
            }
            writer.addRowBatch(rowBatch);
        }
        writer.close();
        System.out.println("Written file WITH hidden column: " + FILE_WITH_HIDDEN);
    }

    private static void writeFileWithoutHiddenColumn() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString(SCHEMA_STR);
        PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setHasHiddenColumn(false)
                .setPixelStride(10000)
                .setRowGroupSize(64 * 1024 * 1024)
                .setStorage(storage)
                .setPath(FILE_NO_HIDDEN)
                .setBlockSize(256 * 1024)
                .setReplication((short) 1)
                .setBlockPadding(false)
                .setEncodingLevel(EncodingLevel.EL2)
                .setCompressionBlockSize(1)
                .setNullsPadding(false)
                .build();

        VectorizedRowBatch rowBatch = schema.createRowBatch(ROWS_PER_BATCH);
        LongColumnVector x = (LongColumnVector) rowBatch.cols[0];
        LongColumnVector y = (LongColumnVector) rowBatch.cols[1];
        for (int i = 0; i < ROWS_PER_BATCH; i++)
        {
            int row = rowBatch.size++;
            x.vector[row] = i * 100L;
            x.isNull[row] = false;
            y.vector[row] = i * 200L;
            y.isNull[row] = false;
        }
        writer.addRowBatch(rowBatch);
        writer.close();
        System.out.println("Written file WITHOUT hidden column: " + FILE_NO_HIDDEN);
    }

    // ======================= tests =======================

    /**
     * T1: empty projection + expose only (no filter).
     * Enters else-branch, reads hidden column directly into hiddenColumnVector.
     * All 30 rows returned with correct timestamps.
     */
    private static void testT1_EmptyProjection_ExposeOnly() throws Exception
    {
        System.out.println("\n--- T1: Empty projection + expose only (no filter) ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[0]);
        option.exposeHiddenColumn(true);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);

        check(batch.cols.length == 0, "T1: expected 0 user columns, got " + batch.cols.length);
        check(batch.size == TOTAL_ROWS, "T1: expected " + TOTAL_ROWS + " rows, got " + batch.size);
        LongColumnVector hv = batch.getHiddenColumnVector();
        check(hv != null, "T1: hiddenColumnVector should not be null");
        for (int i = 0; i < TOTAL_ROWS; i++)
        {
            check(hv.vector[i] == expectedTimestamp(i),
                    "T1: row " + i + " ts expected " + expectedTimestamp(i) + ", got " + hv.vector[i]);
        }
        rr.close();
        reader.close();
        System.out.println("T1 passed!");
    }

    /**
     * T2: empty projection + filter + expose.
     * Enters if-branch, filters by timestamp, copies selected timestamps.
     */
    private static void testT2_EmptyProjection_FilterAndExpose() throws Exception
    {
        System.out.println("\n--- T2: Empty projection + filter + expose ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[0]);
        option.exposeHiddenColumn(true);
        option.transId(0);
        option.transTimestamp(FILTER_TIMESTAMP);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);

        check(batch.size == EXPECTED_FILTERED_ROWS,
                "T2: expected " + EXPECTED_FILTERED_ROWS + " rows, got " + batch.size);
        check(batch.cols.length == 0, "T2: expected 0 user columns, got " + batch.cols.length);
        LongColumnVector hv = batch.getHiddenColumnVector();
        check(hv != null, "T2: hiddenColumnVector should not be null");
        for (int i = 0; i < batch.size; i++)
        {
            check(hv.vector[i] == expectedTimestamp(i),
                    "T2: row " + i + " ts expected " + expectedTimestamp(i) + ", got " + hv.vector[i]);
            check(hv.vector[i] <= FILTER_TIMESTAMP,
                    "T2: row " + i + " ts " + hv.vector[i] + " should be <= " + FILTER_TIMESTAMP);
        }
        rr.close();
        reader.close();
        System.out.println("T2 passed!");
    }

    /**
     * T3: with projection + filter + expose.
     * Verifies user columns and hidden column are row-aligned after filtering.
     */
    private static void testT3_WithProjection_FilterAndExpose() throws Exception
    {
        System.out.println("\n--- T3: With projection + filter + expose ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x", "y"});
        option.exposeHiddenColumn(true);
        option.transId(0);
        option.transTimestamp(FILTER_TIMESTAMP);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);

        check(batch.size == EXPECTED_FILTERED_ROWS,
                "T3: expected " + EXPECTED_FILTERED_ROWS + " rows, got " + batch.size);
        check(batch.cols.length == 2, "T3: expected 2 user columns, got " + batch.cols.length);

        LongColumnVector xCol = (LongColumnVector) batch.cols[0];
        LongColumnVector yCol = (LongColumnVector) batch.cols[1];
        LongColumnVector hv = batch.getHiddenColumnVector();
        check(hv != null, "T3: hiddenColumnVector should not be null");

        for (int i = 0; i < batch.size; i++)
        {
            check(xCol.vector[i] == i * 100L,
                    "T3: row " + i + " x expected " + (i * 100L) + ", got " + xCol.vector[i]);
            check(yCol.vector[i] == i * 200L,
                    "T3: row " + i + " y expected " + (i * 200L) + ", got " + yCol.vector[i]);
            check(hv.vector[i] == expectedTimestamp(i),
                    "T3: row " + i + " ts expected " + expectedTimestamp(i) + ", got " + hv.vector[i]);
        }
        rr.close();
        reader.close();
        System.out.println("T3 passed!");
    }

    /**
     * T4: with projection + expose only (no filter).
     * Enters else-branch, reads all rows with user columns and hidden column.
     */
    private static void testT4_WithProjection_ExposeOnly() throws Exception
    {
        System.out.println("\n--- T4: With projection + expose only (no filter) ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x"});
        option.exposeHiddenColumn(true);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);

        check(batch.size == TOTAL_ROWS, "T4: expected " + TOTAL_ROWS + " rows, got " + batch.size);
        check(batch.cols.length == 1, "T4: expected 1 user column, got " + batch.cols.length);

        LongColumnVector xCol = (LongColumnVector) batch.cols[0];
        LongColumnVector hv = batch.getHiddenColumnVector();
        check(hv != null, "T4: hiddenColumnVector should not be null");

        for (int i = 0; i < batch.size; i++)
        {
            check(xCol.vector[i] == i * 100L,
                    "T4: row " + i + " x expected " + (i * 100L) + ", got " + xCol.vector[i]);
            check(hv.vector[i] == expectedTimestamp(i),
                    "T4: row " + i + " ts expected " + expectedTimestamp(i) + ", got " + hv.vector[i]);
        }
        rr.close();
        reader.close();
        System.out.println("T4 passed!");
    }

    /**
     * T5: multi-RG stride correctness.
     * Uses a small batch size (7) so that readBatch must cross RG boundaries.
     * Reads with filter (if-branch) to exercise the fixed stride computation.
     * Verifies every row's data is correct across all RGs.
     */
    private static void testT5_MultiRG_StrideCorrectness() throws Exception
    {
        System.out.println("\n--- T5: Multi-RG stride correctness ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        int rgNum = reader.getRowGroupNum();
        System.out.println("  Row groups in file: " + rgNum);
        check(rgNum >= 2, "T5: expected >= 2 row groups, got " + rgNum);

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x", "y"});
        option.exposeHiddenColumn(true);
        option.transId(0);
        option.transTimestamp(Long.MAX_VALUE); // accept all rows, but still enters if-branch
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        int totalRead = 0;
        while (true)
        {
            VectorizedRowBatch batch = rr.readBatch(7); // odd size to cross RG boundaries
            if (batch.size == 0 && batch.endOfFile)
            {
                break;
            }

            LongColumnVector xCol = (LongColumnVector) batch.cols[0];
            LongColumnVector hv = batch.getHiddenColumnVector();
            check(hv != null, "T5: hiddenColumnVector should not be null in batch starting at row " + totalRead);

            for (int i = 0; i < batch.size; i++)
            {
                int g = totalRead + i;
                check(xCol.vector[i] == g * 100L,
                        "T5: globalRow " + g + " x expected " + (g * 100L) + ", got " + xCol.vector[i]);
                check(hv.vector[i] == expectedTimestamp(g),
                        "T5: globalRow " + g + " ts expected " + expectedTimestamp(g) + ", got " + hv.vector[i]);
            }
            totalRead += batch.size;
            if (batch.endOfFile)
            {
                break;
            }
        }
        check(totalRead == TOTAL_ROWS, "T5: expected " + TOTAL_ROWS + " total rows, got " + totalRead);
        rr.close();
        reader.close();
        System.out.println("T5 passed! (read " + totalRead + " rows across multiple RGs)");
    }

    /**
     * T6: no hidden column + valid transTimestamp.
     * After the fix, this enters the else-branch (no filtering needed).
     * Must not crash, data must be correct.
     */
    private static void testT6_NoHiddenColumn_ValidTimestamp() throws Exception
    {
        System.out.println("\n--- T6: No hidden column + valid transTimestamp ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_NO_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x", "y"});
        option.transId(0);
        option.transTimestamp(100);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(100);

        check(batch.size == ROWS_PER_BATCH,
                "T6: expected " + ROWS_PER_BATCH + " rows, got " + batch.size);
        check(batch.getHiddenColumnVector() == null,
                "T6: hiddenColumnVector should be null (exposeHiddenColumn not set)");

        LongColumnVector xCol = (LongColumnVector) batch.cols[0];
        for (int i = 0; i < batch.size; i++)
        {
            check(xCol.vector[i] == i * 100L,
                    "T6: row " + i + " x expected " + (i * 100L) + ", got " + xCol.vector[i]);
        }
        rr.close();
        reader.close();
        System.out.println("T6 passed!");
    }

    /**
     * T7: empty projection + filter + no expose.
     * Equivalent to count(*) with timestamp filter.
     * filterByHiddenTimestamp=true, needReadHiddenColumn=true, exposeHiddenColumn=false.
     * Enters if-branch, filters rows, but hiddenColumnVector stays null.
     */
    private static void testT7_EmptyProjection_FilterNoExpose() throws Exception
    {
        System.out.println("\n--- T7: Empty projection + filter + no expose (filtered count(*)) ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[0]);
        option.transId(0);
        option.transTimestamp(FILTER_TIMESTAMP);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);

        check(batch.cols.length == 0, "T7: expected 0 user columns, got " + batch.cols.length);
        check(batch.size == EXPECTED_FILTERED_ROWS,
                "T7: expected " + EXPECTED_FILTERED_ROWS + " rows, got " + batch.size);
        check(batch.getHiddenColumnVector() == null,
                "T7: hiddenColumnVector should be null when exposeHiddenColumn is false");
        rr.close();
        reader.close();
        System.out.println("T7 passed!");
    }

    /**
     * T8: exposeHiddenColumn=true on a file without hidden column.
     * Constructor must throw IOException.
     */
    private static void testT8_ExposeNoHiddenColumn_ThrowsIOException() throws Exception
    {
        System.out.println("\n--- T8: Expose + no hidden column file → IOException ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_NO_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x"});
        option.exposeHiddenColumn(true);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        try
        {
            reader.read(option);
            check(false, "T8: should have thrown IOException");
        } catch (IOException e)
        {
            check(e.getMessage().contains("no hidden column"),
                    "T8: exception should mention 'no hidden column', got: " + e.getMessage());
            System.out.println("  Caught expected IOException: " + e.getMessage());
        }
        reader.close();
        System.out.println("T8 passed!");
    }

    /**
     * T9: filter + no expose (original behavior regression).
     * hiddenColumnVector must be null in the result batch.
     */
    private static void testT9_FilterNoExpose_HiddenVectorNull() throws Exception
    {
        System.out.println("\n--- T9: Filter + no expose → hiddenColumnVector null ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x", "y"});
        option.transId(0);
        option.transTimestamp(FILTER_TIMESTAMP);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        // exposeHiddenColumn defaults to false

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);

        check(batch.size == EXPECTED_FILTERED_ROWS,
                "T9: expected " + EXPECTED_FILTERED_ROWS + " rows, got " + batch.size);
        check(batch.getHiddenColumnVector() == null,
                "T9: hiddenColumnVector should be null when exposeHiddenColumn is false");

        LongColumnVector xCol = (LongColumnVector) batch.cols[0];
        for (int i = 0; i < batch.size; i++)
        {
            check(xCol.vector[i] == i * 100L,
                    "T9: row " + i + " x expected " + (i * 100L) + ", got " + xCol.vector[i]);
        }
        rr.close();
        reader.close();
        System.out.println("T9 passed!");
    }

    /**
     * T10: VectorizedRowBatch.applyFilter must sync hiddenColumnVector.
     * Read all rows with expose, then apply an external filter keeping even-indexed rows.
     */
    private static void testT10_ApplyFilter_SyncsHiddenColumn() throws Exception
    {
        System.out.println("\n--- T10: applyFilter syncs hiddenColumnVector ---");
        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(FILE_WITH_HIDDEN)
                .setPixelsFooterCache(new PixelsFooterCache()).build();

        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[]{"x"});
        option.exposeHiddenColumn(true);
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);

        PixelsRecordReader rr = reader.read(option);
        VectorizedRowBatch batch = rr.readBatch(TOTAL_ROWS + 100);
        check(batch.size == TOTAL_ROWS, "T10: expected " + TOTAL_ROWS + " rows before filter");

        Bitmap filter = new Bitmap(TOTAL_ROWS, false);
        for (int i = 0; i < TOTAL_ROWS; i += 2)
        {
            filter.set(i); // keep even-indexed rows: 0, 2, 4, ...
        }
        batch.applyFilter(filter);

        int expectedAfterFilter = (TOTAL_ROWS + 1) / 2; // ceil(30/2) = 15
        check(batch.size == expectedAfterFilter,
                "T10: expected " + expectedAfterFilter + " rows after filter, got " + batch.size);

        LongColumnVector xCol = (LongColumnVector) batch.cols[0];
        LongColumnVector hv = batch.getHiddenColumnVector();
        check(hv != null, "T10: hiddenColumnVector should survive applyFilter");

        for (int i = 0; i < batch.size; i++)
        {
            int originalIdx = i * 2;
            check(xCol.vector[i] == originalIdx * 100L,
                    "T10: filtered row " + i + " x expected " + (originalIdx * 100L) + ", got " + xCol.vector[i]);
            check(hv.vector[i] == expectedTimestamp(originalIdx),
                    "T10: filtered row " + i + " ts expected " + expectedTimestamp(originalIdx) + ", got " + hv.vector[i]);
        }
        rr.close();
        reader.close();
        System.out.println("T10 passed!");
    }
}
