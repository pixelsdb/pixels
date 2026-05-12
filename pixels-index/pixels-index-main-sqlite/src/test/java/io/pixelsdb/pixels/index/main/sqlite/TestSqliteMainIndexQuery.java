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

import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSqliteMainIndexQuery
{
    private static long nextTableId = 3035L;

    MainIndex mainIndex;
    long tableId;
    String sqlitePath;
    Connection connection;

    @BeforeEach
    public void setUp() throws Exception
    {
        tableId = nextTableId++;
        sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
        try
        {
            FileUtils.forceMkdir(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to create SQLite test directory: " + e.getMessage());
        }

        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
        String path = sqlitePath.endsWith("/") ? sqlitePath : sqlitePath + "/";
        connection = DriverManager.getConnection("jdbc:sqlite:" + path + tableId + ".main.index.db");
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (connection != null)
        {
            connection.close();
        }
        MainIndexFactory.Instance().closeIndex(tableId, true);
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
    public void testQueryRowRangesFromCommittedFlush() throws Exception
    {
        putMainIndexEntry(11000L, 51L, 0, 0);
        putMainIndexEntry(11001L, 51L, 0, 1);
        putMainIndexEntry(11010L, 51L, 1, 0);
        Assertions.assertTrue(mainIndex.flushCache(51L));

        List<RowIdRange> rowIdRanges = queryRowRanges();
        Assertions.assertEquals(2, rowIdRanges.size());
        assertRange(rowIdRanges.get(0), 11000L, 11002L, 51L, 0, 0, 2);
        assertRange(rowIdRanges.get(1), 11010L, 11011L, 51L, 1, 0, 1);
    }

    @Test
    public void testQueryRowRangesFromOutOfOrderBatchFlushesMultipleFiles() throws Exception
    {
        assertAllTrue(mainIndex.putEntries(Arrays.asList(
                primaryEntry(11102L, 52L, 0, 2),
                primaryEntry(11201L, 53L, 0, 1),
                primaryEntry(11100L, 52L, 0, 0),
                primaryEntry(11200L, 53L, 0, 0),
                primaryEntry(11101L, 52L, 0, 1),
                primaryEntry(11202L, 53L, 0, 2))));

        Assertions.assertTrue(mainIndex.flushCache(53L));
        Assertions.assertTrue(mainIndex.flushCache(52L));

        List<RowIdRange> rowIdRanges = queryRowRanges();
        Assertions.assertEquals(2, rowIdRanges.size());
        assertRange(rowIdRanges.get(0), 11100L, 11103L, 52L, 0, 0, 3);
        assertRange(rowIdRanges.get(1), 11200L, 11203L, 53L, 0, 0, 3);
    }

    @Test
    public void testQueryRowRangesReflectDeleteSplitForRecoveryCleanup() throws Exception
    {
        putContiguousEntries(11300L, 11306L, 54L, 0, 0);
        Assertions.assertTrue(mainIndex.flushCache(54L));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(11302L, 11305L, 54L, 0, 2, 5)));

        List<RowIdRange> rowIdRanges = queryRowRanges();
        Assertions.assertEquals(2, rowIdRanges.size());
        assertRange(rowIdRanges.get(0), 11300L, 11302L, 54L, 0, 0, 2);
        assertRange(rowIdRanges.get(1), 11305L, 11306L, 54L, 0, 5, 6);
    }

    private void putMainIndexEntry(long rowId, long fileId, int rgId, int rgRowOffset)
    {
        Assertions.assertTrue(mainIndex.putEntry(rowId, IndexProto.RowLocation.newBuilder()
                .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffset).build()));
    }

    private void putContiguousEntries(long rowIdStart, long rowIdEnd, long fileId, int rgId, int rgRowOffsetStart)
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

    private List<RowIdRange> queryRowRanges() throws Exception
    {
        String query = "SELECT * FROM row_id_ranges ORDER BY row_id_start";
        List<RowIdRange> ranges = new ArrayList<>();
        try (PreparedStatement pst = this.connection.prepareStatement(query);
             ResultSet rs = pst.executeQuery())
        {
            while (rs.next())
            {
                long rowIdStart = rs.getLong("row_id_start");
                long rowIdEnd = rs.getLong("row_id_end");
                int rgRowOffsetStart = rs.getInt("rg_row_offset_start");
                int rgRowOffsetEnd = rs.getInt("rg_row_offset_end");
                Assertions.assertEquals(rowIdEnd - rowIdStart, rgRowOffsetEnd - rgRowOffsetStart);

                ranges.add(new RowIdRange(
                        rowIdStart,
                        rowIdEnd,
                        rs.getLong("file_id"),
                        rs.getInt("rg_id"),
                        rgRowOffsetStart,
                        rgRowOffsetEnd));
            }
        }
        return ranges;
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
}
