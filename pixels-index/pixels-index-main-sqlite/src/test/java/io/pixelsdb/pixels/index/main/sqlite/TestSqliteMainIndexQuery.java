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
import io.pixelsdb.pixels.common.exception.RowIdException;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
public class TestSqliteMainIndexQuery
{
    MainIndex mainIndex;
    Long tableId =2876L;
    Connection connection;
    @BeforeEach
    public void setUp() throws Exception
    {
        String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
        if (!sqlitePath.endsWith("/"))
        {
            sqlitePath += "/";
        }
        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
        connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath + tableId + ".main.index.db");
    }

    @Test
    public void testQueryRowRanges() throws Exception
    {
        String query = "SELECT * FROM row_id_ranges order by row_id_start";
        long fileid = 0;
        try (PreparedStatement pst = this.connection.prepareStatement(query))
        {
//            pst.setLong(1, fileid);
            try (ResultSet rs = pst.executeQuery())
            {
                while (rs.next())
                {
                    long rowIdStart = rs.getLong("row_id_start");
                    long rowIdEnd = rs.getLong("row_id_end");
                    long fileId = rs.getLong("file_id");
                    int rgId = rs.getInt("rg_id");
                    int rgRowOffsetStart = rs.getInt("rg_row_offset_start");
                    int rgRowOffsetEnd = rs.getInt("rg_row_offset_end");
                    if (rowIdEnd - rowIdStart != rgRowOffsetEnd - rgRowOffsetStart)
                    {
                        throw new RowIdException("The width of row id range (" + rowIdStart + ", " +
                                rgRowOffsetEnd + ") does not match the width of row group row offset range (" +
                                rgRowOffsetStart + ", " + rgRowOffsetEnd + ")");
                    }
                    System.out.println(
                            "rowIdStart=" + rowIdStart +
                                    ", rowIdEnd=" + rowIdEnd +
                                    ", fileId=" + fileId +
                                    ", rgId=" + rgId +
                                    ", rgRowOffsetStart=" + rgRowOffsetStart +
                                    ", rgRowOffsetEnd=" + rgRowOffsetEnd
                    );
                }
            }
        }

    }
}
