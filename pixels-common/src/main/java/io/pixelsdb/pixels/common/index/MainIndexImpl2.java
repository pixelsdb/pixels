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

import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.lock.PersistentAutoIncrement;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class MainIndexImpl2 implements MainIndex
{
    private static final Logger logger = LogManager.getLogger(MainIndexImpl2.class);
    private static final HashMap<Long, PersistentAutoIncrement> persistentAIMap = new HashMap<>();

    private static final String createTableSql = "CREATE TABLE IF NOT EXISTS row_id_ranges" +
            "(row_id_start BIGINT NOT NULL, row_id_end BIGINT NOT NULL, file_id BIGINT NOT NULL, rg_id INT NOT NULL," +
            "rg_row_offset_start INT NOT NULL, rg_row_offset_end INT NOT NULL, PRIMARY KEY (row_id_start, row_id_end))";
    private static final String queryRowIdRangeSql = "SELECT * FROM row_id_ranges WHERE row_id_start <= ? AND ? < row_id_end";

    private final long tableId;
    private final MainIndexBuffer indexBuffer = new MainIndexBuffer();
    private final Connection connection;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public MainIndexImpl2(long tableId) throws MainIndexException
    {
        this.tableId = tableId;
        String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
        if (sqlitePath == null || sqlitePath.isEmpty())
        {
            throw new MainIndexException("index.sqlite.path is not set");
        }
        if (!sqlitePath.endsWith("/"))
        {
            sqlitePath += "/";
        }
        try
        {
            // We create a dependent sqlite instance for the main index of each table.
            connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath + tableId + ".main.index.db");
            try (Statement statement = connection.createStatement())
            {
                boolean res = statement.execute(createTableSql);
                if (!res)
                {
                    throw new MainIndexException("Failed to create table row_id_ranges");
                }
            }
        } catch (SQLException e)
        {
            throw new MainIndexException("failed to connect to sqlite", e);
        }
    }

    @Override
    public long getTableId()
    {
        return tableId;
    }

    @Override
    public boolean hasCache()
    {
        return true;
    }

    @Override
    public IndexProto.RowIdBatch allocateRowIdBatch(long tableId, int numRowIds) throws RowIdException
    {
        try
        {
            // 1. get or create the persistent auto increment
            PersistentAutoIncrement autoIncrement = persistentAIMap.computeIfAbsent(tableId, tblId -> {
                try
                {
                    return new PersistentAutoIncrement("rowid-" + tblId); // key in etcd
                }
                catch (EtcdException e)
                {
                    logger.error(e);
                    throw new RuntimeException(e); // wrap to unchecked, will rethrow
                }
            });
            // 2. allocate numRowIds
            long start = autoIncrement.getAndIncrement(numRowIds);
            return IndexProto.RowIdBatch.newBuilder().setRowIdStart(start).setLength(numRowIds).build();
        }
        catch (RuntimeException | EtcdException e)
        {
            throw new RowIdException(e);
        }
    }

    @Override
    public IndexProto.RowLocation getLocation(long rowId) throws MainIndexException
    {
        this.rwLock.readLock().lock();
        IndexProto.RowLocation location = this.indexBuffer.lookup(rowId);
        if (location == null)
        {
            try (PreparedStatement pst = connection.prepareStatement(queryRowIdRangeSql))
            {
                pst.setLong(1, rowId);
                pst.setLong(2, rowId);
                try (ResultSet rs = pst.executeQuery())
                {
                    if (rs.next())
                    {
                        long rowIdStart = rs.getLong("row_id_start");
                        long rowIdEnd = rs.getLong("row_id_end");
                        long fileId = rs.getLong("file_id");
                        int rgId = rs.getInt("rg_id");
                        int rgRowOffsetStart = rs.getInt("rg_row_offset_start");
                        int rgRowOffsetEnd = rs.getInt("rg_row_offset_end");
                        if (rowIdEnd - rowIdStart != rgRowOffsetEnd - rgRowOffsetStart)
                        {
                            throw new MainIndexException("The width of row id range (" + rowIdStart + ", " +
                                    rgRowOffsetEnd + ") does not match the width of row group row offset range (" +
                                    rgRowOffsetStart + ", " + rgRowOffsetEnd + ")");
                        }
                        int offset = (int) (rowId - rowIdStart);
                        location = IndexProto.RowLocation.newBuilder()
                                .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffsetStart + offset).build();
                    }
                    this.rwLock.readLock().unlock();
                }
            }
            catch (SQLException e)
            {
                this.rwLock.readLock().unlock();
                throw new MainIndexException("failed to query row location from sqlite", e);
            }
        }
        return location;
    }

    @Override
    public boolean putEntry(long rowId, IndexProto.RowLocation rowLocation)
    {
        this.rwLock.writeLock().lock();
        boolean res = this.indexBuffer.insert(rowId, rowLocation);
        this.rwLock.writeLock().unlock();
        return res;
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange rowIdRange)
    {
        this.rwLock.writeLock().lock();
        this.rwLock.writeLock().unlock();
    }

    @Override
    public boolean flushCache()
    {
        this.rwLock.writeLock().lock();
        this.rwLock.writeLock().unlock();
    }

    @Override
    public void close() throws IOException
    {
        this.rwLock.writeLock().lock();
        this.rwLock.writeLock().unlock();
    }
}