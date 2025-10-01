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

import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexBuffer;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.lock.PersistentAutoIncrement;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 * @update 2025-07-19 use sqlite as the persistent storage.
 */
public class SqliteMainIndex implements MainIndex
{
    private static final Logger logger = LogManager.getLogger(SqliteMainIndex.class);
    /**
     * Issue-1085: Use ConcurrentHashMap to avoid
     * <code>ConcurrentModificationException</code> when allocating row IDs under high load.
     */
    private static final Map<Long, PersistentAutoIncrement> persistentAIMap = new ConcurrentHashMap<>();

    /**
     * The SQL statement to create the row id range table.
     */
    private static final String createTableSql = "CREATE TABLE IF NOT EXISTS row_id_ranges" +
            "(row_id_start BIGINT NOT NULL, row_id_end BIGINT NOT NULL, file_id BIGINT NOT NULL, rg_id INT NOT NULL," +
            "rg_row_offset_start INT NOT NULL, rg_row_offset_end INT NOT NULL, PRIMARY KEY (row_id_start, row_id_end))";

    /**
     * The SQL statement to query the row id range that covers the given row id (the two ? are of the same value).
     */
    private static final String queryRangeSql = "SELECT * FROM row_id_ranges WHERE row_id_start <= ? AND ? < row_id_end";

    /**
     * The SQL statement to delete the row id ranges covered by the given row id range.
     */
    private static final String deleteRangesSql = "DELETE FROM row_id_ranges WHERE ? <= row_id_start AND row_id_end <= ?";

    /**
     * The SQL statement to update the width of the give row id range
     */
    private static final String updateRangeWidthSql = "UPDATE row_id_ranges SET row_id_start = ?, row_id_end = ?, " +
            "rg_row_offset_start = ?, rg_row_offset_end = ? WHERE row_id_start = ? AND row_id_end = ?";

    /**
     * The SQL statement to insert a new row id range
     */
    private static final String insertRangeSql = "INSERT INTO row_id_ranges VALUES(?, ?, ?, ?, ?, ?)";

    private final long tableId;
    private final String sqlitePath;
    private final MainIndexBuffer indexBuffer = new MainIndexBuffer();
    private final Connection connection;
    private final ReentrantReadWriteLock cacheRwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock dbRwLock = new ReentrantReadWriteLock();
    private boolean closed = false;
    private boolean removed = false;

    public SqliteMainIndex(long tableId, String sqlitePath) throws MainIndexException
    {
        this.tableId = tableId;
        if (sqlitePath == null || sqlitePath.isEmpty())
        {
            throw new MainIndexException("invalid sqlite path");
        }
        if (!sqlitePath.endsWith("/"))
        {
            sqlitePath += "/";
        }
        this.sqlitePath = sqlitePath;
        try
        {
            // We create a dependent sqlite instance for the main index of each table.
            connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath + tableId + ".main.index.db");
            try (Statement statement = connection.createStatement())
            {
                statement.execute(createTableSql);
            }
        }
        catch (SQLException e)
        {
            throw new MainIndexException("failed to connect to sqlite and open/create table", e);
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
                    /* Issue #986:
                     * We use numRowIds * 10L as the step for etcd auto-increment generation, reducing the etcd-overhead
                     * to less than 10%.
                     */
                    return new PersistentAutoIncrement("rowid-" + tblId, numRowIds * 10L); // key in etcd
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
        this.cacheRwLock.readLock().lock();
        /*
         * Issue #916:
         * cache-read lock and db-read lock are not acquired together before reading cache.
         * Thus putEntry() does not block db-read in this method. It is fine that deleteRowIdRange(rowIdRange)
         * happen between cache-read and db-read of this method.
         */
        IndexProto.RowLocation location = this.indexBuffer.lookup(rowId);
        this.cacheRwLock.readLock().unlock();
        if (location == null)
        {
            /*
             * Issue #916:
             * There is a gap between cache-read unlocking and db-read locking, and flushCache(fileId) and close()
             * may happen in this gap. This is OK, because if the cache is hit, this method will not read db;
             * otherwise, if cache misses, cache-flushing in flushCache(fileId) and close() has the same effect
             * as flushing the cache before read it.
             */
            try
            {
                this.dbRwLock.readLock().lock();
                RowIdRange rowIdRange = getRowIdRangeFromSqlite(rowId);
                if (rowIdRange != null)
                {
                    long rowIdStart = rowIdRange.getRowIdStart();
                    long fileId = rowIdRange.getFileId();
                    int rgId = rowIdRange.getRgId();
                    int rgRowOffsetStart = rowIdRange.getRgRowOffsetStart();
                    int offset = (int) (rowId - rowIdStart);
                    location = IndexProto.RowLocation.newBuilder()
                            .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffsetStart + offset).build();
                }
                this.dbRwLock.readLock().unlock();
            }
            catch (RowIdException e)
            {
                this.dbRwLock.readLock().unlock();
                throw new MainIndexException("failed to query row location from sqlite", e);
            }
        }
        return location;
    }

    @Override
    public boolean putEntry(long rowId, IndexProto.RowLocation rowLocation)
    {
        this.cacheRwLock.writeLock().lock();
        boolean res = this.indexBuffer.insert(rowId, rowLocation);
        this.cacheRwLock.writeLock().unlock();
        return res;
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange rowIdRange) throws MainIndexException
    {
        this.dbRwLock.writeLock().lock();
        try (PreparedStatement pst = connection.prepareStatement(deleteRangesSql))
        {
            long rowIdStart = rowIdRange.getRowIdStart();
            long rowIdEnd = rowIdRange.getRowIdEnd();
            pst.setLong(1, rowIdStart);
            pst.setLong(2, rowIdEnd);
            int n = pst.executeUpdate();
            logger.debug("deleted {} rows from sqlite", n);
            RowIdRange leftBorderRange = getRowIdRangeFromSqlite(rowIdStart);
            RowIdRange rightBorderRange = getRowIdRangeFromSqlite(rowIdEnd - 1);
            boolean res = true;
            if (leftBorderRange != null)
            {
                int width = (int) (rowIdStart - leftBorderRange.getRowIdStart());
                RowIdRange newLeftBorderRange = leftBorderRange.toBuilder()
                        .setRowIdEnd(rowIdStart).setRgRowOffsetEnd(leftBorderRange.getRgRowOffsetStart() + width).build();
                res &= updateRowIdRangeWidth(leftBorderRange, newLeftBorderRange);
            }
            if (rightBorderRange != null)
            {
                int width = (int) (rightBorderRange.getRowIdEnd() - rowIdEnd);
                RowIdRange newRightBorderRange = rightBorderRange.toBuilder()
                        .setRowIdStart(rowIdEnd).setRgRowOffsetStart(rightBorderRange.getRgRowOffsetEnd() - width).build();
                res &= updateRowIdRangeWidth(rightBorderRange, newRightBorderRange);
            }
            this.dbRwLock.writeLock().unlock();
            return res;
        }
        catch (SQLException | RowIdException e)
        {
            this.dbRwLock.writeLock().unlock();
            throw new MainIndexException("failed to delete row id ranges from sqlite", e);
        }
    }

    /**
     * Get the row id range that contains the given row id from sqlite.
     * @param rowId the given row id
     * @return the row id range, or null if no matching row id range is found
     * @throws RowIdException if failed to query a valid row id range from sqlite
     */
    private RowIdRange getRowIdRangeFromSqlite (long rowId) throws RowIdException
    {
        try (PreparedStatement pst = this.connection.prepareStatement(queryRangeSql))
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
                        throw new RowIdException("the width of row id range (" + rowIdStart + ", " +
                                rgRowOffsetEnd + ") does not match the width of row group row offset range (" +
                                rgRowOffsetStart + ", " + rgRowOffsetEnd + ")");
                    }
                    return new RowIdRange(rowIdStart, rowIdEnd, fileId, rgId, rgRowOffsetStart, rgRowOffsetEnd);
                }
                else
                {
                    return null;
                }
            }
        }
        catch (SQLException e)
        {
            throw new RowIdException("failed to query row id range from sqlite", e);
        }
    }

    /**
     * Update the width of an existing row id range
     * @param oldRange the old row id range
     * @param newRange the new row id range
     * @return true if any row id range is updated successfully
     * @throws RowIdException if failed to update the row id range in sqlite
     */
    private boolean updateRowIdRangeWidth(RowIdRange oldRange, RowIdRange newRange) throws RowIdException
    {
        try (PreparedStatement pst = this.connection.prepareStatement(updateRangeWidthSql))
        {
            pst.setLong(1, newRange.getRowIdStart());
            pst.setLong(2, newRange.getRowIdEnd());
            pst.setInt(3, newRange.getRgRowOffsetStart());
            pst.setInt(4, newRange.getRgRowOffsetEnd());
            pst.setLong(5, oldRange.getRowIdStart());
            pst.setLong(6, oldRange.getRowIdEnd());
            return pst.executeUpdate() > 0;
        }
        catch (SQLException e)
        {
            throw new RowIdException("failed to update row id range width", e);
        }
    }

    @Override
    public boolean flushCache(long fileId) throws MainIndexException
    {
        this.cacheRwLock.writeLock().lock();
        this.dbRwLock.writeLock().lock();
        try
        {
            List<RowIdRange> rowIdRanges = this.indexBuffer.flush(fileId);
            try (PreparedStatement pst = this.connection.prepareStatement(insertRangeSql))
            {
                for (RowIdRange range : rowIdRanges)
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
                this.cacheRwLock.writeLock().unlock();
                this.dbRwLock.writeLock().unlock();
                return true;
            }
        }
        catch (MainIndexException | SQLException e)
        {
            this.cacheRwLock.writeLock().unlock();
            this.dbRwLock.writeLock().unlock();
            throw new MainIndexException("failed to flush index cache into sqlite", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (!closed)
        {
            this.closed = true;
            this.cacheRwLock.writeLock().lock();
            this.dbRwLock.writeLock().lock();
            List<Long> cachedFileIds = this.indexBuffer.cachedFileIds();
            for (long fileId : cachedFileIds)
            {
                try
                {
                    // the locks are reentrant, no deadlocking problem
                    this.flushCache(fileId);
                } catch (MainIndexException e)
                {
                    this.cacheRwLock.writeLock().unlock();
                    this.dbRwLock.writeLock().unlock();
                    throw new IOException("failed to flush main index cache of file id " + fileId, e);
                }
            }
            this.indexBuffer.close();
            try
            {
                this.connection.close();
            } catch (SQLException e)
            {
                this.cacheRwLock.writeLock().unlock();
                this.dbRwLock.writeLock().unlock();
                throw new IOException("failed to close sqlite connection", e);
            }
            this.cacheRwLock.writeLock().unlock();
            this.dbRwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean closeAndRemove() throws MainIndexException
    {
        try
        {
            this.close();
        } catch (IOException e)
        {
            throw new MainIndexException("failed to close main index", e);
        }

        if (!removed)
        {
            removed = true;
            // clear SQLite directory for main index
            try
            {
                FileUtils.deleteDirectory(new File(sqlitePath));
            } catch (IOException e)
            {
                throw new MainIndexException("failed to clean up SQLite directory: " + e);
            }
        }
        return true;
    }
}