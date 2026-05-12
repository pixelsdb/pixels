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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexBuffer;
import io.pixelsdb.pixels.common.index.MainIndexCache;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.lock.PersistentAutoIncrement;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Arrays;
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
    
    /*
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
     * The SQL statement to create the per-file flush marker table.
     */
    private static final String createFlushMarkerTableSql = "CREATE TABLE IF NOT EXISTS row_id_range_flush_markers " +
            "(file_id BIGINT NOT NULL PRIMARY KEY, entry_count BIGINT NOT NULL, range_count BIGINT NOT NULL, " +
            "range_hash BLOB NOT NULL, committed_at_ms BIGINT NOT NULL)";

    /**
     * The SQL statement to query the row id range that covers the given row id (the two ? are of the same value).
     */
    private static final String queryRangeSql = "SELECT * FROM row_id_ranges WHERE row_id_start <= ? AND ? < row_id_end";

    /**
     * The SQL statement to delete the row id ranges covered by the given row id range.
     */
    private static final String deleteRangesSql = "DELETE FROM row_id_ranges WHERE ? <= row_id_start AND row_id_end <= ?";

    /**
     * The SQL statement to update the width of the give row id range.
     */
    private static final String updateRangeWidthSql = "UPDATE row_id_ranges SET row_id_start = ?, row_id_end = ?, " +
            "rg_row_offset_start = ?, rg_row_offset_end = ? WHERE row_id_start = ? AND row_id_end = ?";

    /**
     * The SQL statement to insert a new row id range.
     */
    private static final String insertRangeSql = "INSERT INTO row_id_ranges VALUES(?, ?, ?, ?, ?, ?)";

    /**
     * The SQL statement to query a per-file flush marker.
     */
    private static final String queryFlushMarkerSql =
            "SELECT entry_count, range_count, range_hash FROM row_id_range_flush_markers WHERE file_id = ?";

    /**
     * The SQL statement to insert a per-file flush marker.
     */
    private static final String insertFlushMarkerSql =
            "INSERT INTO row_id_range_flush_markers VALUES(?, ?, ?, ?, ?)";

    private static final class FlushMarker
    {
        private final long fileId;
        private final long entryCount;
        private final long rangeCount;
        private final byte[] rangeHash;

        private FlushMarker(long fileId, long entryCount, long rangeCount, byte[] rangeHash)
        {
            this.fileId = fileId;
            this.entryCount = entryCount;
            this.rangeCount = rangeCount;
            this.rangeHash = rangeHash;
        }

        private boolean matches(MainIndexBuffer.FlushSnapshot snapshot, byte[] snapshotHash)
        {
            return this.fileId == snapshot.getFileId()
                    && this.entryCount == snapshot.getEntryCount()
                    && this.rangeCount == snapshot.getRowIdRanges().size()
                    && Arrays.equals(this.rangeHash, snapshotHash);
        }
    }

    private final long tableId;
    private final String sqlitePath;
    private final MainIndexBuffer indexBuffer;
    private final MainIndexCache indexCache;
    private final Connection connection;
    private final ReentrantReadWriteLock cacheRwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock dbRwLock = new ReentrantReadWriteLock();
    private boolean closed = false;
    private boolean removed = false;

    public SqliteMainIndex(long tableId, String sqlitePath) throws MainIndexException
    {
        this.tableId = tableId;
        this.indexCache = new MainIndexCache();
        this.indexBuffer = new MainIndexBuffer(this.indexCache);
        if (sqlitePath == null || sqlitePath.isEmpty())
        {
            throw new MainIndexException("Invalid sqlite path");
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
                statement.execute(createFlushMarkerTableSql);
            }
        }
        catch (SQLException e)
        {
            throw new MainIndexException("Failed to connect to sqlite and open/create table", e);
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
                    /*
                     * Issue #986:
                     * We use numRowIds * 10L as the step for etcd auto-increment generation, reducing the etcd-overhead
                     * to less than 10%.
                     */
                    long step = Math.max(Constants.AI_DEFAULT_STEP, numRowIds * 10L);
                    return new PersistentAutoIncrement(Constants.AI_ROW_ID_PREFIX + tblId, step, false);
                }
                catch (EtcdException e)
                {
                    logger.error("Failed to create persistent auto-increment for table " + tblId, e);
                    throw new RuntimeException("Failed to create persistent auto-increment for table " + tblId, e); // wrap to unchecked, will rethrow
                }
            });
            // 2. allocate numRowIds
            /*
             * Issue #1099: auto increment starts from 1, whereas row id starts from 0,
             * so we use auto increment minus 1 as the row id start.
             */
            long start = autoIncrement.getAndIncrement(numRowIds) - 1;
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
        IndexProto.RowLocation location;
        try
        {
            /*
             * Issue #916:
             * cache-read lock and db-read lock are not acquired together before reading cache.
             * Thus putEntry() does not block db-read in this method. It is fine that deleteRowIdRange(rowIdRange)
             * happen between cache-read and db-read of this method.
             */
            location = this.indexBuffer.lookup(rowId);
        }
        finally
        {
            this.cacheRwLock.readLock().unlock();
        }
        if (location == null)
        {
            location = getRowLocationFromSqlite(rowId);
            if (location == null)
            {
                throw new MainIndexException("Failed to get row location for rowId=" + rowId
                        + " (tableId=" + tableId + ")");
            }
        }
        return location;
    }

    @Override
    public List<IndexProto.RowLocation> getLocations(List<Long> rowIds) throws MainIndexException
    {
        ImmutableList.Builder<IndexProto.RowLocation> builder = ImmutableList.builder();
        this.cacheRwLock.readLock().lock();
        try
        {
            for (long rowId : rowIds)
            {
                IndexProto.RowLocation location;
                location = this.indexBuffer.lookup(rowId);
                if (location == null)
                {
                    location = getRowLocationFromSqlite(rowId);
                    if (location == null)
                    {
                        throw new MainIndexException("Failed to get row location for rowId=" + rowId
                                + " (tableId=" + tableId + ")");
                    }
                }
                builder.add(location);
            }
        }
        finally
        {
            this.cacheRwLock.readLock().unlock();
        }
        return builder.build();
    }

    private IndexProto.RowLocation getRowLocationFromSqlite(long rowId) throws MainIndexException
    {
        /*
         * Issue #916:
         * There is a gap between cache-read unlocking and db-read locking, and flushCache(fileId) and close()
         * may happen in this gap. This is OK, because if the cache is hit, this method will not read db;
         * otherwise, if cache misses, cache-flushing in flushCache(fileId) and close() has the same effect
         * as flushing the cache before read it.
         */
        this.dbRwLock.readLock().lock();
        IndexProto.RowLocation location = null;
        try
        {
            RowIdRange rowIdRange = getRowIdRangeFromSqlite(rowId);
            if (rowIdRange != null)
            {
                // Issue #1150: add the range to cache to accelerate main index lookups
                this.indexCache.admitRange(rowIdRange);

                long rowIdStart = rowIdRange.getRowIdStart();
                long fileId = rowIdRange.getFileId();
                int rgId = rowIdRange.getRgId();
                int rgRowOffsetStart = rowIdRange.getRgRowOffsetStart();
                int offset = (int) (rowId - rowIdStart);
                location = IndexProto.RowLocation.newBuilder()
                        .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffsetStart + offset).build();
            }
        }
        catch (RowIdException e)
        {
            throw new MainIndexException("Failed to query row location from sqlite", e);
        }
        finally
        {
            this.dbRwLock.readLock().unlock();
        }
        return location;
    }

    @Override
    public boolean putEntry(long rowId, IndexProto.RowLocation rowLocation)
    {
        this.cacheRwLock.writeLock().lock();
        boolean res;
        try
        {
            res = this.indexBuffer.insert(rowId, rowLocation);
        }
        finally
        {
            this.cacheRwLock.writeLock().unlock();
        }
        return res;
    }

    @Override
    public List<Boolean> putEntries(List<IndexProto.PrimaryIndexEntry> primaryEntries)
    {
        ImmutableList.Builder<Boolean> builder = ImmutableList.builder();
        this.cacheRwLock.writeLock().lock();
        try
        {
            for (IndexProto.PrimaryIndexEntry entry : primaryEntries)
            {
                boolean res = this.indexBuffer.insert(entry.getRowId(), entry.getRowLocation());
                builder.add(res);
            }
        }
        finally
        {
            this.cacheRwLock.writeLock().unlock();
        }
        return builder.build();
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange rowIdRange) throws MainIndexException
    {
        long rowIdStart = rowIdRange.getRowIdStart();
        long rowIdEnd = rowIdRange.getRowIdEnd();
        if (rowIdEnd <= rowIdStart)
        {
            throw new MainIndexException("Invalid row id range to delete: [" + rowIdStart + ", " + rowIdEnd + ")");
        }

        this.dbRwLock.writeLock().lock();
        try
        {
            boolean originalAutoCommit = this.connection.getAutoCommit();
            try
            {
                this.connection.setAutoCommit(false);
                RowIdRange leftBorderRange = getRowIdRangeFromSqlite(rowIdStart);
                RowIdRange rightBorderRange = getRowIdRangeFromSqlite(rowIdEnd - 1);
                boolean res = true;
                try (PreparedStatement pst = connection.prepareStatement(deleteRangesSql))
                {
                    pst.setLong(1, rowIdStart);
                    pst.setLong(2, rowIdEnd);
                    pst.executeUpdate();
                }
                if (leftBorderRange != null && rightBorderRange != null &&
                        leftBorderRange.getRowIdStart() == rightBorderRange.getRowIdStart() &&
                        leftBorderRange.getRowIdEnd() == rightBorderRange.getRowIdEnd())
                {
                    res &= trimSingleOverlappingRange(leftBorderRange, rowIdStart, rowIdEnd);
                }
                else
                {
                    if (leftBorderRange != null && leftBorderRange.getRowIdStart() < rowIdStart &&
                            rowIdStart < leftBorderRange.getRowIdEnd())
                    {
                        int width = (int) (rowIdStart - leftBorderRange.getRowIdStart());
                        RowIdRange newLeftBorderRange = leftBorderRange.toBuilder()
                                .setRowIdEnd(rowIdStart)
                                .setRgRowOffsetEnd(leftBorderRange.getRgRowOffsetStart() + width).build();
                        res &= updateRowIdRangeWidth(leftBorderRange, newLeftBorderRange);
                    }
                    if (rightBorderRange != null && rightBorderRange.getRowIdStart() < rowIdEnd &&
                            rowIdEnd < rightBorderRange.getRowIdEnd())
                    {
                        int width = (int) (rightBorderRange.getRowIdEnd() - rowIdEnd);
                        RowIdRange newRightBorderRange = rightBorderRange.toBuilder()
                                .setRowIdStart(rowIdEnd)
                                .setRgRowOffsetStart(rightBorderRange.getRgRowOffsetEnd() - width).build();
                        res &= updateRowIdRangeWidth(rightBorderRange, newRightBorderRange);
                    }
                }
                this.connection.commit();
                return res;
            }
            catch (SQLException | RowIdException e)
            {
                rollbackQuietly(e);
                throw e;
            }
            finally
            {
                this.connection.setAutoCommit(originalAutoCommit);
            }
        }
        catch (SQLException | RowIdException e)
        {
            throw new MainIndexException("Failed to delete row id ranges from sqlite", e);
        }
        finally
        {
            // Issue #1150: evict the range from cache anyway
            this.indexCache.evictRange(rowIdRange);
            this.dbRwLock.writeLock().unlock();
        }
    }

    private boolean trimSingleOverlappingRange(RowIdRange range, long rowIdStart, long rowIdEnd)
            throws RowIdException, SQLException
    {
        if (range.getRowIdStart() < rowIdStart && rowIdEnd < range.getRowIdEnd())
        {
            int leftWidth = (int) (rowIdStart - range.getRowIdStart());
            RowIdRange newLeftRange = range.toBuilder()
                    .setRowIdEnd(rowIdStart)
                    .setRgRowOffsetEnd(range.getRgRowOffsetStart() + leftWidth).build();
            int rightWidth = (int) (range.getRowIdEnd() - rowIdEnd);
            RowIdRange newRightRange = range.toBuilder()
                    .setRowIdStart(rowIdEnd)
                    .setRgRowOffsetStart(range.getRgRowOffsetEnd() - rightWidth).build();
            boolean res = updateRowIdRangeWidth(range, newLeftRange);
            try (PreparedStatement pst = this.connection.prepareStatement(insertRangeSql))
            {
                bindRangeInsertStatement(pst, newRightRange);
                res &= pst.executeUpdate() > 0;
            }
            return res;
        }
        if (range.getRowIdStart() < rowIdStart && rowIdStart < range.getRowIdEnd())
        {
            int width = (int) (rowIdStart - range.getRowIdStart());
            RowIdRange newLeftRange = range.toBuilder()
                    .setRowIdEnd(rowIdStart)
                    .setRgRowOffsetEnd(range.getRgRowOffsetStart() + width).build();
            return updateRowIdRangeWidth(range, newLeftRange);
        }
        if (range.getRowIdStart() < rowIdEnd && rowIdEnd < range.getRowIdEnd())
        {
            int width = (int) (range.getRowIdEnd() - rowIdEnd);
            RowIdRange newRightRange = range.toBuilder()
                    .setRowIdStart(rowIdEnd)
                    .setRgRowOffsetStart(range.getRgRowOffsetEnd() - width).build();
            return updateRowIdRangeWidth(range, newRightRange);
        }
        return true;
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
                        throw new RowIdException("The width of row id range (" + rowIdStart + ", " +
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
            throw new RowIdException("Failed to query row id range from sqlite", e);
        }
    }

    private static void bindRangeInsertStatement(PreparedStatement pst, RowIdRange range) throws SQLException
    {
        pst.setLong(1, range.getRowIdStart());
        pst.setLong(2, range.getRowIdEnd());
        pst.setLong(3, range.getFileId());
        pst.setInt(4, range.getRgId());
        pst.setInt(5, range.getRgRowOffsetStart());
        pst.setInt(6, range.getRgRowOffsetEnd());
    }

    /**
     * Update the width of an existing row id range.
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
            throw new RowIdException("Failed to update row id range width", e);
        }
    }

    @Override
    public boolean flushCache(long fileId) throws MainIndexException
    {
        this.cacheRwLock.writeLock().lock();
        this.dbRwLock.writeLock().lock();
        try
        {
            MainIndexBuffer.FlushSnapshot snapshot = this.indexBuffer.snapshotForFlush(fileId);
            if (snapshot.isEmpty())
            {
                return true;
            }

            byte[] snapshotHash = buildRangeHash(snapshot.getRowIdRanges());
            FlushMarker marker = readFlushMarker(snapshot.getFileId());
            if (marker != null)
            {
                if (!marker.matches(snapshot, snapshotHash))
                {
                    throw new MainIndexException("Conflicting flush marker already exists for fileId=" + fileId);
                }
                this.indexBuffer.discardFlushed(snapshot);
                return true;
            }

            boolean originalAutoCommit = this.connection.getAutoCommit();
            try
            {
                this.connection.setAutoCommit(false);
                try (PreparedStatement pst = this.connection.prepareStatement(insertRangeSql))
                {
                    for (RowIdRange range : snapshot.getRowIdRanges())
                    {
                        bindRangeInsertStatement(pst, range);
                        pst.addBatch();
                    }
                    pst.executeBatch();
                }
                insertFlushMarker(snapshot, snapshotHash);
                this.connection.commit();
            }
            catch (SQLException e)
            {
                rollbackQuietly(e);
                throw e;
            }
            finally
            {
                this.connection.setAutoCommit(originalAutoCommit);
            }

            this.indexBuffer.discardFlushed(snapshot);
            return true;
        }
        catch (MainIndexException | SQLException e)
        {
            throw new MainIndexException("Failed to flush index cache into sqlite", e);
        }
        finally
        {
            this.cacheRwLock.writeLock().unlock();
            this.dbRwLock.writeLock().unlock();
        }
    }

    private FlushMarker readFlushMarker(long fileId) throws SQLException
    {
        try (PreparedStatement pst = this.connection.prepareStatement(queryFlushMarkerSql))
        {
            pst.setLong(1, fileId);
            try (ResultSet rs = pst.executeQuery())
            {
                if (!rs.next())
                {
                    return null;
                }
                return new FlushMarker(fileId, rs.getLong("entry_count"),
                        rs.getLong("range_count"), rs.getBytes("range_hash"));
            }
        }
    }

    private void insertFlushMarker(MainIndexBuffer.FlushSnapshot snapshot, byte[] rangeHash) throws SQLException
    {
        try (PreparedStatement pst = this.connection.prepareStatement(insertFlushMarkerSql))
        {
            pst.setLong(1, snapshot.getFileId());
            pst.setLong(2, snapshot.getEntryCount());
            pst.setLong(3, snapshot.getRowIdRanges().size());
            pst.setBytes(4, rangeHash);
            pst.setLong(5, System.currentTimeMillis());
            pst.executeUpdate();
        }
    }

    private byte[] buildRangeHash(List<RowIdRange> rowIdRanges) throws MainIndexException
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (RowIdRange range : rowIdRanges)
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
        catch (NoSuchAlgorithmException e)
        {
            throw new MainIndexException("Failed to build range hash for main index flush", e);
        }
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

    private void rollbackQuietly(Exception failure)
    {
        try
        {
            this.connection.rollback();
        }
        catch (SQLException rollbackException)
        {
            failure.addSuppressed(rollbackException);
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
            try
            {
                List<Long> cachedFileIds = this.indexBuffer.cachedFileIds();
                for (long fileId : cachedFileIds)
                {
                    try
                    {
                        // the locks are reentrant, no deadlocking problem
                        this.flushCache(fileId);
                    } catch (MainIndexException e)
                    {
                        throw new IOException("Failed to flush main index cache of file id " + fileId, e);
                    }
                }
                this.indexBuffer.close();
                try
                {
                    this.connection.close();
                } catch (SQLException e)
                {
                    throw new IOException("Failed to close sqlite connection", e);
                }
            }
            finally
            {
                this.cacheRwLock.writeLock().unlock();
                this.dbRwLock.writeLock().unlock();
            }

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
            throw new MainIndexException("Failed to close main index", e);
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
                throw new MainIndexException("Failed to clean up SQLite directory", e);
            }
        }
        return true;
    }
}
