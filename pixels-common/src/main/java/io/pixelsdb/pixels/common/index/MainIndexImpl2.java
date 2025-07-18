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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
            "rg_row_id_start INT NOT NULL, rg_row_id_end INT NOT NULL)";
    private static final String createIndexSql =
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_row_id_range ON row_id_ranges (row_id_start ASC, row_id_end ASC)";

    private final long tableId;
    private final MainIndexBuffer indexBuffer = new MainIndexBuffer();
    private final Connection connection;
    // Read-Write lock
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // Dirty flag
    private boolean dirty = false;

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
                res = statement.execute(createIndexSql);
                if (!res)
                {
                    throw new MainIndexException("Failed to create index on row_id_range");
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
            }
            );
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
    public IndexProto.RowLocation getLocation(long rowId)
    {
        IndexProto.RowLocation location = this.indexBuffer.lookup(rowId);
        if (location == null)
        {

        }
        return location;
    }

    @Override
    public boolean putEntry(long rowId, IndexProto.RowLocation rowLocation)
    {
        RowIdRange newRange = new RowIdRange(rowId, rowId, rowLocation.getFileId(),
                rowLocation.getRgId(), rowLocation.getRgRowId(), rowLocation.getRgRowId()+1);
        RgLocation rgLocation = new RgLocation(rowLocation.getFileId(), rowLocation.getRgId());
        return putRowIds(newRange, rgLocation);
    }

    @Override
    public boolean deleteEntry(long rowId)
    {
        int index = binarySearch(rowId);
        if (index < 0)
        {
            logger.error("Delete failure: RowId {} not found", rowId);
            return false;
        }

        Entry entry = entries.get(index);
        RowIdRange original = entry.getRowIdRange();

        long start = original.getRowIdStart();
        long end = original.getRowIdEnd();
        // lock
        rwLock.writeLock().lock();
        entries.remove(index);
        // In-place insert the remaining entries
        if (rowId > start)
        {
            entries.add(index, new Entry(new RowIdRange(start, rowId - 1, 1L, 0, 0, 0), entry.getRgLocation()));
            index++;
        }
        if (rowId < end)
        {
            entries.add(index, new Entry(new RowIdRange(rowId + 1, end, 1L, 0, 0, 0), entry.getRgLocation()));
        }
        rwLock.writeLock().unlock();
        dirty = true;
        return true;
    }

    @Override
    public boolean putRowIds(RowIdRange rowIdRangeOfRg, RgLocation rgLocation)
    {
        long start = rowIdRangeOfRg.getRowIdStart();
        long end = rowIdRangeOfRg.getRowIdEnd();
        if (start > end)
        {
            logger.error("Invalid RowIdRange: startRowId {} > endRowId {}", start, end);
            return false;
        }

        // Check whether it conflicts with the last entry.
        if (!entries.isEmpty())
        {
            RowIdRange lastRange = entries.get(entries.size() - 1).getRowIdRange();
            if (start <= lastRange.getRowIdEnd())
            {
                logger.error("Insert failure: RowIdRange [{}-{}] overlaps with previous [{}-{}]",
                        start, end, lastRange.getRowIdStart(), lastRange.getRowIdEnd());
                return false;
            }
        }
        rwLock.writeLock().lock();
        entries.add(new Entry(rowIdRangeOfRg, rgLocation));
        rwLock.writeLock().unlock();
        dirty = true;
        return true;
    }

    @Override
    public boolean deleteRowIds(RowIdRange targetRange)
    {
        int index = binarySearch(targetRange.getRowIdStart());
        if (index < 0)
        {
            logger.error("Delete failure: RowIdRange [{}-{}] not found", targetRange.getRowIdStart(), targetRange.getRowIdEnd());
            return false;
        }

        Entry entry = entries.get(index);
        RowIdRange existingRange = entry.getRowIdRange();

        if (existingRange.getRowIdStart() == targetRange.getRowIdStart() && existingRange.getRowIdEnd() == targetRange.getRowIdEnd())
        {
            rwLock.writeLock().lock();
            entries.remove(index);
            rwLock.writeLock().unlock();
            dirty = true;
            return true;
        }
        else
        {
            logger.error("Delete failure: RowIdRange [{}-{}] does not exactly match existing range [{}-{}]",
                    targetRange.getRowIdStart(), targetRange.getRowIdEnd(),
                    existingRange.getRowIdStart(), existingRange.getRowIdEnd());
            return false;
        }

    }

    @Override
    public boolean persist()
    {
        try
        {
            // Iterate through entries and persist each to etcd
            for (Entry entry : entries)
            {
                String key = "/mainindex/" + entry.getRowIdRange().getRowIdStart();
                String value = serializeEntry(entry); // Serialize Entry to string
                etcdUtil.putKeyValue(key, value);
            }
            logger.info("Persisted {} entries to etcd", entries.size());
            return true;
        }
        catch (Exception e)
        {
            logger.error("Failed to persist entries to etcd", e);
            return false;
        }
    }

    public boolean persistIfDirty()
    {
        if (dirty)
        {
            if (persist())
            {
                dirty = false; // Reset dirty flag
                return true;
            }
            return false;
        }
        return true; // No changes, no need to persist
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            // Check dirty flag and persist to etcd if true
            if (!persistIfDirty())
            {
                logger.error("Failed to persist data to etcd before closing");
                throw new IOException("Failed to persist data to etcd before closing");
            }
            logger.info("Data persisted to etcd successfully before closing");
        }
        catch (Exception e)
        {
            logger.error("Error occurred while closing MainIndexImpl", e);
            throw new IOException("Error occurred while closing MainIndexImpl", e);
        }
    }

    private int binarySearch(long rowId)
    {
        int low = 0;
        int high = entries.size() - 1;

        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            Entry entry = entries.get(mid);
            RowIdRange range = entry.getRowIdRange();

            if (rowId >= range.getRowIdStart() && rowId <= range.getRowIdEnd())
            {
                return mid; // Found the containing Entry
            }
            else if (rowId < range.getRowIdStart())
            {
                high = mid - 1;
            }
            else
            {
                low = mid + 1;
            }
        }

        return -1; // Not found
    }

    // Serialize Entry
    private String serializeEntry(Entry entry)
    {
        return String.format("{\"startRowId\": %d, \"endRowId\": %d, \"fieldId\": %d, \"rowGroupId\": %d}",
                entry.getRowIdRange().getRowIdStart(),
                entry.getRowIdRange().getRowIdEnd(),
                entry.getRgLocation().getFileId(),
                entry.getRgLocation().getRowGroupId());
    }
}