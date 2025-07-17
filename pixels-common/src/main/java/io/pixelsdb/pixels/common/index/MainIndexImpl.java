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
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.lock.PersistentAutoIncrement;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class MainIndexImpl implements MainIndex
{
    private static final Logger logger = LogManager.getLogger(MainIndexImpl.class);
    private static final HashMap<Long, PersistentAutoIncrement> persistentAIMap = new HashMap<>();

    private final long tableId;



    // Cache for storing generated rowIds
    private final Queue<Long> rowIdCache = new ConcurrentLinkedQueue<>();
    // Get the singleton instance of EtcdUtil
    private final EtcdUtil etcdUtil = EtcdUtil.Instance();
    // Read-Write lock
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // Dirty flag
    private boolean dirty = false;

    public MainIndexImpl(long tableId)
    {
        this.tableId = tableId;
    }

    public static class Entry
    {
        private final RowIdRange rowIdRange;
        private final RgLocation rgLocation;

        public Entry(RowIdRange rowIdRange, RgLocation rgLocation)
        {
            this.rowIdRange = rowIdRange;
            this.rgLocation = rgLocation;
        }

        public RowIdRange getRowIdRange()
        {
            return rowIdRange;
        }

        public RgLocation getRgLocation()
        {
            return rgLocation;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

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

    }

    public IndexProto.RowLocation getLocationOld(long rowId)
    {
        // Use binary search to find the Entry containing the rowId
        int index = binarySearch(rowId);
        if (index >= 0)
        {
            Entry entry = entries.get(index);
            RgLocation rgLocation = entry.getRgLocation();
            return IndexProto.RowLocation.newBuilder()
                    .setFileId(rgLocation.getFileId())
                    .setRgId(rgLocation.getRowGroupId())
                    .setRgRowId((int) (rowId - entry.getRowIdRange().getStartRowId())) // Calculate the offset within the row group
                    .build();
        }
        return null; // Return null if not found
    }

    @Override
    public boolean putEntry(long rowId, IndexProto.RowLocation rowLocation)
    {
        RowIdRange newRange = new RowIdRange(rowId, rowId);
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

        long start = original.getStartRowId();
        long end = original.getEndRowId();
        // lock
        rwLock.writeLock().lock();
        entries.remove(index);
        // In-place insert the remaining entries
        if (rowId > start)
        {
            entries.add(index, new Entry(new RowIdRange(start, rowId - 1), entry.getRgLocation()));
            index++;
        }
        if (rowId < end)
        {
            entries.add(index, new Entry(new RowIdRange(rowId + 1, end), entry.getRgLocation()));
        }
        rwLock.writeLock().unlock();
        dirty = true;
        return true;
    }

    @Override
    public boolean putRowIds(RowIdRange rowIdRangeOfRg, RgLocation rgLocation)
    {
        long start = rowIdRangeOfRg.getStartRowId();
        long end = rowIdRangeOfRg.getEndRowId();
        if (start > end)
        {
            logger.error("Invalid RowIdRange: startRowId {} > endRowId {}", start, end);
            return false;
        }

        // Check whether it conflicts with the last entry.
        if (!entries.isEmpty())
        {
            RowIdRange lastRange = entries.get(entries.size() - 1).getRowIdRange();
            if (start <= lastRange.getEndRowId())
            {
                logger.error("Insert failure: RowIdRange [{}-{}] overlaps with previous [{}-{}]",
                        start, end, lastRange.getStartRowId(), lastRange.getEndRowId());
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
        int index = binarySearch(targetRange.getStartRowId());
        if (index < 0)
        {
            logger.error("Delete failure: RowIdRange [{}-{}] not found", targetRange.getStartRowId(), targetRange.getEndRowId());
            return false;
        }

        Entry entry = entries.get(index);
        RowIdRange existingRange = entry.getRowIdRange();

        if (existingRange.getStartRowId() == targetRange.getStartRowId() && existingRange.getEndRowId() == targetRange.getEndRowId())
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
                    targetRange.getStartRowId(), targetRange.getEndRowId(),
                    existingRange.getStartRowId(), existingRange.getEndRowId());
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
                String key = "/mainindex/" + entry.getRowIdRange().getStartRowId();
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

            if (rowId >= range.getStartRowId() && rowId <= range.getEndRowId())
            {
                return mid; // Found the containing Entry
            }
            else if (rowId < range.getStartRowId())
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
                entry.getRowIdRange().getStartRowId(),
                entry.getRowIdRange().getEndRowId(),
                entry.getRgLocation().getFileId(),
                entry.getRgLocation().getRowGroupId());
    }
}