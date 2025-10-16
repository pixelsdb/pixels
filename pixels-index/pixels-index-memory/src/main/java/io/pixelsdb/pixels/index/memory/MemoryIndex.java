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
package io.pixelsdb.pixels.index.memory;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory implementation of SinglePointIndex that is thread-safe, lock-free,
 * and supports multi-version concurrency control (MVCC).
 *
 * @author hank
 * @create 2025-10-11
 */
public class MemoryIndex implements SinglePointIndex
{
    public static final Logger LOGGER = LogManager.getLogger(MemoryIndex.class);

    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean removed = new AtomicBoolean(false);

    /**
     * For unique indexes: baseKey -> (timestamp -> rowId).
     */
    private final ConcurrentHashMap<CompositeKey, ConcurrentSkipListMap<Long, Long>> uniqueIndex;
    /**
     * For non-unique indexes: baseKey -> (rowId -> timestamps).
     */
    private final ConcurrentHashMap<CompositeKey, ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>>> nonUniqueIndex;

    /**
     * Tombstones: baseKey -> tombstone timestamps.
     */
    private final ConcurrentHashMap<CompositeKey, ConcurrentSkipListSet<Long>> tombstones;

    public MemoryIndex(long tableId, long indexId, boolean unique)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.unique = unique;
        this.uniqueIndex = new ConcurrentHashMap<>();
        this.nonUniqueIndex = new ConcurrentHashMap<>();
        this.tombstones = new ConcurrentHashMap<>();
    }

    @Override
    public long getTableId()
    {
        return tableId;
    }

    @Override
    public long getIndexId()
    {
        return indexId;
    }

    @Override
    public boolean isUnique()
    {
        return unique;
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        checkClosed();
        if (!unique)
        {
            throw new SinglePointIndexException("getUniqueRowId can only be called on unique indexes");
        }
        CompositeKey baseKey = extractBaseKey(key);
        long snapshotTimestamp = key.getTimestamp();
        // Find the latest version visible at the snapshot timestamp
        return findUniqueRowId(baseKey, snapshotTimestamp);
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        checkClosed();
        CompositeKey baseKey = extractBaseKey(key);
        long snapshotTimestamp = key.getTimestamp();
        if (unique)
        {
            long rowId = findUniqueRowId(baseKey, snapshotTimestamp);
            return rowId >= 0 ? ImmutableList.of(rowId) : ImmutableList.of();
        }
        else
        {
            return findNonUniqueRowIds(baseKey, snapshotTimestamp);
        }
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        checkClosed();
        CompositeKey baseKey = extractBaseKey(key);
        long timestamp = key.getTimestamp();
        internalPutEntry(rowId, baseKey, timestamp);
        return true;
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        checkClosed();
        if (!unique)
        {
            throw new SinglePointIndexException("putPrimaryEntries can only be called on unique indexes");
        }
        for (IndexProto.PrimaryIndexEntry entry : entries)
        {
            CompositeKey baseKey = extractBaseKey(entry.getIndexKey());
            long timestamp = entry.getIndexKey().getTimestamp();
            ConcurrentSkipListMap<Long, Long> versions =
                    this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
            versions.put(timestamp, entry.getRowId());
        }
        return true;
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        checkClosed();

        for (IndexProto.SecondaryIndexEntry entry : entries)
        {
            IndexProto.IndexKey key = entry.getIndexKey();
            long rowId = entry.getRowId();
            CompositeKey baseKey = extractBaseKey(key);
            long timestamp = key.getTimestamp();
            internalPutEntry(rowId, baseKey, timestamp);
        }
        return true;
    }

    private void internalPutEntry(long rowId, CompositeKey baseKey, long timestamp)
    {
        if (unique)
        {
            // For unique index, store a single rowId per timestamp
            ConcurrentSkipListMap<Long, Long> versions =
                    this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
            versions.put(timestamp, rowId);
        }
        else
        {
            // For non-unique index, store a queue of timestamps per row id
            putNonUniqueNewVersion(rowId, baseKey, timestamp);
        }
    }

    @Override
    public long updatePrimaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        checkClosed();
        if (!unique)
        {
            throw new SinglePointIndexException("updatePrimaryEntry can only be called on unique indexes");
        }
        CompositeKey baseKey = extractBaseKey(key);
        long timestamp = key.getTimestamp();
        long prevRowId = getUniqueRowId(key);
        ConcurrentSkipListMap<Long, Long> versions =
                this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
        versions.put(timestamp, rowId);
        return prevRowId;
    }

    @Override
    public List<Long> updateSecondaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        checkClosed();
        CompositeKey baseKey = extractBaseKey(key);
        long timestamp = key.getTimestamp();
        if (unique)
        {
            long prevRowId = getUniqueRowId(key);
            ConcurrentSkipListMap<Long, Long> versions =
                    this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
            versions.put(timestamp, rowId);
            return prevRowId >= 0 ? ImmutableList.of(prevRowId) : ImmutableList.of();
        }
        else
        {
            List<Long> prevRowIds = getRowIds(key);
            putNonUniqueNewVersion(rowId, baseKey, timestamp);
            return prevRowIds;
        }
    }

    @Override
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        checkClosed();
        if (!unique)
        {
            throw new SinglePointIndexException("updatePrimaryEntries can only be called on unique indexes");
        }
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (IndexProto.PrimaryIndexEntry entry : entries)
        {
            CompositeKey baseKey = extractBaseKey(entry.getIndexKey());
            long timestamp = entry.getIndexKey().getTimestamp();
            long prevRowId = getUniqueRowId(entry.getIndexKey());
            builder.add(prevRowId);
            ConcurrentSkipListMap<Long, Long> versions =
                    this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
            versions.put(timestamp, entry.getRowId());
        }
        return builder.build();
    }

    @Override
    public List<Long> updateSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        checkClosed();
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (IndexProto.SecondaryIndexEntry entry : entries)
        {
            IndexProto.IndexKey key = entry.getIndexKey();
            long rowId = entry.getRowId();
            CompositeKey baseKey = extractBaseKey(key);
            long timestamp = key.getTimestamp();
            if (unique)
            {
                long prevRowId = getUniqueRowId(key);
                builder.add(prevRowId);
                ConcurrentSkipListMap<Long, Long> versions =
                        this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
                versions.put(timestamp, rowId);
            }
            else
            {
                List<Long> prevRowIds = getRowIds(key);
                builder.addAll(prevRowIds);
                putNonUniqueNewVersion(rowId, baseKey, timestamp);
            }
        }
        return builder.build();
    }

    private void putNonUniqueNewVersion(long rowId, CompositeKey baseKey, long timestamp)
    {
        ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> rowIds =
                this.nonUniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentHashMap<>());
        rowIds.compute(rowId, (id, existingVersions) -> {
            if (existingVersions == null)
            {
                ConcurrentSkipListSet<Long> newVersions = new ConcurrentSkipListSet<>();
                newVersions.add(timestamp);
                return newVersions;
            }
            else
            {
                existingVersions.add(timestamp);
                return existingVersions;
            }
        });
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        checkClosed();
        if (!unique)
        {
            throw new SinglePointIndexException("deleteUniqueEntry can only be called on unique indexes");
        }
        CompositeKey baseKey = extractBaseKey(key);
        long rowId = getUniqueRowId(key);
        if (rowId < 0)
        {
            return -1L;
        }
        // Add tombstone instead of removing the entry
        ConcurrentSkipListSet<Long> existingTombstones =
                this.tombstones.computeIfAbsent(baseKey, k -> new ConcurrentSkipListSet<>());
        existingTombstones.add(key.getTimestamp());
        return rowId;
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        checkClosed();
        try
        {
            if (unique)
            {
                long rowId = getUniqueRowId(key);
                if (rowId < 0)
                {
                    return ImmutableList.of();
                }
                return ImmutableList.of(rowId);
            } else
            {
                List<Long> rowIds = getRowIds(key);
                if (rowIds.isEmpty())
                {
                    return ImmutableList.of();
                }
                return rowIds;
            }
        }
        finally
        {
            CompositeKey baseKey = extractBaseKey(key);
            ConcurrentSkipListSet<Long> existingTombstones =
                    this.tombstones.computeIfAbsent(baseKey, k -> new ConcurrentSkipListSet<>());
            existingTombstones.add(key.getTimestamp());
        }
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        checkClosed();
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (IndexProto.IndexKey key : keys)
        {
            List<Long> prevRowIds = deleteEntry(key);
            if (prevRowIds.isEmpty())
            {
                return ImmutableList.of();
            }
            builder.addAll(prevRowIds);
        }
        return builder.build();
    }

    @Override
    public List<Long> purgeEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        checkClosed();
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (IndexProto.IndexKey key : indexKeys)
        {
            CompositeKey baseKey = extractBaseKey(key);
            ConcurrentSkipListSet<Long> existingTombstones = this.tombstones.get(baseKey);
            if (existingTombstones == null)
            {
                continue;
            }
            // Remove all versions with timestamp <= purgeTimestamp
            Long tombstone = existingTombstones.floor(key.getTimestamp());
            if (tombstone != null && tombstone <= key.getTimestamp())
            {
                if (unique)
                {
                    // timestamp -> row id
                    ConcurrentSkipListMap<Long, Long> versions = this.uniqueIndex.get(baseKey);
                    if (versions != null)
                    {
                        ConcurrentNavigableMap<Long, Long> purging = versions.headMap(tombstone, true);
                        if (!purging.isEmpty())
                        {
                            builder.addAll(purging.values());
                            purging.clear();
                        }
                        if (versions.isEmpty())
                        {
                            this.uniqueIndex.remove(baseKey);
                        }
                    }
                }
                else
                {
                    // row id -> timestamps
                    ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> rowIds = this.nonUniqueIndex.get(baseKey);
                    if (rowIds != null)
                    {
                        for (Iterator<Map.Entry<Long, ConcurrentSkipListSet<Long>>> it = rowIds.entrySet().iterator(); it.hasNext(); )
                        {
                            Map.Entry<Long, ConcurrentSkipListSet<Long>> entry = it.next();
                            NavigableSet<Long> versions = entry.getValue().headSet(tombstone, true);
                            if (!versions.isEmpty())
                            {
                                builder.add(entry.getKey());
                                versions.clear();
                            }
                            if (entry.getValue().isEmpty())
                            {
                                it.remove();
                            }
                        }
                        builder.addAll(rowIds.keySet());
                        if (rowIds.isEmpty())
                        {
                            this.nonUniqueIndex.remove(baseKey);
                        }
                    }
                }
                existingTombstones.headSet(key.getTimestamp(), true).clear();
                if (existingTombstones.isEmpty())
                {
                    this.tombstones.remove(baseKey);
                }
            }
        }
        return builder.build();
    }

    @Override
    @Deprecated
    public void close()
    {
        if (closed.compareAndSet(false, true))
        {
            LOGGER.debug("MemoryIndex closed for table {} index {}", tableId, indexId);
        }
    }

    @Override
    public boolean closeAndRemove()
    {
        if (closed.compareAndSet(false, true) && removed.compareAndSet(false, true))
        {
            this.uniqueIndex.clear();
            this.nonUniqueIndex.clear();
            this.tombstones.clear();
            LOGGER.debug("MemoryIndex closed and removed for table {} index {}", tableId, indexId);
            return true;
        }
        return false; // Already closed and/or removed
    }

    /**
     * Get the current size of the index (for testing and monitoring)
     */
    public int size()
    {
        if (unique)
        {
            return this.uniqueIndex.values().stream().mapToInt(ConcurrentSkipListMap::size).sum();
        }
        else
        {
            return this.nonUniqueIndex.values().stream().mapToInt(rowIdToVersions ->
                            rowIdToVersions.values().stream().mapToInt(ConcurrentSkipListSet::size).sum()).sum();
        }

    }

    /**
     * Get the number of tombstones (for testing and monitoring)
     */
    public int tombstonesSize()
    {
        return tombstones.size();
    }

    /**
     * Check if the index is closed and throw exception if it is
     */
    private void checkClosed() throws SinglePointIndexException
    {
        if (closed.get())
        {
            throw new SinglePointIndexException("MemoryIndex is closed for table " + tableId + " index " + indexId);
        }
    }

    /**
     * Extract base key (without timestamp) from IndexKey
     */
    private CompositeKey extractBaseKey(IndexProto.IndexKey key)
    {
        return new CompositeKey(key.getIndexId(), key.getKey());
    }

    /**
     * Find the latest visible version for a unique index.
     * @param baseKey the base key of the index entry
     * @param snapshotTimestamp the snapshot timestamp of the transaction
     * @return the latest visible row id, or -1 if no row id is visible
     */
    private long findUniqueRowId(CompositeKey baseKey, long snapshotTimestamp)
    {
        ConcurrentSkipListMap<Long, Long> versions = this.uniqueIndex.get(baseKey);
        if (versions == null)
        {
            return -1;
        }
        // Find the latest version with timestamp <= snapshot timestamp
        Map.Entry<Long, Long> versionEntry = versions.floorEntry(snapshotTimestamp);
        if (versionEntry == null)
        {
            return -1;
        }
        // Check if this version is visible (not deleted by a tombstone)
        if (!isVersionVisible(baseKey, versionEntry.getKey(), snapshotTimestamp))
        {
            return -1;
        }
        return versionEntry.getValue();
    }

    /**
     * Find the latest visible version for a non-unique index.
     * @param baseKey the base key of the index entry
     * @param snapshotTimestamp the snapshot timestamp of the transaction
     * @return the latest visible row ids, or empty if no row id is visible
     */
    private List<Long> findNonUniqueRowIds(CompositeKey baseKey, long snapshotTimestamp)
    {
        ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> rowIds = this.nonUniqueIndex.get(baseKey);
        if (rowIds == null)
        {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (Map.Entry<Long, ConcurrentSkipListSet<Long>> entry : rowIds.entrySet())
        {
            Long version = entry.getValue().floor(snapshotTimestamp);
            if (version != null && isVersionVisible(baseKey, version, snapshotTimestamp))
            {
                builder.add(entry.getKey());
            }
        }
        return builder.build();
    }

    /**
     * Check is this version of index record is visible (i.e., not marked deleted by a tombstone
     * with timestamp >= the version's timestamp)
     * @param baseKey the key of the index record
     * @param versionTimestamp the version of the index record
     * @param snapshotTimestamp the snapshot timestamp of the transaction
     * @return true if this index record version is visible
     */
    private boolean isVersionVisible(CompositeKey baseKey, long versionTimestamp, long snapshotTimestamp)
    {
        ConcurrentSkipListSet<Long> existingTombstones = tombstones.get(baseKey);
        if (existingTombstones == null)
        {
            return true;
        }
        Long tombstone = existingTombstones.floor(snapshotTimestamp);
        if (tombstone == null)
        {
            return true;
        }
        return tombstone < versionTimestamp || snapshotTimestamp < tombstone;
    }
}