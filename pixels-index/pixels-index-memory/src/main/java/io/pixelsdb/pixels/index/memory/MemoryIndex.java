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
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
     * For unique indexes: baseKey -> (timestamp -> rowId)
     */
    private final ConcurrentHashMap<ByteString, ConcurrentSkipListMap<Long, Long>> uniqueIndex;
    /**
     * For non-unique indexes: baseKey -> (rowId -> timestamps)
     */
    private final ConcurrentHashMap<ByteString, ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>>> nonUniqueIndex;

    // Tombstones: baseKey -> tombstone timestamp, the transactions with timestamp >= tombstone timestamp can not see the deleted versions
    private final ConcurrentHashMap<ByteString, Long> tombstones;

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
        ByteString baseKey = extractBaseKey(key);
        long snapshotTimestamp = key.getTimestamp();
        // Find the latest version visible at the snapshot timestamp
        return findUniqueRowId(baseKey, snapshotTimestamp);
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        checkClosed();
        ByteString baseKey = extractBaseKey(key);
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
        ByteString baseKey = extractBaseKey(key);
        long timestamp = key.getTimestamp();
        internalPutEntry(rowId, baseKey, timestamp);
        return true;
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws MainIndexException, SinglePointIndexException
    {
        checkClosed();
        if (!unique)
        {
            throw new SinglePointIndexException("putPrimaryEntries can only be called on unique indexes");
        }
        MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
        for (IndexProto.PrimaryIndexEntry entry : entries)
        {
            ByteString baseKey = extractBaseKey(entry.getIndexKey());
            long timestamp = entry.getIndexKey().getTimestamp();
            ConcurrentSkipListMap<Long, Long> versions =
                    this.uniqueIndex.computeIfAbsent(baseKey, k -> new ConcurrentSkipListMap<>());
            versions.put(timestamp, entry.getRowId());
            mainIndex.putEntry(entry.getRowId(), entry.getRowLocation());
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
            ByteString baseKey = extractBaseKey(key);
            long timestamp = key.getTimestamp();
            internalPutEntry(rowId, baseKey, timestamp);
        }
        return true;
    }

    private void internalPutEntry(long rowId, ByteString baseKey, long timestamp)
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
        ByteString baseKey = extractBaseKey(key);
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
        ByteString baseKey = extractBaseKey(key);
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
            ByteString baseKey = extractBaseKey(entry.getIndexKey());
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
            ByteString baseKey = extractBaseKey(key);
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

    private void putNonUniqueNewVersion(long rowId, ByteString baseKey, long timestamp)
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
        ByteString baseKey = extractBaseKey(key);
        long rowId = getUniqueRowId(key);
        if (rowId < 0)
        {
            return -1L;
        }
        // Add tombstone instead of removing the entry
        this.tombstones.put(baseKey, key.getTimestamp());

        return rowId;
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        checkClosed();
        ByteString baseKey = extractBaseKey(key);
        if (unique)
        {
            long rowId = getUniqueRowId(key);
            if (rowId < 0)
            {
                return ImmutableList.of();
            }
            this.tombstones.put(baseKey, key.getTimestamp());
            return ImmutableList.of(rowId);
        }
        else
        {
            List<Long> rowIds = getRowIds(key);
            if (rowIds.isEmpty())
            {
                return ImmutableList.of();
            }
            tombstones.put(baseKey, key.getTimestamp());
            return rowIds;
        }
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        checkClosed();
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (IndexProto.IndexKey key : keys)
        {
            builder.addAll(deleteEntry(key));
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
            ByteString baseKey = extractBaseKey(key);
            // Remove all versions with timestamp <= purgeTimestamp
            Long tombstone = this.tombstones.get(baseKey);
            if (tombstone != null && tombstone <= key.getTimestamp())
            {
                if (unique)
                {
                    // timestamp -> row id
                    ConcurrentNavigableMap<Long, Long> versions = this.uniqueIndex.get(baseKey).headMap(key.getTimestamp(), true);
                    if (!versions.isEmpty())
                    {
                        builder.addAll(versions.values());
                        versions.clear();
                    }
                }
                else
                {
                    // row id -> timestamps
                    ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> rowIds = this.nonUniqueIndex.get(baseKey);
                    if (rowIds != null)
                    {
                        for (Map.Entry<Long, ConcurrentSkipListSet<Long>> entry : rowIds.entrySet())
                        {
                            NavigableSet<Long> versions = entry.getValue().headSet(key.getTimestamp(), true);
                            if (!versions.isEmpty())
                            {
                                builder.add(entry.getKey());
                                versions.clear();
                            }
                        }
                        builder.addAll(rowIds.keySet());
                    }
                }
            }
            tombstones.remove(baseKey);
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
    private ByteString extractBaseKey(IndexProto.IndexKey key)
    {
        byte[] keyBytes = toKeyBytesWithoutTimestamp(key);
        return ByteString.copyFrom(keyBytes);
    }

    /**
     * Convert IndexKey to byte array without timestamp (for base key)
     */
    private byte[] toKeyBytesWithoutTimestamp(IndexProto.IndexKey key)
    {
        // Same as RocksDBIndex but without the timestamp part
        int keySize = key.getKey().size();
        byte[] baseKeyBytes = new byte[Long.BYTES + keySize];
        // Write indexId (8 bytes, big endian)
        writeLongBE(baseKeyBytes, 0, key.getIndexId());
        // Write key bytes (variable length)
        key.getKey().copyTo(baseKeyBytes, Long.BYTES);
        return baseKeyBytes;
    }

    /**
     * Find the latest visible version for a unique index.
     * @param baseKey the base key of the index entry
     * @param snapshotTimestamp the snapshot timestamp
     * @return the latest visible row id, or -1 if no row id is visible
     */
    private long findUniqueRowId(ByteString baseKey, long snapshotTimestamp)
    {
        // Check if this version is visible (not deleted by a tombstone)
        if (!isVersionVisible(baseKey, snapshotTimestamp))
        {
            return -1;
        }
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
        return versionEntry.getValue();
    }

    /**
     * Find the latest visible version for a non-unique index.
     */
    private List<Long> findNonUniqueRowIds(ByteString baseKey, long snapshotTimestamp)
    {
        ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> rowIds = this.nonUniqueIndex.get(baseKey);
        if (rowIds == null || !isVersionVisible(baseKey, snapshotTimestamp))
        {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (Map.Entry<Long, ConcurrentSkipListSet<Long>> entry : rowIds.entrySet())
        {
            Long version = entry.getValue().floor(snapshotTimestamp);
            if (version != null)
            {
                builder.add(entry.getKey());
            }
        }
        return builder.build();
    }

    private boolean isVersionVisible(ByteString baseKey, long snapshotTimestamp)
    {
        Long tombstone = tombstones.get(baseKey);
        if (tombstone == null)
        {
            return true;
        }
        return tombstone > snapshotTimestamp;
    }

    /**
     * Write long value in big-endian format
     */
    protected static void writeLongBE(byte[] buf, int offset, long value)
    {
        buf[offset]     = (byte)(value >>> 56);
        buf[offset + 1] = (byte)(value >>> 48);
        buf[offset + 2] = (byte)(value >>> 40);
        buf[offset + 3] = (byte)(value >>> 32);
        buf[offset + 4] = (byte)(value >>> 24);
        buf[offset + 5] = (byte)(value >>> 16);
        buf[offset + 6] = (byte)(value >>> 8);
        buf[offset + 7] = (byte)(value);
    }
}