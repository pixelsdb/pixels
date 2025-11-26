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
package io.pixelsdb.pixels.index.mapdb;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.pixelsdb.pixels.index.mapdb.MapDBThreadResources.EMPTY_VALUE_BUFFER;

/**
 * MapDB-based implementation of SinglePointIndex.
 *
 * @author hank
 * @create 2025-11-24
 */
public class MapDBIndex implements SinglePointIndex
{
    public static final Logger LOGGER = LogManager.getLogger(MapDBIndex.class);

    private static final ByteBufferSerializer BYTE_BUFFER_SERIALIZER = new ByteBufferSerializer();
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private final String dbFileDir;
    private final DB db;
    private final BTreeMap<ByteBuffer, ByteBuffer> indexMap;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean removed = new AtomicBoolean(false);

    /**
     * Constructor for persistent MapDB index
     */
    public MapDBIndex(long tableId, long indexId, boolean unique, String mapdbPath) throws SinglePointIndexException
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.unique = unique;
        if (mapdbPath == null || mapdbPath.isEmpty())
        {
            throw new SinglePointIndexException("Invalid mapdb path");
        }
        if (!mapdbPath.endsWith("/"))
        {
            mapdbPath += "/";
        }
        this.dbFileDir = mapdbPath + indexId + "/";
        try
        {
            FileUtils.forceMkdir(new File(mapdbPath + indexId));
        }
        catch (IOException e)
        {
            throw new SinglePointIndexException("Failed to create data path for index " + indexId, e);
        }

        File dbFile = new File(dbFileDir + "/index_" + indexId + ".db");

        this.db = DBMaker.fileDB(dbFile)
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                //.transactionEnable()
                .make();

        String mapName = "index_" + indexId;
        this.indexMap = db.treeMap(mapName)
                .keySerializer(BYTE_BUFFER_SERIALIZER)
                .valueSerializer(BYTE_BUFFER_SERIALIZER)
                .createOrOpen();
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
        if (!unique)
        {
            throw new SinglePointIndexException("getUniqueRowId should only be called on unique index");
        }
        ByteBuffer keyBuffer = toKeyBuffer(key);
        Map.Entry<ByteBuffer, ByteBuffer> entry = indexMap.ceilingEntry(keyBuffer);
        if (entry == null)
        {
            return -1L;
        }
        return entry.getValue().getLong();
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (unique)
        {
            return ImmutableList.of(getUniqueRowId(key));
        }
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        ByteBuffer lowerBound = toKeyBuffer(key);
        ByteBuffer upperBound = toUpperBoundKeyBuffer(key);
        Iterator<ByteBuffer> iterator = indexMap.keyIterator(lowerBound, true, upperBound, true);
        while (iterator.hasNext())
        {
            ByteBuffer keyBuffer = iterator.next();
            long rowId = extractRowIdFromKey(keyBuffer);
            long timestamp = extractTimestampFromKey(keyBuffer);
            if (rowId < 0)
            {
                break;
            }
            builder.add(rowId);
        }
        return builder.build();
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            if (unique)
            {
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                indexMap.put(keyBuffer, valueBuffer);
            }
            else
            {
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                indexMap.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return true;
        }
        catch (Throwable e)
        {
            throw new SinglePointIndexException("Failed to put mapdb index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        try
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                indexMap.put(keyBuffer, valueBuffer);
            }
            return true;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to put primary index entries", e);
        }
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if(unique)
                {
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    indexMap.put(keyBuffer, valueBuffer);
                }
                else
                {
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    indexMap.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            return true;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to put secondary index entries", e);
        }
    }

    @Override
    public long updatePrimaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("updatePrimaryEntry should only be called on unique index");
        }
        try
        {
            long prevRowId = getUniqueRowId(key);
            if (prevRowId < 0)
            {
                return prevRowId;
            }
            ByteBuffer keyBuffer = toKeyBuffer(key);
            ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
            valueBuffer.putLong(rowId).position(0);
            indexMap.put(keyBuffer, valueBuffer);
            return prevRowId;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update primary index entry", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            if(unique)
            {
                long prevRowId = getUniqueRowId(key);
                if (prevRowId < 0)   // no previous row ids are found
                {
                    return ImmutableList.of();
                }
                builder.add(prevRowId);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                indexMap.put(keyBuffer, valueBuffer);
            }
            else
            {
                builder.addAll(this.getRowIds(key));
                if (builder.build().isEmpty())  // no previous row ids are found
                {
                    return ImmutableList.of();
                }
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                indexMap.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update secondary index entry", e);
        }
    }

    @Override
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                long prevRowId = getUniqueRowId(key);
                if (prevRowId < 0)  // indicates that this entry hasn't put or has been deleted
                {
                    return ImmutableList.of();
                }
                builder.add(prevRowId);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                indexMap.put(keyBuffer, valueBuffer);
            }
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update primary index entries", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if(unique)
                {
                    long prevRowId = getUniqueRowId(key);
                    if (prevRowId < 0)
                    {
                        return ImmutableList.of();
                    }
                    builder.add(prevRowId);
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    indexMap.put(keyBuffer, valueBuffer);
                }
                else
                {
                    builder.addAll(this.getRowIds(key));
                    if (builder.build().isEmpty())
                    {
                        return ImmutableList.of();
                    }
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    indexMap.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update secondary index entries", e);
        }
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("deleteUniqueEntry should only be called on unique index");
        }
        try
        {
            long rowId = getUniqueRowId(key);
            ByteBuffer keyBuffer = toKeyBuffer(key);
            ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
            valueBuffer.putLong(-1L).position(0); // -1 means a tombstone
            indexMap.put(keyBuffer, valueBuffer);
            return rowId;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to delete unique index entry", e);
        }
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            if(unique)
            {
                long rowId = getUniqueRowId(key);
                if(rowId < 0) // indicates there is a delete before put
                {
                    return ImmutableList.of();
                }
                builder.add(rowId);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                valueBuffer.putLong(-1L).position(0); // -1 means a tombstone
                indexMap.put(keyBuffer, valueBuffer);
            }
            else
            {
                List<Long> rowIds = getRowIds(key);
                if(rowIds.isEmpty()) // indicates there is a delete before put
                {
                    return ImmutableList.of();
                }
                builder.addAll(rowIds);
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, -1L);
                indexMap.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to delete index entry", e);
        }
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.IndexKey key : keys)
            {
                if(unique)
                {
                    long rowId = getUniqueRowId(key);
                    if(rowId < 0) // indicates there is a delete before put
                    {
                        return ImmutableList.of();
                    }
                    builder.add(rowId);
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = MapDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(-1L).position(0); // -1 means a tombstone
                    indexMap.put(keyBuffer, valueBuffer);
                }
                else
                {
                    List<Long> rowIds = getRowIds(key);
                    if(rowIds.isEmpty()) // indicates there is a delete before put
                    {
                        return ImmutableList.of();
                    }
                    builder.addAll(rowIds);
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, -1L);
                    indexMap.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to delete index entries", e);
        }
    }

    @Override
    public List<Long> purgeEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        try
        {
            for (IndexProto.IndexKey key : indexKeys)
            {
                ByteBuffer lowerBound = toKeyBuffer(key);
                ByteBuffer upperBound = toUpperBoundKeyBuffer(key);
                Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator =
                        indexMap.entryIterator(lowerBound, true, upperBound, true);
                boolean foundTombstone = false;
                while (iterator.hasNext())
                {
                    Map.Entry<ByteBuffer, ByteBuffer> entry = iterator.next();
                    ByteBuffer keyFound = entry.getKey();
                    long rowId;
                    if(unique)
                    {
                        rowId = entry.getValue().getLong();
                    }
                    else
                    {
                        rowId = extractRowIdFromKey(keyFound);
                    }
                    if (rowId < 0)
                    {
                        foundTombstone = true;
                    }
                    else if(foundTombstone)
                    {
                        builder.add(rowId);
                    }
                    else
                    {
                        continue;
                    }
                    iterator.remove();
                }
            }
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to purge index entries by prefix", e);
        }
    }

    @Override
    @Deprecated
    public void close()
    {
        if (closed.compareAndSet(false, true))
        {
            if (db != null && !db.isClosed())
            {
                db.close();
            }
            LOGGER.debug("MapDBIndex closed for table {} index {}", tableId, indexId);
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        if (closed.compareAndSet(false, true) && removed.compareAndSet(false, true))
        {
            try
            {
                if (db != null && !db.isClosed())
                {
                    db.close();
                }
                if (dbFileDir != null)
                {
                    FileUtils.deleteDirectory(new File(dbFileDir));
                }
                LOGGER.debug("MapDBIndex closed and removed for table {} index {}", tableId, indexId);
                return true;
            }
            catch (Exception e)
            {
                throw new SinglePointIndexException("Failed to close and remove MapDB index", e);
            }
        }
        return false;
    }

    /**
     * @return the number of entries in this index
     */
    public long size()
    {
        return indexMap.size();
    }



    protected static ByteBuffer toBuffer(long indexId, ByteString key, int bufferNum, long... postValues) throws SinglePointIndexException
    {
        int keySize = key.size();
        int totalLength = Long.BYTES + keySize + Long.BYTES * postValues.length;
        ByteBuffer compositeKey;
        if (bufferNum == 1)
        {
            compositeKey = MapDBThreadResources.getKeyBuffer(totalLength);
        }
        else if (bufferNum == 2)
        {
            compositeKey = MapDBThreadResources.getKeyBuffer2(totalLength);
        }
        else if (bufferNum == 3)
        {
            compositeKey = MapDBThreadResources.getKeyBuffer3(totalLength);
        }
        else
        {
            throw new SinglePointIndexException("Invalid buffer number");
        }
        // Write indexId (8 bytes, big endian)
        compositeKey.putLong(indexId);
        // Write key bytes (variable length)
        key.copyTo(compositeKey);
        // Write post values (8 bytes each, big endian)
        for (long postValue : postValues)
        {
            compositeKey.putLong(postValue);
        }
        compositeKey.position(0);
        return compositeKey;
    }

    protected static ByteBuffer toKeyBuffer(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 1, Long.MAX_VALUE - key.getTimestamp());
    }

    protected static ByteBuffer toUpperBoundKeyBuffer(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 2, Long.MAX_VALUE);
    }

    protected static ByteBuffer toNonUniqueKeyBuffer(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 1, Long.MAX_VALUE - key.getTimestamp(), rowId);
    }

    /**
     * Extract rowId from non-unique key.
     * @param keyBuffer the key buffer of the non-unique key
     * @return the extracted row id
     */
    protected static long extractRowIdFromKey(ByteBuffer keyBuffer)
    {
        // extract rowId portion (last 8 bytes of key)
        return keyBuffer.getLong(keyBuffer.limit() - Long.BYTES);
    }

    /**
     * Extract timestamp from non-unique key.
     * @param keyBuffer the key buffer of the non-unique key
     * @return the extracted row id
     */
    protected static long extractTimestampFromKey(ByteBuffer keyBuffer)
    {
        // extract rowId portion (last 8 bytes of key)
        return Long.MAX_VALUE - keyBuffer.getLong(keyBuffer.limit() - Long.BYTES * 2);
    }
}