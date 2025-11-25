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
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static io.pixelsdb.pixels.index.mapdb.MapDBThreadResources.EMPTY_VALUE_BUFFER;

/**
 * MapDB-based implementation of SinglePointIndex
 *
 * @author hank
 * @create 2025-11-24
 */
public class MapDBIndex implements SinglePointIndex
{
    private static final Logger logger = LogManager.getLogger(MapDBIndex.class);

    private static final ByteBufferSerializer BYTE_BUFFER_SERIALIZER = new ByteBufferSerializer();
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private final String dbFilePath;
    private final DB db;
    private final BTreeMap<ByteBuffer, ByteBuffer> indexMap;

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
        this.dbFilePath = mapdbPath + indexId + ".db";

        File dbFile = new File(dbFilePath);

        this.db = DBMaker.fileDB(dbFile)
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .transactionEnable()
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
        else
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            ByteBuffer lowerBound = toKeyBuffer(key);
            ByteBuffer upperBound = BYTE_BUFFER_SERIALIZER.nextValue(lowerBound);
            Iterator<ByteBuffer> iterator = indexMap.keyIterator(lowerBound, true, upperBound, false);
            while (iterator.hasNext())
            {
                builder.add(extractRowIdFromKey(iterator.next()));
            }
            return builder.build();
        }
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
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries)
            throws SinglePointIndexException
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
                return prevRowId;
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
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException
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

        long rowId = getUniqueRowId(key);

        try
        {
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
        try ()
        {
            for (IndexProto.IndexKey key : indexKeys)
            {
                ByteBuffer keyBuffer = toKeyBuffer(key);

                try (RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle, readOptions))
                {
                    iterator.seek(keyBuffer);
                    while (iterator.isValid())
                    {
                        ByteBuffer keyFound = ByteBuffer.wrap(iterator.key());
                        if (startsWith(keyFound, keyBuffer))
                        {
                            if(unique)
                            {
                                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                                iterator.value(valueBuffer);
                                long rowId = valueBuffer.getLong();
                                if(rowId > 0)
                                    builder.add(rowId);
                            }
                            // keyFound is not direct, must use its backing array
                            writeBatch.delete(columnFamilyHandle, keyFound.array());
                            iterator.next();
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to purge entries by prefix", e);
        }
    }

    @Override
    @Deprecated
    public void close() throws IOException
    {
        if (db != null && !db.isClosed())
        {
            db.close();
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        try
        {
            if (db != null && !db.isClosed())
            {
                db.close();
            }

            if (dbFilePath != null)
            {
                File dbFile = new File(dbFilePath);
                if (dbFile.exists())
                {
                    return dbFile.delete();
                }
            }

            return true;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to close and remove MapDB index", e);
        }
    }

    /**
     * Serialize IndexKey to byte array for use as MapDB key
     */
    private byte[] serializeIndexKey(IndexProto.IndexKey key)
    {
        return key.toByteArray();
    }

    /**
     * Get the number of entries in this index
     *
     * @return the size of the index
     */
    public long size()
    {
        return indexMap.size();
    }

    /**
     * Check if the index is empty
     *
     * @return true if the index is empty
     */
    public boolean isEmpty()
    {
        return indexMap.isEmpty();
    }

    /**
     * Clear all entries from the index
     *
     * @throws SinglePointIndexException
     */
    public void clear() throws SinglePointIndexException
    {
        try
        {
            indexMap.clear();
            db.commit();
        }
        catch (Exception e)
        {
            db.rollback();
            throw new SinglePointIndexException("Failed to clear index", e);
        }
    }

    /**
     * Get primary index entry by key
     */
    public IndexProto.PrimaryIndexEntry getPrimaryEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("getPrimaryEntry should only be called on unique index");
        }

        byte[] keyBytes = serializeIndexKey(key);
        byte[] valueBytes = indexMap.get(keyBytes);

        if (valueBytes == null)
        {
            return null;
        }

        try
        {
            return IndexProto.PrimaryIndexEntry.parseFrom(valueBytes);
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse primary index entry", e);
        }
    }

    /**
     * Get secondary index entry by key
     */
    public IndexProto.SecondaryIndexEntry getSecondaryEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (unique)
        {
            throw new SinglePointIndexException("getSecondaryEntry should only be called on non-unique index");
        }

        byte[] keyBytes = serializeIndexKey(key);
        byte[] valueBytes = indexMap.get(keyBytes);

        if (valueBytes == null)
        {
            return null;
        }

        try
        {
            return IndexProto.SecondaryIndexEntry.parseFrom(valueBytes);
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse secondary index entry", e);
        }
    }

    /**
     * Check if the index is closed and throw exception if it is.
     */
    private void checkClosed() throws SinglePointIndexException
    {
        if (db == null || db.isClosed())
        {
            throw new SinglePointIndexException("MapDBIndex is closed for table " + tableId + " index " + indexId);
        }
    }

    protected static ByteBuffer toBuffer(long indexId, ByteString key, int bufferNum, long... postValues)
            throws SinglePointIndexException
    {
        int keySize = key.size();
        int totalLength = Long.BYTES + keySize + Long.BYTES;
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

    // convert IndexKey to byte array
    protected static ByteBuffer toKeyBuffer(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 1, Long.MAX_VALUE - key.getTimestamp());
    }

    // create composite key with rowId
    protected static ByteBuffer toNonUniqueKeyBuffer(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 1, Long.MAX_VALUE - key.getTimestamp(), rowId);
    }

    // check if byte array starts with specified prefix
    protected static boolean startsWith(ByteBuffer keyFound, ByteBuffer keyCurrent)
    {
        // prefix is indexId + key, without timestamp
        int prefixLength = keyCurrent.limit() - Long.BYTES;
        if (keyFound.limit() < prefixLength)
        {
            return false;
        }
        keyFound.position(0);
        keyCurrent.position(0);
        ByteBuffer keyFound1 = keyFound.slice();
        keyFound1.limit(prefixLength);
        ByteBuffer keyCurrent1 = keyCurrent.slice();
        keyCurrent1.limit(prefixLength);
        return keyFound1.compareTo(keyCurrent1) == 0;
    }

    // extract rowId from key
    protected static long extractRowIdFromKey(ByteBuffer keyBuffer)
    {
        // extract rowId portion (last 8 bytes of key)
        return Long.MAX_VALUE - keyBuffer.getLong(keyBuffer.limit() - Long.BYTES);
    }
}