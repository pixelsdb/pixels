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
package io.pixelsdb.pixels.index.rocksdb;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static io.pixelsdb.pixels.index.rocksdb.RocksDBThreadResources.EMPTY_VALUE_BUFFER;

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndex implements SinglePointIndex
{
    private final RocksDB rocksDB;
    private final String rocksDBPath;
    private final WriteOptions writeOptions;
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private boolean closed = false;
    private boolean removed = false;

    public RocksDBIndex(long tableId, long indexId, boolean unique) throws RocksDBException
    {
        this.tableId = tableId;
        this.indexId = indexId;
        // Initialize RocksDB instance
        this.rocksDBPath = RocksDBFactory.getDbPath();
        this.rocksDB = RocksDBFactory.getRocksDB();
        this.unique = unique;
        this.writeOptions = new WriteOptions();
    }

    /**
     * The constructor only for testing (direct RocksDB injection)
     * @param tableId the table id
     * @param indexId the index id
     * @param rocksDB the rocksdb instance
     */
    @Deprecated
    protected RocksDBIndex(long tableId, long indexId, RocksDB rocksDB, String rocksDBPath, boolean unique)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.rocksDBPath = rocksDBPath;
        this.rocksDB = rocksDB;  // Use injected mock directly
        this.unique = unique;
        this.writeOptions = new WriteOptions();
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
        ReadOptions readOptions = RocksDBThreadResources.getReadOptions();
        readOptions.setPrefixSameAsStart(true);
        ByteBuffer keyBuffer = toKeyBuffer(key);
        long rowId = -1L;
        try (RocksIterator iterator = rocksDB.newIterator(readOptions))
        {
            iterator.seek(keyBuffer);
            if (iterator.isValid())
            {
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                iterator.value(valueBuffer);
                rowId = valueBuffer.getLong();
            }
        }
        return rowId;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        ReadOptions readOptions = RocksDBThreadResources.getReadOptions();
        setIteratorBounds(readOptions, key);
        ByteBuffer keyBuffer = toKeyBuffer(key);
        // Use RocksDB iterator for prefix search
        try (RocksIterator iterator = rocksDB.newIterator(readOptions))
        {
            iterator.seekForPrev(keyBuffer);
            // Search in reverse order if index entry isn't deleted.
            while (iterator.isValid())
            {
                ByteBuffer keyFound = ByteBuffer.wrap(iterator.key());
                if (startsWith(keyFound, keyBuffer))
                {
                    long rowId = extractRowIdFromKey(keyFound);
                    if (rowId < 0)
                    {
                        break;
                    }
                    builder.add(rowId);
                    iterator.prev();
                }
                else
                {
                    break;
                }
            }
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
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                rocksDB.put(writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                rocksDB.put(writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to put rocksdb index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                writeBatch.put(keyBuffer, valueBuffer);
            }
            rocksDB.write(writeOptions, writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to put rocksdb index entries", e);
        }
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if(unique)
                {
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    writeBatch.put(keyBuffer, valueBuffer);
                }
                else
                {
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    writeBatch.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to put secondary entries", e);
        }
    }

    @Override
    public long updatePrimaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            long prevRowId = getUniqueRowId(key);
            ByteBuffer keyBuffer = toKeyBuffer(key);
            ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
            valueBuffer.putLong(rowId).position(0);
            rocksDB.put(writeOptions, keyBuffer, valueBuffer);
            return prevRowId;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to update primary entry", e);
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
                builder.add(this.getUniqueRowId(key));
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                rocksDB.put(writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                builder.addAll(this.getRowIds(key));
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                rocksDB.put(writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to update secondary entry", e);
        }
    }

    @Override
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                builder.add(this.getUniqueRowId(key));
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                writeBatch.put(keyBuffer, valueBuffer);
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to update primary index entries", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();

                if(unique)
                {
                    builder.add(this.getUniqueRowId(key));
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    writeBatch.put(keyBuffer, valueBuffer);
                }
                else
                {
                    builder.addAll(this.getRowIds(key));
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    writeBatch.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to put secondary entries", e);
        }
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        long rowId = getUniqueRowId(key);
        try
        {
            ByteBuffer keyBuffer = toKeyBuffer(key);
            ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
            valueBuffer.putLong(-1L).position(0); // -1 means a tombstone
            rocksDB.put(writeOptions, keyBuffer, valueBuffer);
            return rowId;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to delete unique entry", e);
        }
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        try
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
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(-1L).position(0); // -1 means a tombstone
                rocksDB.put(writeOptions, keyBuffer, valueBuffer);
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
                rocksDB.put(writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to delete entry", e);
        }
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Delete single point index
            for(IndexProto.IndexKey key : keys)
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
                    ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(-1L).position(0); // -1 means a tombstone
                    writeBatch.put(keyBuffer, valueBuffer);
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
                    writeBatch.put(nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to delete entries", e);
        }
    }

    @Override
    public List<Long> purgeEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        try (WriteBatch writeBatch = new WriteBatch())
        {
            for (IndexProto.IndexKey key : indexKeys)
            {
                toKeyBuffer(key);
                ReadOptions readOptions = RocksDBThreadResources.getReadOptions();
                setIteratorBounds(readOptions, key);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                try (RocksIterator iterator = rocksDB.newIterator(readOptions))
                {
                    iterator.seekForPrev(keyBuffer);
                    while (iterator.isValid())
                    {
                        ByteBuffer keyFound = ByteBuffer.wrap(iterator.key());
                        if (startsWith(keyFound, keyBuffer))
                        {
                            if(unique)
                            {
                                long rowId = ByteBuffer.wrap(iterator.value()).getLong();
                                if(rowId > 0)
                                    builder.add(rowId);
                            }
                            // keyFound is not direct, must use its backing array
                            writeBatch.delete(keyFound.array());
                            iterator.prev();
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
            throw new SinglePointIndexException("failed to purge entries by prefix", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (!closed)
        {
            closed = true;
            // Issue #1158: do not directly close the rocksDB instance as it is shared by other indexes
            RocksDBFactory.close();
            writeOptions.close();
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        try
        {
            this.close();
        } catch (IOException e)
        {
            throw new SinglePointIndexException("failed to close single point index", e);
        }

        if (!removed)
        {
            removed = true;
            // clear RocksDB directory for main index
            try
            {
                FileUtils.deleteDirectory(new File(rocksDBPath));
            }
            catch (IOException e)
            {
                throw new SinglePointIndexException("failed to clean up RocksDB directory: " + e);
            }
        }
        return true;
    }

    protected static ByteBuffer toBuffer(long indexId, ByteString key, long postValue, int bufferNum)
            throws SinglePointIndexException
    {
        int keySize = key.size();
        int totalLength = Long.BYTES + keySize + Long.BYTES;
        ByteBuffer compositeKey;
        if (bufferNum == 1)
        {
            compositeKey = RocksDBThreadResources.getKeyBuffer(totalLength);
        }
        else if (bufferNum == 2)
        {
            compositeKey = RocksDBThreadResources.getKeyBuffer2(totalLength);
        }
        else if (bufferNum == 3)
        {
            compositeKey = RocksDBThreadResources.getKeyBuffer3(totalLength);
        }
        else
        {
            throw new SinglePointIndexException("invalid buffer number");
        }
        // Write indexId (8 bytes, big endian)
        compositeKey.putLong(indexId);
        // Write key bytes (variable length)
        key.copyTo(compositeKey);
        // Write post value (8 bytes, big endian)
        compositeKey.putLong(postValue);
        compositeKey.position(0);
        return compositeKey;
    }

    // Convert IndexKey to byte array
    protected static ByteBuffer toKeyBuffer(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), Long.MAX_VALUE - key.getTimestamp(), 1);

    }

    // Create composite key with rowId
    protected static ByteBuffer toNonUniqueKeyBuffer(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), rowId, 1);
    }

    // Check if byte array starts with specified prefix
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

    /**
     * Set iterator bounds in the read options to [indexId_keyString_0, indexId_keyString_ts+1) and return the key bytes
     * of indexId_keyString_ts that can be further used to seek to in the iterator.
     * @param readOptions the read options
     * @param key the index key
     */
    protected static void setIteratorBounds(ReadOptions readOptions, IndexProto.IndexKey key) throws SinglePointIndexException
    {
        ByteBuffer lowerBoundBuffer = toBuffer(key.getIndexId(), key.getKey(), 0, 2);
        // Build lower bound (timestamp = 0)
        // Issue #1174: due to a bug in rocksdbjni, we much set the buffer length to create DirectSlice correctly.
        DirectSlice lowerBound = new DirectSlice(lowerBoundBuffer, lowerBoundBuffer.limit());
        // Build upper bound (timestamp = timestamp + 1)
        ByteBuffer upperBoundBuffer = toBuffer(key.getIndexId(), key.getKey(), key.getTimestamp() + 1, 3);
        DirectSlice upperBound = new DirectSlice(upperBoundBuffer, upperBoundBuffer.limit());
        readOptions.setIterateLowerBound(lowerBound);
        readOptions.setIterateUpperBound(upperBound);
    }

    // Extract rowId from key
    protected static long extractRowIdFromKey(ByteBuffer keyBuffer)
    {
        // Extract rowId portion (last 8 bytes of key)
        return keyBuffer.getLong(keyBuffer.limit() - Long.BYTES);
    }
}