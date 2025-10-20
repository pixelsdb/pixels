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

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndex implements SinglePointIndex
{
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final RocksDB rocksDB;
    private final String rocksDBPath;
    private final WriteOptions writeOptions;
    private final ReadOptionsThreadFactory readOptionsFactory;
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
        this.readOptionsFactory = new ReadOptionsThreadFactory();
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
        this.readOptionsFactory = new ReadOptionsThreadFactory();
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
    public long getUniqueRowId(IndexProto.IndexKey key)
    {
        ReadOptions readOptions = readOptionsFactory.getReadOptions();
        byte[] keyBytes = setIteratorBounds(readOptions, key);
        long rowId = -1L;
        try (RocksIterator iterator = rocksDB.newIterator(readOptions))
        {
            iterator.seekForPrev(keyBytes);
            if (iterator.isValid())
            {
                byte[] valueBytes = iterator.value();
                rowId = ByteBuffer.wrap(valueBytes).getLong();
            }
        }
        return rowId;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        ReadOptions readOptions = readOptionsFactory.getReadOptions();
        byte[] keyBytes = setIteratorBounds(readOptions, key);
        // Use RocksDB iterator for prefix search
        try (RocksIterator iterator = rocksDB.newIterator(readOptions))
        {
            iterator.seekForPrev(keyBytes);
            // Search in reverse order if index entry isn't deleted.
            while (iterator.isValid())
            {
                byte[] currentKeyBytes = iterator.key();
                if (startsWith(currentKeyBytes, keyBytes))
                {
                    long rowId = extractRowIdFromKey(currentKeyBytes);
                    if (rowId < 0)
                        break;
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
                // Convert IndexKey to byte array
                byte[] keyBytes = toKeyBytes(key);
                // Convert rowId to byte array
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                // Write to RocksDB
                rocksDB.put(keyBytes, valueBytes);
            }
            else
            {
                // Create composite key
                byte[] nonUniqueKey = toNonUniqueKeyBytes(key, rowId);
                // Store in RocksDB
                rocksDB.put(nonUniqueKey, EMPTY_BYTE_ARRAY);
            }
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("failed to put rocksdb index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            // Process each Entry object
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                // Extract key and rowId from Entry object
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                // Convert IndexKey to byte array
                byte[] keyBytes = toKeyBytes(key);
                // Convert rowId to byte array
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                // Write to RocksDB
                writeBatch.put(keyBytes, valueBytes);
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
            // Process each Entry object
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                // Extract key and rowId from Entry object
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if(unique)
                {
                    // Convert IndexKey to byte array
                    byte[] keyBytes = toKeyBytes(key);
                    // Convert rowId to byte array
                    byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                    // Write to RocksDB
                    writeBatch.put(keyBytes, valueBytes);
                }
                else
                {
                    byte[] nonUniqueKey = toNonUniqueKeyBytes(key, rowId);
                    writeBatch.put(nonUniqueKey, EMPTY_BYTE_ARRAY);
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
            // Get previous rowId and rowLocation
            long prevRowId = getUniqueRowId(key);
            // Convert key and new rowId to bytes
            byte[] keyBytes = toKeyBytes(key);
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            // Write to RocksDB
            rocksDB.put(keyBytes, valueBytes);
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
                // Convert key and new rowId to bytes
                byte[] keyBytes = toKeyBytes(key);
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                // Get previous rowIds
                builder.add(this.getUniqueRowId(key));
                // Write to RocksDB
                rocksDB.put(keyBytes, valueBytes);
            }
            else
            {
                // Get previous rowIds
                builder.addAll(this.getRowIds(key));
                // Write to RocksDB
                byte[] nonUniqueKey = toNonUniqueKeyBytes(key, rowId);
                rocksDB.put(nonUniqueKey, EMPTY_BYTE_ARRAY);
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
            // Process each Entry object
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                // Extract key and new rowId from Entry object
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                // Convert IndexKey and new rowId to byte array
                byte[] keyBytes = toKeyBytes(key);
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                // Get prev rowId
                builder.add(this.getUniqueRowId(key));
                // Write to RocksDB
                writeBatch.put(keyBytes, valueBytes);
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
            // Process each Entry object
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                // Extract key and new rowId from Entry object
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();

                if(unique)
                {
                    // Convert IndexKey and new rowId to byte array
                    byte[] keyBytes = toKeyBytes(key);
                    byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                    // Get old rowIds from index
                    builder.add(this.getUniqueRowId(key));
                    // Write to RocksDB
                    writeBatch.put(keyBytes, valueBytes);
                }
                else
                {
                    // Get previous rowIds from index
                    builder.addAll(this.getRowIds(key));
                    // Write to RocksDB
                    byte[] nonUniqueKey = toNonUniqueKeyBytes(key, rowId);
                    writeBatch.put(nonUniqueKey, EMPTY_BYTE_ARRAY);
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
            byte[] keyBytes = toKeyBytes(key);
            byte[] newValue = ByteBuffer.allocate(Long.BYTES).putLong(-1L).array(); // -1 means a tombstone
            rocksDB.put(keyBytes, newValue);
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
                byte[] keyBytes = toKeyBytes(key);
                byte[] newValue = ByteBuffer.allocate(Long.BYTES).putLong(-1L).array(); // -1 means a tombstone
                long rowId = getUniqueRowId(key);
                if(rowId < 0)   // indicates there is a transaction error, delete invalid index entry
                {
                    // Return empty array if entry not found
                    return ImmutableList.of();
                }
                builder.add(rowId);
                rocksDB.put(keyBytes, newValue);
            }
            else
            {
                List<Long> rowIds = getRowIds(key);
                if(rowIds.isEmpty())    // indicates there is a transaction error, delete invalid index entry
                {
                    // Return empty array if entry not found
                    return ImmutableList.of();
                }
                builder.addAll(rowIds);
                byte[] nonUniqueKey = toNonUniqueKeyBytes(key, -1L);
                rocksDB.put(nonUniqueKey, EMPTY_BYTE_ARRAY);
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
                    byte[] keyBytes = toKeyBytes(key);
                    byte[] newValue = ByteBuffer.allocate(Long.BYTES).putLong(-1L).array(); // -1 means a tombstone
                    long rowId = getUniqueRowId(key);
                    if(rowId < 0)  // indicates there is a transaction error, delete invalid index entry
                    {
                        // Return empty array if entry not found
                        return ImmutableList.of();
                    }
                    builder.add(rowId);
                    writeBatch.put(keyBytes, newValue);
                }
                else
                {
                    List<Long> rowIds = getRowIds(key);
                    if(rowIds.isEmpty())  // indicates there is a transaction error, delete invalid index entry
                    {
                        // Return empty array if entry not found
                        return ImmutableList.of();
                    }
                    builder.addAll(rowIds);
                    byte[] nonUniqueKey = toNonUniqueKeyBytes(key, -1L);
                    writeBatch.put(nonUniqueKey, EMPTY_BYTE_ARRAY);
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
                toKeyBytes(key);
                ReadOptions readOptions = readOptionsFactory.getReadOptions();
                byte[] keyBytes = setIteratorBounds(readOptions, key);
                try (RocksIterator iterator = rocksDB.newIterator(readOptions))
                {
                    iterator.seekForPrev(keyBytes);
                    while (iterator.isValid())
                    {
                        byte[] currentKeyBytes = iterator.key();
                        if (startsWith(currentKeyBytes, keyBytes))
                        {
                            if(unique)
                            {
                                long rowId = ByteBuffer.wrap(iterator.value()).getLong();
                                if(rowId > 0)
                                    builder.add(rowId);
                            }
                            writeBatch.delete(currentKeyBytes);
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
            readOptionsFactory.shutdownAllThreads();
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

    protected static byte[] toBytes(long indexId, ByteString key, long postValue)
    {
        int keySize = key.size();
        int totalLength = Long.BYTES + keySize + Long.BYTES;
        byte[] compositeKey = new byte[totalLength];
        int pos = 0;
        // Write indexId (8 bytes, big endian)
        writeLongBE(compositeKey, pos, indexId);
        pos += 8;
        // Write key bytes (variable length)
        key.copyTo(compositeKey, pos);
        pos += keySize;
        // Write post value (8 bytes, big endian)
        writeLongBE(compositeKey, pos, postValue);
        return compositeKey;
    }

    // Convert IndexKey to byte array
    protected static byte[] toKeyBytes(IndexProto.IndexKey key)
    {
        return toBytes(key.getIndexId(), key.getKey(), key.getTimestamp());
    }

    // Create composite key with rowId
    protected static byte[] toNonUniqueKeyBytes(IndexProto.IndexKey key, long rowId)
    {
        return toBytes(key.getIndexId(), key.getKey(), rowId);
    }

    // Check if byte array starts with specified prefix
    protected static boolean startsWith(byte[] array, byte[] keyBytes)
    {
        // prefix is indexId + key, without timestamp
        int prefixLength = keyBytes.length - Long.BYTES;
        if (array.length < prefixLength)
        {
            return false;
        }
        for (int i = 0; i < prefixLength; i++)
        {
            if (array[i] != keyBytes[i])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Set iterator bounds in the read options to [indexId_keyString_0, indexId_keyString_ts+1) and return the key bytes
     * of indexId_keyString_ts that can be further used to seek to in the iterator.
     * @param readOptions the read options
     * @param key
     * @return
     */
    protected static byte[] setIteratorBounds(ReadOptions readOptions, IndexProto.IndexKey key)
    {
        byte[] keyBytes = toBytes(key.getIndexId(), key.getKey(), 0);
        // Build lower bound (timestamp = 0)
        Slice lowerBound = new Slice(keyBytes);
        // Build upper bound (timestamp = timestamp + 1)
        final int offset = keyBytes.length - 8;
        writeLongBE(keyBytes, offset, key.getTimestamp() + 1);
        Slice upperBound = new Slice(keyBytes);
        readOptions.setIterateLowerBound(lowerBound);
        readOptions.setIterateUpperBound(upperBound);
        // Recover the timestamp postfix in the key bytes
        writeLongBE(keyBytes, offset, key.getTimestamp());
        return keyBytes;
    }

    // Extract rowId from key
    protected static long extractRowIdFromKey(byte[] keyBytes)
    {
        // Extract rowId portion (last 8 bytes of key)
        return ByteBuffer.wrap(keyBytes).getLong(keyBytes.length - Long.BYTES);
    }
}