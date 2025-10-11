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
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndex implements SinglePointIndex
{
    private final RocksDB rocksDB;
    private final String rocksDBPath;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private boolean closed = false;
    private boolean removed = false;

    public RocksDBIndex(long tableId, long indexId, String rocksDBPath, boolean unique) throws RocksDBException
    {
        this.tableId = tableId;
        this.indexId = indexId;
        // Initialize RocksDB instance
        this.rocksDBPath = rocksDBPath;
        this.rocksDB = RocksDBFactory.getRocksDB(rocksDBPath);
        this.unique = unique;
        this.writeOptions = new WriteOptions();
        this.readOptions = new ReadOptions();
    }

    /**
     * The constructor only for testing (direct RocksDB injection)
     *
     * @param tableId the table id
     * @param indexId the index id
     * @param rocksDB the rocksdb instance
     */
    protected RocksDBIndex(long tableId, long indexId, RocksDB rocksDB, String rocksDBPath, boolean unique)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.rocksDBPath = rocksDBPath;
        this.rocksDB = rocksDB;  // Use injected mock directly
        this.unique = unique;
        this.writeOptions = new WriteOptions();
        this.readOptions = new ReadOptions();
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
        // Get prefix
        byte[] keyBytes = toKeyBytes(key);
        long timestamp = key.getTimestamp();
        byte[] copyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
        setIteratorBounds(readOptions, copyBytes, timestamp + 1);
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
        byte[] keyBytes = toKeyBytes(key);
        long timestamp = key.getTimestamp();
        byte[] copyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
        setIteratorBounds(readOptions, copyBytes, timestamp+1);
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
                rocksDB.put(nonUniqueKey, new byte[0]);
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
            throws SinglePointIndexException, MainIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
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
                // Put main index
                mainIndex.putEntry(entry.getRowId(), entry.getRowLocation());
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
                    writeBatch.put(nonUniqueKey, new byte[0]);
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
                rocksDB.put(nonUniqueKey, new byte[0]);
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
                    writeBatch.put(nonUniqueKey, new byte[0]);
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
                rocksDB.put(nonUniqueKey, new byte[0]);
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
                    writeBatch.put(nonUniqueKey, new byte[0]);
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
                byte[] keyBytes = toKeyBytes(key);
                long timestamp = key.getTimestamp();
                byte[] copyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
                setIteratorBounds(readOptions, copyBytes, timestamp+1);
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
            if (rocksDB != null)
            {
                rocksDB.close(); // Close RocksDB instance
            }
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

    protected static void setIteratorBounds(ReadOptions readOptions, byte[] keyBytes, long timestamp)
    {
        // Build lower bound (timestamp = 0)
        int offset = keyBytes.length - 8;
        for (int i = 0; i < Long.BYTES; i++)
        {
            keyBytes[offset + i] = 0;
        }
        Slice lowerBound = new Slice(keyBytes);
        // Build upper bound (timestamp = timestamp + 1)
        for (int i = Long.BYTES - 1; i >= 0; i--)
        {
            keyBytes[offset + i] = (byte)(timestamp & 0xFF);
            timestamp >>>= 8;
        }
        Slice upperBound = new Slice(keyBytes);
        // Build readOptions
        readOptions.setIterateLowerBound(lowerBound);
        readOptions.setIterateUpperBound(upperBound);
    }

    // Extract rowId from key
    protected static long extractRowIdFromKey(byte[] keyBytes)
    {
        // Extract rowId portion (last 8 bytes of key)
        byte[] rowIdBytes = new byte[Long.BYTES];
        System.arraycopy(keyBytes, keyBytes.length - Long.BYTES, rowIdBytes, 0, Long.BYTES);

        // Convert rowId to long
        return ByteBuffer.wrap(rowIdBytes).getLong();
    }
}