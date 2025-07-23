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
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndex implements SinglePointIndex
{
    public static final Logger LOGGER = LogManager.getLogger(RocksDBIndex.class);

    private final RocksDB rocksDB;
    private final String rocksDBPath;
    private final WriteOptions writeOptions;
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
        this.rocksDB = createRocksDB(rocksDBPath);
        this.unique = unique;
        this.writeOptions = new WriteOptions();
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
    }

    protected RocksDB createRocksDB(String path) throws RocksDBException
    {
        // 1. Get existing column families (returns empty list for new database)
        List<byte[]> existingColumnFamilies;
        try
        {
            existingColumnFamilies = RocksDB.listColumnFamilies(new Options(), path);
        }
        catch (RocksDBException e)
        {
            // For new database, return list containing only default column family
            existingColumnFamilies = Arrays.asList(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        // 2. Ensure default column family is included
        if (!existingColumnFamilies.contains(RocksDB.DEFAULT_COLUMN_FAMILY))
        {
            existingColumnFamilies = new ArrayList<>(existingColumnFamilies);
            existingColumnFamilies.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        // 3. Prepare column family descriptors
        List<ColumnFamilyDescriptor> descriptors = existingColumnFamilies.stream()
                .map(name -> new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()))
                .collect(Collectors.toList());

        // 4. Open database
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);

        return RocksDB.open(dbOptions, path, descriptors, handles);
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
        // Get prefix
        byte[] keyBytes = toByteArray(key);
        byte[] prefixBytes = toKeyPrefix(key);
        long timestamp = key.getTimestamp();
        // Build lower bound and upper bound
        Slice lowerBound = setLowerBound(prefixBytes);
        Slice upperBound = setUpperBound(prefixBytes, timestamp);
        // Build readOptions
        ReadOptions readOptions = new ReadOptions();
        readOptions.setIterateLowerBound(lowerBound);
        readOptions.setIterateUpperBound(upperBound);

        long RowId = -1L;
        try (RocksIterator iterator = rocksDB.newIterator(readOptions))
        {
            iterator.seekForPrev(keyBytes);
            if(iterator.isValid())
            {
                byte[] valueBytes = iterator.value();
                RowId = ByteBuffer.wrap(valueBytes).getLong();
            }
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to get unique row ID by prefix for key: {}", key, e);
        }
        return RowId;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key)
    {
        List<Long> rowIdList = new ArrayList<>();
        byte[] keyBytes = toByteArray(key);
        byte[] prefixBytes = toKeyPrefix(key);
        long timestamp = key.getTimestamp();
        // Build lower bound and upper bound
        Slice lowerBound = setLowerBound(prefixBytes);
        Slice upperBound = setUpperBound(prefixBytes, timestamp);
        // Build readOptions
        ReadOptions readOptions = new ReadOptions();
        readOptions.setIterateLowerBound(lowerBound);
        readOptions.setIterateUpperBound(upperBound);

        // Use RocksDB iterator for prefix search
        try (RocksIterator iterator = rocksDB.newIterator(readOptions))
        {
            for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next())
            {
                byte[] currentKeyBytes = iterator.key();

                // Check if current key starts with prefixBytes
                if (startsWith(currentKeyBytes, prefixBytes))
                {
                    // Extract rowId from key
                    long rowId = extractRowIdFromKey(currentKeyBytes, prefixBytes.length);
                    if(rowId < 0)
                        continue;
                    rowIdList.add(rowId);
                }
                else
                {
                    break; // Stop when prefix no longer matches
                }
            }
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to get row IDs for key: {}", key, e);
            // Return empty array if key doesn't exist or exception occurs
            return ImmutableList.of();
        }
        return rowIdList;
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Convert rowId to byte array
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            if (unique)
            {
                // Write to RocksDB
                writeBatch.put(keyBytes, valueBytes);
            }
            else
            {
                // Create composite key
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                // Store in RocksDB
                writeBatch.put(nonUniqueKey, new byte[0]);
            }
            rocksDB.write(writeOptions, writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("failed to put rocksdb index entry", e);
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
                byte[] keyBytes = toByteArray(key);
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
            LOGGER.error("failed to put rocksdb index entries", e);
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
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Convert rowId to byte array
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                if(unique)
                {
                    // Write to RocksDB
                    writeBatch.put(keyBytes, valueBytes);
                }
                else
                {
                    byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                    writeBatch.put(nonUniqueKey, new byte[0]);
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("failed to put secondary entries", e);
            throw new SinglePointIndexException("failed to put secondary entries", e);
        }
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        long rowId = getUniqueRowId(key);
        try(WriteBatch writeBatch = new WriteBatch())
        {
            byte[] keyBytes = toByteArray(key);
            byte[] newValue = ByteBuffer.allocate(Long.BYTES).putLong(-1L).array();
            writeBatch.put(keyBytes,newValue);
            rocksDB.write(writeOptions, writeBatch);
            return rowId;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("failed to delete unique entry", e);
            throw new SinglePointIndexException("failed to delete unique entry", e);
        }
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        List<Long> rowIds = new ArrayList<>();
        try(WriteBatch writeBatch = new WriteBatch())
        {
            byte[] keyBytes = toByteArray(key);
            if(unique)
            {
                rowIds.add(this.getUniqueRowId(key));
                byte[] newValue = ByteBuffer.allocate(Long.BYTES).putLong(-1L).array();
                writeBatch.put(keyBytes,newValue);
            }
            else
            {
                rowIds.addAll(this.getRowIds(key));
                overwriteRowId(keyBytes, -1L);
                writeBatch.put(keyBytes, new byte[0]);
            }
            rocksDB.write(writeOptions, writeBatch);
            return rowIds;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("failed to delete entry", e);
            throw new SinglePointIndexException("failed to delete entry", e);
        }
    }


    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        List<Long> rowIds = new ArrayList<>();
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Delete single point index
            for(IndexProto.IndexKey key : keys)
            {
                byte[] keyBytes = toByteArray(key);
                if(unique)
                {
                    rowIds.add(this.getUniqueRowId(key));
                    byte[] newValue = ByteBuffer.allocate(Long.BYTES).putLong(-1L).array();
                    writeBatch.put(keyBytes,newValue);
                }
                else
                {
                    rowIds.addAll(this.getRowIds(key));
                    overwriteRowId(keyBytes, -1L);
                    writeBatch.put(keyBytes, new byte[0]);
                }
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return rowIds;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("failed to delete entries", e);
            throw new SinglePointIndexException("failed to delete entries", e);
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
            } catch (IOException e)
            {
                throw new SinglePointIndexException("failed to clean up RocksDB directory: " + e);
            }
        }
        return true;
    }

    // Convert IndexKey to byte array
    private static byte[] toByteArray(IndexProto.IndexKey key)
    {
        byte[] indexIdBytes = ByteBuffer.allocate(Long.BYTES).putLong(key.getIndexId()).array();
        byte[] keyBytes = key.getKey().toByteArray();
        byte[] timestampBytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(key.getTimestamp()).array();
        // Combine indexId, key and timestamp
        byte[] compositeKey = new byte[indexIdBytes.length + keyBytes.length + timestampBytes.length];
        // Copy indexId
        System.arraycopy(indexIdBytes, 0, compositeKey, 0, indexIdBytes.length);
        // Copy key
        System.arraycopy(keyBytes, 0, compositeKey, indexIdBytes.length, keyBytes.length);
        // Copy timestamp
        System.arraycopy(timestampBytes, 0, compositeKey, indexIdBytes.length + keyBytes.length, timestampBytes.length);

        return compositeKey;
    }

    // Extract key prefix (key without timestamp) from key
    private static byte[] toKeyPrefix(IndexProto.IndexKey key)
    {
        byte[] fullKey = toByteArray(key);
        int prefixLength = fullKey.length - Long.BYTES;
        return Arrays.copyOf(fullKey, prefixLength);
    }

    // Create composite key with rowId
    private static byte[] toNonUniqueKey(byte[] keyBytes, byte[] valueBytes)
    {
        byte[] nonUniqueKey = new byte[keyBytes.length + valueBytes.length];
        System.arraycopy(keyBytes, 0, nonUniqueKey, 0, keyBytes.length);
        System.arraycopy(valueBytes, 0, nonUniqueKey, keyBytes.length, valueBytes.length);
        return nonUniqueKey;
    }

    // Check if byte array starts with specified prefix
    private boolean startsWith(byte[] array, byte[] prefix)
    {
        if (array.length < prefix.length)
        {
            return false;
        }
        for (int i = 0; i < prefix.length; i++)
        {
            if (array[i] != prefix[i])
            {
                return false;
            }
        }
        return true;
    }

    private Slice setLowerBound(byte[] prefixBytes)
    {
        ByteBuffer lowerBoundBuffer = ByteBuffer.allocate(prefixBytes.length + Long.BYTES);
        lowerBoundBuffer.put(prefixBytes);
        lowerBoundBuffer.putLong(0L);
        byte[] lowerBoundBytes = lowerBoundBuffer.array();
        return new Slice(lowerBoundBytes);
    }

    private Slice setUpperBound(byte[] prefixBytes, long timestamp)
    {
        ByteBuffer upperBoundBuffer = ByteBuffer.allocate(prefixBytes.length + Long.BYTES);
        upperBoundBuffer.put(prefixBytes);
        upperBoundBuffer.order(ByteOrder.BIG_ENDIAN).putLong(timestamp + 1);
        byte[] upperBoundBytes = upperBoundBuffer.array();
        return new Slice(upperBoundBytes);
    }

    // Extract rowId from key
    private long extractRowIdFromKey(byte[] keyBytes, int prefixLength)
    {
        // Extract rowId portion (last 8 bytes of key)
        byte[] rowIdBytes = new byte[Long.BYTES];
        System.arraycopy(keyBytes, keyBytes.length - Long.BYTES, rowIdBytes, 0, Long.BYTES);

        // Convert rowId to long
        return ByteBuffer.wrap(rowIdBytes).getLong();
    }

    // Update rowId for single point index
    private void overwriteRowId(byte[] key, long value)
    {
        byte[] minusOneBytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        System.arraycopy(minusOneBytes, 0, key, key.length - Long.BYTES, Long.BYTES);
    }
}
