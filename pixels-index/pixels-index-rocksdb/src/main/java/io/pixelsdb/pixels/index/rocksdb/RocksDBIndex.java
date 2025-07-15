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

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndex implements SinglePointIndex
{
    private final RocksDB rocksDB;
    public static final Logger LOGGER = LogManager.getLogger(RocksDBIndex.class);
    private final MainIndex mainIndex;

    public RocksDBIndex(String rocksDBPath, MainIndex mainIndex) throws RocksDBException
    {
        // Initialize RocksDB instance
        this.rocksDB = createRocksDB(rocksDBPath);
        this.mainIndex = mainIndex;
    }

    // Constructor for testing (direct RocksDB injection)
    protected RocksDBIndex(RocksDB rocksDB, MainIndex mainIndex)
    {
        this.rocksDB = rocksDB;  // Use injected mock directly
        this.mainIndex = mainIndex;
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
                .map(name -> new ColumnFamilyDescriptor(
                        name,
                        new ColumnFamilyOptions()
                ))
                .collect(Collectors.toList());

        // 4. Open database
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true);

        return RocksDB.open(dbOptions, path, descriptors, handles);
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key)
    {
        try
        {
            // Generate composite key
            byte[] compositeKey = toByteArray(key);

            // Get value from RocksDB
            byte[] valueBytes = rocksDB.get(compositeKey);

            if (valueBytes != null)
            {
                return ByteBuffer.wrap(valueBytes).getLong();
            }
            else
            {
                System.out.println("No value found for composite key: " + key);
            }
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to get unique row ID for key: {}", key, e);
        }
        // Return default value (0) if key doesn't exist or exception occurs
        return 0;
    }

    @Override
    public long[] getRowIds(IndexProto.IndexKey key)
    {
        List<Long> rowIdList = new ArrayList<>();

        try
        {
            // Convert IndexKey to byte array (without rowId)
            byte[] prefixBytes = toByteArray(key);

            // Use RocksDB iterator for prefix search
            try (RocksIterator iterator = rocksDB.newIterator())
            {
                for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next())
                {
                    byte[] currentKeyBytes = iterator.key();

                    // Check if current key starts with prefixBytes
                    if (startsWith(currentKeyBytes, prefixBytes))
                    {
                        // Extract rowId from key
                        long rowId = extractRowIdFromKey(currentKeyBytes, prefixBytes.length);
                        rowIdList.add(rowId);
                    }
                    else
                    {
                        break; // Stop when prefix no longer matches
                    }
                }
            }
            // Convert List<Long> to long[]
            return parseRowIds(rowIdList);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to get row IDs for key: {}", key, e);
        }
        // Return empty array if key doesn't exist or exception occurs
        return new long[0];
    }

    @Override
    public boolean putPrimaryEntry(IndexProto.PrimaryIndexEntry entry) throws MainIndexException, SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Extract key and rowId from Entry object
            IndexProto.IndexKey key = entry.getIndexKey();
            long rowId = entry.getTableRowId();
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Convert rowId to byte array
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();

            // Write to RocksDB
            writeBatch.put(keyBytes, valueBytes);
            // Put rowId into MainIndex
            IndexProto.RowLocation rowLocation = entry.getRowLocation();
            boolean success = mainIndex.putRowId(rowId, rowLocation);
            if (!success)
            {
                LOGGER.error("Failed to put Entry into main index for rowId {}", rowId);
                throw new MainIndexException("Failed to put Entry into main index for rowId");
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to put Primary Entry: {} by entry", entry);
            throw new SinglePointIndexException("Failed to put Primary Entry",e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws MainIndexException, SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Process each Entry object
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                // Extract key and rowId from Entry object
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getTableRowId();
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Convert rowId to byte array
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();

                // Write to RocksDB
                writeBatch.put(keyBytes, valueBytes);
            }
            // Select start rowId and end rowId
            IndexProto.PrimaryIndexEntry entryStart = entries.get(0);
            IndexProto.PrimaryIndexEntry entryEnd = entries.get(entries.size() - 1);
            long start = entryStart.getTableRowId();
            long end = entryEnd.getTableRowId();
            // Create new RowIdRange and RgLocation
            RowIdRange newRange = new RowIdRange(start, end);
            IndexProto.RowLocation rowLocation = entryStart.getRowLocation();
            MainIndex.RgLocation rgLocation = new MainIndex.RgLocation(rowLocation.getFileId(), rowLocation.getRgId());
            // Put RowIds to MainIndex
            boolean success = mainIndex.putRowIds(newRange, rgLocation);
            if (!success)
            {
                LOGGER.error("Failed to put Entry into main index for rowId RowIdRange [{}-{}]", start, end);
                throw new MainIndexException("Failed to put Entry into main index for rowId RowIdRange");
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to put Primary Entries: {} by entries", entries, e);
            throw new SinglePointIndexException("Failed to put Primary Entries",e);
        }
    }

    @Override
    public boolean putSecondaryEntry(IndexProto.SecondaryIndexEntry entry) throws SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Extract key and rowId from Entry object
            IndexProto.IndexKey key = entry.getIndexKey();
            long rowId = entry.getTableRowId();
            boolean unique = entry.getUnique();
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
                writeBatch.put(nonUniqueKey, null);
            } 
            rocksDB.write(new WriteOptions(), writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to put Secondary Entry: {} by entry", entry);
            throw new SinglePointIndexException("Failed to put Secondary Entry",e);
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
                long rowId = entry.getTableRowId();
                boolean unique = entry.getUnique();
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
                    writeBatch.put(nonUniqueKey, null);
                }
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to put Secondary Entries: {} by entries", entries, e);
            throw new SinglePointIndexException("Failed to put Secondary Entries",e);
        }
    }

    @Override
    public IndexProto.RowLocation deletePrimaryEntry(IndexProto.IndexKey key) throws MainIndexException, SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Get RowId in order to delete MainIndex
            long rowId = getUniqueRowId(key);
            // Delete key-value pair from RocksDB
            writeBatch.delete(keyBytes);
            // Delete MainIndex
            boolean success = mainIndex.deleteRowId(rowId);
            if (!success)
            {
                LOGGER.error("Failed to delete Entry of main index for rowId {}", rowId);
                throw new MainIndexException("Failed to delete Entry of main index for rowId");
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return null; // TODO: implement
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to delete Primary Entry: {}", key, e);
            throw new SinglePointIndexException("Failed to delete Primary Entry",e);
        }
    }

    @Override
    public List<IndexProto.RowLocation> deletePrimaryEntries(List<IndexProto.IndexKey> keys) throws MainIndexException, SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            List<Long> rowIds = new ArrayList<>();
            // Delete single point index
            for(IndexProto.IndexKey key : keys)
            {
                // Get rowId
                long rowId = getUniqueRowId(key);
                rowIds.add(rowId);
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Delete key-value pair from RocksDB
                writeBatch.delete(keyBytes);
            }
            if (rowIds.isEmpty())
            {
                LOGGER.warn("No rowIds found for keys: {}", keys);
                throw new MainIndexException("No rowIds found for keys");
            }
            // Found start rowId and end rowId
            long start = Collections.min(rowIds);
            long end = Collections.max(rowIds);
            RowIdRange newRange = new RowIdRange(start, end);
            // Delete MainIndex
            boolean success = mainIndex.deleteRowIds(newRange);
            if (!success)
            {
                LOGGER.error("Failed to delete Entry of main index for rowId RowIdRange [{}-{}]", start, end);
                throw new MainIndexException("Failed to delete Entry of main index for rowId RowIdRange");
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return null; // TODO: implement
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to delete Entries: {}", keys, e);
            throw new SinglePointIndexException("Failed to delete Entries",e);
        }
    }

    @Override
    public long deleteSecondaryEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Delete key-value pair from RocksDB
            writeBatch.delete(keyBytes);
            rocksDB.write(new WriteOptions(), writeBatch);
            return 0; // TODO: implement
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to delete Secondary Entry: {}", key, e);
            throw new SinglePointIndexException("Failed to delete Secondary Entry",e);
        }
    }

    @Override
    public List<Long> deleteSecondaryEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        try(WriteBatch writeBatch = new WriteBatch())
        {
            // Delete single point index
            for(IndexProto.IndexKey key : keys)
            {
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Delete key-value pair from RocksDB
                writeBatch.delete(keyBytes);
            }
            rocksDB.write(new WriteOptions(), writeBatch);
            return null; // TODO: implement
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to delete Secondary Entries: {}", keys, e);
            throw new SinglePointIndexException("Failed to delete Secondary Entries",e);
        }
    }

    @Override
    public void close() throws IOException
    {
        mainIndex.close();
        if (rocksDB != null)
        {
            rocksDB.close(); // Close RocksDB instance
        }
    }

    // Convert IndexKey to byte array
    private static byte[] toByteArray(IndexProto.IndexKey key)
    {
        byte[] indexIdBytes = ByteBuffer.allocate(Long.BYTES).putLong(key.getIndexId()).array(); // Get indexId bytes
        byte[] keyBytes = key.getKey().toByteArray(); // Get key bytes
        byte[] timestampBytes = ByteBuffer.allocate(Long.BYTES).putLong(key.getTimestamp()).array(); // Get timestamp bytes
        // Combine indexId, key and timestamp
        byte[] compositeKey = new byte[indexIdBytes.length + 1 + keyBytes.length + 1 + timestampBytes.length];
        // Copy indexId
        System.arraycopy(indexIdBytes, 0, compositeKey, 0, indexIdBytes.length);
        // Add separator
        compositeKey[indexIdBytes.length] = ':';
        // Copy key
        System.arraycopy(keyBytes, 0, compositeKey, indexIdBytes.length + 1, keyBytes.length);
        // Add separator
        compositeKey[indexIdBytes.length + 1 + keyBytes.length] = ':';
        // Copy timestamp
        System.arraycopy(timestampBytes, 0, compositeKey, indexIdBytes.length + 1 + keyBytes.length + 1, timestampBytes.length);

        return compositeKey;
    }

    // Create composite key with rowId
    private static byte[] toNonUniqueKey(byte[] keyBytes, byte[] valueBytes)
    {
        byte[] nonUniqueKey = new byte[keyBytes.length + 1 + valueBytes.length];
        System.arraycopy(keyBytes, 0, nonUniqueKey, 0, keyBytes.length);
        nonUniqueKey[keyBytes.length] = ':';
        System.arraycopy(valueBytes, 0, nonUniqueKey, keyBytes.length + 1, valueBytes.length);
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

    // Extract rowId from key
    private long extractRowIdFromKey(byte[] keyBytes, int prefixLength)
    {
        // Extract rowId portion (last 8 bytes of key)
        byte[] rowIdBytes = new byte[Long.BYTES];
        System.arraycopy(keyBytes, keyBytes.length - Long.BYTES, rowIdBytes, 0, Long.BYTES);

        // Convert rowId to long
        return ByteBuffer.wrap(rowIdBytes).getLong();
    }

    // Helper method to parse multiple rowIds
    private long[] parseRowIds(List<Long> rowIdList)
    {
        long[] rowIds = new long[rowIdList.size()];
        for (int i = 0; i < rowIdList.size(); i++)
        {
            rowIds[i] = rowIdList.get(i);
        }
        return rowIds;
    }
}
