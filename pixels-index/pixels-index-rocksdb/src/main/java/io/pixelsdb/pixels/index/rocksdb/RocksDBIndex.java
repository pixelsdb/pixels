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

import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hank
 * @create 2025-02-09
 */
public class RocksDBIndex implements SecondaryIndex
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

        return RocksDB.open(
                dbOptions,
                path,
                descriptors,
                handles
        );
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
    public boolean putEntry(Entry entry)
    {
        try
        {
            // Get rowId for Entry
            mainIndex.getRowId(entry);
            // Extract key and rowId from Entry object
            IndexProto.IndexKey key = entry.getKey();
            long rowId = entry.getRowId();
            boolean unique = entry.getIsUnique();
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Convert rowId to byte array
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            if (unique)
            {
                // Write to RocksDB
                rocksDB.put(keyBytes, valueBytes);
            }
            else
            {
                // Create composite key
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                // Store in RocksDB
                rocksDB.put(nonUniqueKey, null);
            }
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to put Entry: {} by entry", entry, e);
            return false;
        }
    }

    @Override
    public boolean putEntries(List<Entry> entries)
    {
        // Get rowIds for Entries
        mainIndex.getRgOfRowIds(entries);
        try
        {
            // Process each Entry object
            for (Entry entry : entries)
            {
                // Extract key and rowId from Entry object
                IndexProto.IndexKey key = entry.getKey();
                long rowId = entry.getRowId();
                boolean unique = entry.getIsUnique();
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Convert rowId to byte array
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                if(unique)
                {
                    // Write to RocksDB
                    rocksDB.put(keyBytes, valueBytes);
                }
                else
                {
                    byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                    rocksDB.put(nonUniqueKey, null);
                }
            }
            return true; // All entries written successfully
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to put Entries: {} by entries", entries, e);
            return false; // Operation failed
        }
    }

    @Override
    public boolean deleteEntry(IndexProto.IndexKey key)
    {
        try
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);

            // Delete key-value pair from RocksDB
            rocksDB.delete(keyBytes);
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to delete Entry: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean deleteEntries(List<IndexProto.IndexKey> keys)
    {
        try
        {
            for(IndexProto.IndexKey key : keys)
            {
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);

                // Delete key-value pair from RocksDB
                rocksDB.delete(keyBytes);
            }
            return true;
        }
        catch (RocksDBException e)
        {
            LOGGER.error("Failed to delete Entries: {}", keys, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
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
