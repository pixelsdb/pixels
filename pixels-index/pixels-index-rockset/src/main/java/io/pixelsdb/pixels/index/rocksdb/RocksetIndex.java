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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class RocksetIndex implements SinglePointIndex
{
    // load RocksetJni
    static
    {
        System.loadLibrary("RocksetJni");
    }

    // Native method
    private native long CreateCloudFileSystem0(
            String bucketName,
            String s3Prefix);

    private native long OpenDBCloud0(
            long cloudEnvPtr,
            String localDbPath,
            String persistentCachePath,
            long persistentCacheSizeGB,
            boolean readOnly);

    private native void DBput0(long dbHandle, byte[] key, byte[] value);

    private native byte[] DBget0(long dbHandle, byte[] key);

    private native void DBdelete0(long dbHandle, byte[] key);

    private native void CloseDB0(long dbHandle);

    private long CreateDBCloud(
            String bucketName,
            String s3Prefix,
            String localDbPath,
            String persistentCachePath,
            long persistentCacheSizeGB,
            boolean readOnly)
    {
        long cloudEnvPtr = CreateCloudFileSystem0(bucketName, s3Prefix);
        if (cloudEnvPtr == 0)
        {
            throw new RuntimeException("Failed to create CloudFileSystem");
        }

        long dbHandle = OpenDBCloud0(cloudEnvPtr, localDbPath, persistentCachePath, persistentCacheSizeGB, readOnly);
        if (dbHandle == 0)
        {
            CloseDB0(0);
            throw new RuntimeException("Failed to open DBCloud");
        }

        return dbHandle;
    }

    private void DBput(long dbHandle, byte[] key, byte[] value)
    {
        DBput0(dbHandle, key, value);
    }

    private byte[] DBget(long dbHandle, byte[] key)
    {
        return DBget0(dbHandle, key);
    }

    private void DBdelete(long dbHandle, byte[] key)
    {
        DBdelete0(dbHandle, key);
    }

    private void CloseDB(long dbHandle)
    {
        if (dbHandle != 0)
        {
            CloseDB0(dbHandle);
        }
    }

    private long dbHandle = 0;
    public static final Logger LOGGER = LogManager.getLogger(RocksetIndex.class);
    private final MainIndex mainIndex;

    public RocksetIndex(MainIndex mainIndex, String bucketName, String s3Prefix, String localDbPath,
                        String persistentCachePath, long persistentCacheSizeGB, boolean readOnly)
    {
        this.dbHandle = CreateDBCloud(bucketName, s3Prefix, localDbPath,
                persistentCachePath, persistentCacheSizeGB, readOnly);
        this.mainIndex = mainIndex;
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key)
    {
        try
        {
            // Generate composite key
            byte[] compositeKey = toByteArray(key);

            // Get value from RocksDB
            byte[] valueBytes = DBget(this.dbHandle, compositeKey);

            if (valueBytes != null) {
                return ByteBuffer.wrap(valueBytes).getLong();
            } else {
                System.out.println("No value found for composite key: " + key);
            }
        } catch (RuntimeException e)
        {
            LOGGER.error("Failed to get unique row ID for key: {}", key, e);
        }
        // Return default value (0) if key doesn't exist or exception occurs
        return 0;
    }

    @Override
    public long[] getRowIds(IndexProto.IndexKey key)
    {
        return new long[0];
    }

    @Override
    public boolean putPrimaryEntry(Entry entry) throws MainIndexException, SinglePointIndexException
    {
        try
        {
            // Extract key and rowId from Entry object
            IndexProto.IndexKey key = entry.getKey();
            long rowId = entry.getRowId();
            boolean unique = entry.getIsUnique();
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Convert rowId to byte array
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            // Check if dbHandle is valid
            if (this.dbHandle == 0)
            {
                throw new IllegalStateException("RocksDB not initialized");
            }
            if (keyBytes.length == 0 || (unique && valueBytes.length == 0))
            {
                throw new IllegalArgumentException("Key/Value cannot be empty");
            }
            if (unique)
            {
                // Write to RocksDB
                DBput(this.dbHandle, keyBytes, valueBytes);
            }
            else
            {
                // Create composite key
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                // Store in RocksDB
                DBput(this.dbHandle, nonUniqueKey, null);
            }
            // Put rowId into MainIndex
            IndexProto.RowLocation rowLocation = entry.getRowLocation();
            boolean success = mainIndex.putRowId(rowId, rowLocation);
            if (!success) {
                LOGGER.error("Failed to put Entry into main index for rowId {}", rowId);
                throw new MainIndexException("Failed to put Entry into main index for rowId");
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to put Primary Entry: {} by entry", entry, e);
            throw new SinglePointIndexException("Failed to put Primary Entry",e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<Entry> entries) throws MainIndexException, SinglePointIndexException
    {
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
                if (unique)
                {
                    // Write to RocksDB
                    DBput(this.dbHandle, keyBytes, valueBytes);
                }
                else
                {
                    byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                    DBput(this.dbHandle, nonUniqueKey, null);
                }
            }
            // Select start rowId and end rowId
            Entry entryStart = entries.get(0);
            Entry entryEnd = entries.get(entries.size() - 1);
            long start = entryStart.getRowId();
            long end = entryEnd.getRowId();
            // Create new RowIdRange and RgLocation
            RowIdRange newRange = new RowIdRange(start, end);
            IndexProto.RowLocation rowLocation = entryStart.getRowLocation();
            MainIndex.RgLocation rgLocation = new MainIndex.RgLocation(rowLocation.getFileId(), rowLocation.getRgId());
            // Put RowIds to MainIndex
            boolean success = mainIndex.putRowIdsOfRg(newRange, rgLocation);
            if (!success) {
                LOGGER.error("Failed to put Entry into main index for rowId RowIdRange [{}-{}]", start, end);
                throw new MainIndexException("Failed to put Entry into main index for rowId RowIdRange");
            }
            return true; // All entries written successfully
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to put Primary Entries: {} by entries", entries, e);
            throw new SinglePointIndexException("Failed to put Primary Entries",e);
        }
    }

    @Override
    public boolean putSecondaryEntry(Entry entry) throws SinglePointIndexException
    {
        try
        {
            // Extract key and rowId from Entry object
            IndexProto.IndexKey key = entry.getKey();
            long rowId = entry.getRowId();
            boolean unique = entry.getIsUnique();
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Convert rowId to byte array
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            // Check if dbHandle is valid
            if (this.dbHandle == 0)
            {
                throw new IllegalStateException("RocksDB not initialized");
            }
            if (keyBytes.length == 0 || (unique && valueBytes.length == 0))
            {
                throw new IllegalArgumentException("Key/Value cannot be empty");
            }
            if (unique)
            {
                // Write to RocksDB
                DBput(this.dbHandle, keyBytes, valueBytes);
            }
            else
            {
                // Create composite key
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                // Store in RocksDB
                DBput(this.dbHandle, nonUniqueKey, null);
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to put Secondary Entry: {} by entry", entry, e);
            throw new SinglePointIndexException("Failed to put Secondary Entry",e);
        }
    }

    @Override
    public boolean putSecondaryEntries(List<Entry> entries) throws SinglePointIndexException
    {
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
                if (unique)
                {
                    // Write to RocksDB
                    DBput(this.dbHandle, keyBytes, valueBytes);
                }
                else
                {
                    byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                    DBput(this.dbHandle, nonUniqueKey, null);
                }
            }
            return true; // All entries written successfully
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to put Secondary Entries: {} by entries", entries, e);
            throw new SinglePointIndexException("Failed to put Secondary Entries",e);
        }
    }

    @Override
    public boolean deletePrimaryEntry(IndexProto.IndexKey key)
    {
        try
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Get RowId in order to delete MainIndex
            long rowId = getUniqueRowId(key);
            // Delete key-value pair from RocksDB
            DBdelete(this.dbHandle, keyBytes);
            // Delete MainIndex
            boolean success = mainIndex.deleteRowId(rowId);
            if (!success) {
                LOGGER.error("Failed to delete Entry of main index for rowId {}", rowId);
                return false;
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to delete Primary Entry: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean deleteEntries(List<IndexProto.IndexKey> keys)
    {
        try
        {
            List<Long> rowIds = new ArrayList<>();
            for (IndexProto.IndexKey key : keys)
            {
                // Get rowId
                long rowId = getUniqueRowId(key);
                rowIds.add(rowId);
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Delete key-value pair from RocksDB
                DBdelete(this.dbHandle, keyBytes);
            }
            if (rowIds.isEmpty()) {
                LOGGER.warn("No rowIds found for keys: {}", keys);
                return false;
            }
            // Found start rowId and end rowId
            long start = Collections.min(rowIds);
            long end = Collections.max(rowIds);
            RowIdRange newRange = new RowIdRange(start, end);
            // Delete MainIndex
            boolean success = mainIndex.deleteRowIdRange(newRange);
            if (!success) {
                LOGGER.error("Failed to delete Entry of main index for rowId RowIdRange [{}-{}]", start, end);
                return false;
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to delete Primary Entries: {}", keys, e);
            return false;
        }
    }

    @Override
    public boolean deleteSecondaryEntry(IndexProto.IndexKey key)
    {
        try
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Delete key-value pair from RocksDB
            DBdelete(this.dbHandle, keyBytes);
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to delete Secondary Entry: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean deleteSecondaryEntries(List<IndexProto.IndexKey> keys)
    {
        try
        {
            for (IndexProto.IndexKey key : keys)
            {
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Delete key-value pair from RocksDB
                DBdelete(this.dbHandle, keyBytes);
            }
            if (rowIds.isEmpty()) {
                LOGGER.warn("No rowIds found for keys: {}", keys);
                return false;
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to delete Secondary Entries: {}", keys, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        mainIndex.close();
        CloseDB(this.dbHandle); // Close RocksDB instance
    }

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