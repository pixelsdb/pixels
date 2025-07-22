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
package io.pixelsdb.pixels.index.rockset;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class RocksetIndex implements SinglePointIndex
{
    // load pixels-index-rockset
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty())
        {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        String libPath = Paths.get(pixelsHome, "lib/libpixels-index-rockset.so").toString();
        File libFile = new File(libPath);
        if (!libFile.exists())
        {
            throw new IllegalStateException("libpixels-index-rockset.so not found at " + libPath);
        }
        if (!libFile.canRead())
        {
            throw new IllegalStateException("libpixels-index-rockset.so is not readable at " + libPath);
        }
        System.load(libPath);
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

    protected long CreateDBCloud(@Nonnull CloudDBOptions dbOptions)
    {
        long cloudEnvPtr = CreateCloudFileSystem0(dbOptions.getBucketName(), dbOptions.getS3Prefix());
        if (cloudEnvPtr == 0)
        {
            throw new RuntimeException("Failed to create CloudFileSystem");
        }

        long dbHandle = OpenDBCloud0(cloudEnvPtr, dbOptions.getLocalDbPath(), dbOptions.getPersistentCachePath(),
                dbOptions.getPersistentCacheSizeGB(), dbOptions.isReadOnly());
        if (dbHandle == 0)
        {
            CloseDB0(0);
            throw new RuntimeException("Failed to open DBCloud");
        }

        return dbHandle;
    }

    protected void DBput(long dbHandle, byte[] key, byte[] value)
    {
        DBput0(dbHandle, key, value);
    }

    protected byte[] DBget(long dbHandle, byte[] key)
    {
        return DBget0(dbHandle, key);
    }

    protected void DBdelete(long dbHandle, byte[] key)
    {
        DBdelete0(dbHandle, key);
    }

    protected void CloseDB(long dbHandle)
    {
        if (dbHandle != 0)
        {
            CloseDB0(dbHandle);
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(RocksetIndex.class);

    private long dbHandle = 0;
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private boolean closed = false;

    protected RocksetIndex(long tableId, long indexId, CloudDBOptions dbOptions, boolean unique)
    {
        this.dbHandle = CreateDBCloud(dbOptions);
        this.tableId = tableId;
        this.indexId = indexId;
        this.unique = unique;
    }

    protected long getDbHandle()
    {
        return dbHandle;
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
        try
        {
            // Generate composite key
            byte[] compositeKey = toByteArray(key);

            // Get value from Rockset
            byte[] valueBytes = DBget(this.dbHandle, compositeKey);

            if (valueBytes != null)
            {
                return ByteBuffer.wrap(valueBytes).getLong();
            }
            else
            {
                System.out.println("No value found for composite key: " + key);
            }
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to get unique row ID for key: {}", key, e);
        }
        // Return default value (0) if key doesn't exist or exception occurs
        return 0;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key)
    {
        return ImmutableList.of();
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Convert rowId to byte array
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            // Check if dbHandle is valid
            if (this.dbHandle == 0)
            {
                throw new IllegalStateException("Rockset not initialized");
            }
            if (keyBytes.length == 0 || (unique && valueBytes.length == 0))
            {
                throw new IllegalArgumentException("Key/Value cannot be empty");
            }
            if (unique)
            {
                // Write to Rockset
                DBput(this.dbHandle, keyBytes, valueBytes);
            }
            else
            {
                // Create composite key
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                // Store in Rockset
                DBput(this.dbHandle, nonUniqueKey, null);
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("failed to put rockset index entry", e);
            throw new SinglePointIndexException("failed to put rockset index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        try
        {
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

                // Write to Rockset
                DBput(this.dbHandle, keyBytes, valueBytes);
            }
            return true; // All entries written successfully
        }
        catch (RuntimeException e)
        {
            LOGGER.error("failed to put rockset index entries", e);
            throw new SinglePointIndexException("failed to put rockset index entries", e);
        }
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try
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
                if (unique)
                {
                    // Write to Rockset
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
            LOGGER.error("failed to put rockset index entries", e);
            throw new SinglePointIndexException("failed to put rockset index entries", e);
        }
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey key)
    {
        try
        {
            // Convert IndexKey to byte array
            byte[] keyBytes = toByteArray(key);
            // Delete key-value pair from Rockset
            DBdelete(this.dbHandle, keyBytes);
            return 0; // TODO: implement
        }
        catch (RuntimeException e)
        {
            LOGGER.error("failed to delete rockset index entry", e);
            return 0; // TODO: implement
        }
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey indexKey) throws SinglePointIndexException
    {
        return Collections.emptyList();
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys)
    {
        try
        {
            for (IndexProto.IndexKey key : keys)
            {
                // Convert IndexKey to byte array
                byte[] keyBytes = toByteArray(key);
                // Delete key-value pair from Rockset
                DBdelete(this.dbHandle, keyBytes);
            }
            return null; // TODO: implement
        }
        catch (RuntimeException e)
        {
            LOGGER.error("failed to delete rockset index entries", e);
            return null; // TODO: implement
        }
    }

    @Override
    public void close() throws IOException
    {
        if (!closed)
        {
            closed = true;
            CloseDB(this.dbHandle); // Close Rockset instance
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        // TODO: implement
        return false;
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
}