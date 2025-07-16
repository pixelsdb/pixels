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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestRocksDBIndex
{
    private RocksDB rocksDB;
    private final String rocksDBpath = "/tmp/rocksdb";
    private final long tableId = 100L;
    private SinglePointIndex rocksDBIndex; // Class under test

    @BeforeEach
    public void setUp() throws RocksDBException, IOException
    {
        System.out.println("Debug: Creating RocksDBIndex.."); // Debug log
        Options options = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(options, rocksDBpath);
        rocksDBIndex = new RocksDBIndex(1L, 1L, rocksDB);
        System.out.println("Debug: RocksDBIndex instance: " + rocksDBIndex); // Check for null
        assertNotNull(rocksDBIndex);
    }

    @Test
    public void testPutEntry() throws RocksDBException, MainIndexException, SinglePointIndexException
    {
        // Create Entry
        long indexId = 1L;
        byte[] key = "exampleKey".getBytes();
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;
        int rgRowId = 3;
        long rowId = 100L;

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId).put((byte) ':').put(key).put((byte) ':').putLong(timestamp);
        byte[] keyBytes = buffer.array();

        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                .setFileId(fileId)
                .setRgId(rgId)
                .setRgRowId(rgRowId)
                .build();

        boolean success = rocksDBIndex.putEntry(keyProto, rowId, true);
        assertTrue(success, "putEntry should return true");

        // Assert index has been written to rocksDB
        byte[] storedValue = rocksDB.get(keyBytes);
        assertNotNull(storedValue);

        long storedRowId = ByteBuffer.wrap(storedValue).getLong();
        assertEquals(rowId, storedRowId);
    }

    @Test
    public void testPutEntries() throws RocksDBException, SinglePointIndexException
    {
        long indexId = 1L;
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();

        // Create two entries
        for (int i = 0; i < 2; i++)
        {
            byte[] key = ("exampleKey" + i).getBytes(); // 不同 key
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
            buffer.putLong(indexId).put((byte) ':').put(key).put((byte) ':').putLong(timestamp);

            long rowId = i*1000L;

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key))
                    .setTimestamp(timestamp)
                    .build();

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId)
                    .setRgId(rgId)
                    .setRgRowId(i)
                    .build();

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(keyProto).setTableRowId(rowId).setRowLocation(rowLocation).build();
            entries.add(entry);
        }

        boolean success = rocksDBIndex.putPrimaryEntries(entries);
        assertTrue(success, "putEntries should return true");

        // Assert every index has been written to rocksDB
        for (int i = 0; i < entries.size(); i++)
        {
            IndexProto.PrimaryIndexEntry entry = entries.get(i);
            byte[] keyBytes = toByteArray(entry.getIndexKey());
            byte[] storedValue = rocksDB.get(keyBytes);
            assertNotNull(storedValue);
            long storedRowId = ByteBuffer.wrap(storedValue).getLong();
            assertEquals(i* 1000L, storedRowId);
        }
    }

    @Test
    public void testDeleteEntry() throws RocksDBException, SinglePointIndexException
    {
        long indexId = 1L;
        byte[] key = "exampleKey".getBytes();
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;
        int rgRowId = 3;

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId).put((byte) ':').put(key).put((byte) ':').putLong(timestamp);
        byte[] keyBytes = buffer.array();

        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                .setFileId(fileId)
                .setRgId(rgId)
                .setRgRowId(rgRowId)
                .build();

        rocksDBIndex.putEntry(keyProto, 0L, true);

        // Delete index
        long ret = rocksDBIndex.deleteEntry(keyProto);

        // Assert return value
        assertTrue(ret >= 0, "deleteEntry should return true");

        // Assert index has been deleted
        byte[] result = rocksDB.get(keyBytes);
        assertNull(result, "Key should be deleted from RocksDB");
    }

    @Test
    public void testDeleteEntries() throws RocksDBException, SinglePointIndexException
    {
        long indexId = 1L;
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();
        List<byte[]> keyBytesList = new ArrayList<>();
        List<IndexProto.IndexKey> keyList = new ArrayList<>();

        for (int i = 0; i < 2; i++)
        {
            byte[] key = ("exampleKey" + i).getBytes();
            keyBytesList.add(key);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
            buffer.putLong(indexId).put((byte) ':').put(key).put((byte) ':').putLong(timestamp);

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key))
                    .setTimestamp(timestamp)
                    .build();

            keyList.add(keyProto);

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId)
                    .setRgId(rgId)
                    .setRgRowId(i)
                    .build();

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(keyProto).setTableRowId(0L).setRowLocation(rowLocation).build();
            entries.add(entry);
        }

        rocksDBIndex.putPrimaryEntries(entries);

        // delete Indexes
        List<Long> ret = rocksDBIndex.deleteEntries(keyList);
        assertNotNull(ret, "deleteEntries should return true");

        // Assert all indexes have been deleted
        for (byte[] keyBytes : keyBytesList)
        {
            byte[] storedValue = rocksDB.get(keyBytes);
            assertNull(storedValue, "Key should be deleted from RocksDB");
        }
    }

    @AfterEach
    public void tearDown()
    {
        if (rocksDB != null)
        {
            rocksDB.close();
        }

        // Clear RocksDB Directory
        try
        {
            FileUtils.deleteDirectory(new File(rocksDBpath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to clean up RocksDB test directory: " + e.getMessage());
        }
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
}