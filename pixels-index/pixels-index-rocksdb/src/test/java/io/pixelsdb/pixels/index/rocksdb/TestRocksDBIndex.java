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
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexImpl;
import java.io.File;
import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestRocksDBIndex
{
    private RocksDB rocksDB;
    private final String rocksDBpath = "/tmp/rocksdb";
    private SecondaryIndex rocksDBIndex; // Class under test
    private final MainIndex mainIndex = new MainIndexImpl();
    @BeforeEach
    public void setUp() throws RocksDBException, IOException
    {
        System.out.println("Debug: Creating RocksDBIndex.."); // Debug log
        Options options = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(options, rocksDBpath);
        rocksDBIndex = new RocksDBIndex(rocksDB,mainIndex);
        System.out.println("Debug: RocksDBIndex instance: " + rocksDBIndex); // Check for null
        assertNotNull(rocksDBIndex);
    }

    @Test
    public void testPutEntry() throws RocksDBException {
        // Create Entry
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

        SecondaryIndex.Entry entry = new SecondaryIndex.Entry(keyProto, 0L, true, rowLocation);

        long rowId = rocksDBIndex.putEntry(entry);

        // Assert index has been written to rocksDB
        byte[] storedValue = rocksDB.get(keyBytes);
        assertNotNull(storedValue);

        long storedRowId = ByteBuffer.wrap(storedValue).getLong();
        assertEquals(rowId, storedRowId);
    }

    @Test
    public void testPutEntries() throws RocksDBException {
        long indexId = 1L;
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<SecondaryIndex.Entry> entries = new ArrayList<>();

        // Create two entries
        for (int i = 0; i < 2; i++) {
            byte[] key = ("exampleKey" + i).getBytes(); // 不同 key
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
            buffer.putLong(indexId).put((byte) ':').put(key).put((byte) ':').putLong(timestamp);

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

            SecondaryIndex.Entry entry = new SecondaryIndex.Entry(keyProto, 0L, true, rowLocation);
            entries.add(entry);
        }

        List<Long> rowIds = rocksDBIndex.putEntries(entries);

        // Assert rowId num is correct
        assertEquals(2, rowIds.size());

        // Assert every index has been written to rocksDB
        for (int i = 0; i < entries.size(); i++) {
            SecondaryIndex.Entry entry = entries.get(i);
            byte[] keyBytes = toByteArray(entry.getKey());
            byte[] storedValue = rocksDB.get(keyBytes);
            assertNotNull(storedValue);
            long storedRowId = ByteBuffer.wrap(storedValue).getLong();
            assertEquals(rowIds.get(i).longValue(), storedRowId);
        }
    }

    @Test
    public void testDeleteEntry() throws RocksDBException, IOException {
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

        SecondaryIndex.Entry entry = new SecondaryIndex.Entry(keyProto, 0L, true, rowLocation);

        rocksDBIndex.putEntry(entry);

        // Delete index
        boolean success = rocksDBIndex.deleteEntry(keyProto);

        // Assert return value
        assertTrue(success, "deleteEntry should return true");

        // Assert index has been deleted
        byte[] result = rocksDB.get(keyBytes);
        assertNull(result, "Key should be deleted from RocksDB");
    }

    @Test
    public void testDeleteEntries() throws RocksDBException, IOException {
        long indexId = 1L;
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<SecondaryIndex.Entry> entries = new ArrayList<>();
        List<byte[]> keyBytesList = new ArrayList<>();
        List<IndexProto.IndexKey> keyList = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            byte[] key = ("exampleKey" + i).getBytes(); // 不同 key
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

            SecondaryIndex.Entry entry = new SecondaryIndex.Entry(keyProto, 0L, true, rowLocation);
            entries.add(entry);
        }

        List<Long> rowIds = rocksDBIndex.putEntries(entries);

        // delete Indexes
        boolean success = rocksDBIndex.deleteEntries(keyList);
        assertTrue(success, "deleteEntries should return true");

        // Assert all indexes have been deleted
        for (byte[] keyBytes : keyBytesList) {
            byte[] storedValue = rocksDB.get(keyBytes);
            assertNull(storedValue, "Key should be deleted from RocksDB");
        }
    }

    @AfterEach
    public void tearDown() {
        if (rocksDB != null) {
            rocksDB.close();
        }

        // Clear RocksDB Directory
        try {
            FileUtils.deleteDirectory(new File(rocksDBpath));
        } catch (IOException e) {
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