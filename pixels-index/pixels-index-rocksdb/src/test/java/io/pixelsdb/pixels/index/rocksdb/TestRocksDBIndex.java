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
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestRocksDBIndex
{
    private RocksDB rocksDB;
    private final String rocksDBpath = "/tmp/rocksdb";
    private final long tableId = 100L;
    private final long indexId = 100L;
    private SinglePointIndex rocksDBIndex; // Class under test

    @BeforeEach
    public void setUp() throws RocksDBException
    {
        // Create RocksDB Directory
        try
        {
            FileUtils.forceMkdir(new File(rocksDBpath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to create RocksDB test directory: " + e.getMessage());
        }

        // Create SQLite Directory for main index
        try
        {
            String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
            FileUtils.forceMkdir(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to create SQLite test directory: " + e.getMessage());
        }
        System.out.println("Debug: Creating RocksDBIndex.."); // Debug log
        Options options = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(options, rocksDBpath);
        rocksDBIndex = new RocksDBIndex(tableId, indexId, rocksDB, rocksDBpath, true);
        System.out.println("Debug: RocksDBIndex instance: " + rocksDBIndex); // Check for null
        assertNotNull(rocksDBIndex);
    }

    @Test
    public void testPutEntry() throws RocksDBException, SinglePointIndexException
    {
        // Create Entry
        byte[] key = "exampleKey".getBytes();
        long timestamp = System.currentTimeMillis();
        long rowId = 100L;

        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId).setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

        byte[] keyBytes = toByteArray(keyProto);
        boolean success = rocksDBIndex.putEntry(keyProto, rowId);
        assertTrue(success, "putEntry should return true");

        // Assert index has been written to rocksDB
        byte[] storedValue = rocksDB.get(keyBytes);
        assertNotNull(storedValue);

        long storedRowId = ByteBuffer.wrap(storedValue).getLong();
        assertEquals(rowId, storedRowId);
    }

    @Test
    public void testPutEntries() throws RocksDBException, SinglePointIndexException, MainIndexException
    {
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();

        // Create two entries
        for (int i = 0; i < 2; i++)
        {
            byte[] key = ("exampleKey" + i).getBytes(); // use different keys

            long rowId = i*1000L;

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId).setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId).setRgId(rgId).setRgRowOffset(i).build();

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(keyProto).setRowId(rowId).setRowLocation(rowLocation).build();
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
    public void testGetUniqueRowId() throws MainIndexException, SinglePointIndexException
    {
        long indexId = 1L;
        byte[] key = "multiKey".getBytes();
        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = timestamp1 + 1000; // newer

        long rowId1 = 111L;
        long rowId2 = 222L; // expected

        IndexProto.IndexKey key1 = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp1)
                .build();

        rocksDBIndex.putEntry(key1, rowId1);

        IndexProto.IndexKey key2 = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp2)
                .build();

        rocksDBIndex.putEntry(key2,rowId2);

        long result = rocksDBIndex.getUniqueRowId(key2);
        assertEquals(rowId2, result, "getUniqueRowId should return the rowId of the latest timestamp entry");
    }

//    @Test
//    public void testGetRowIds() throws MainIndexException, SinglePointIndexException
//    {
//        long indexId = 1L;
//        byte[] key = "multiKey".getBytes();
//        long timestamp1 = System.currentTimeMillis();
//        long timestamp2 = timestamp1 + 1000; // newer
//
//        long rowId1 = 111L;
//        long rowId2 = 222L; // expected
//        List<Long> rowIds = new ArrayList<>();
//        rowIds.add(rowId1);
//        rowIds.add(rowId2);
//
//        IndexProto.IndexKey key1 = IndexProto.IndexKey.newBuilder()
//                .setIndexId(indexId)
//                .setKey(ByteString.copyFrom(key))
//                .setTimestamp(timestamp1)
//                .build();
//
//        rocksDBIndex.putEntry(key1, rowId1);
//
//        IndexProto.IndexKey key2 = IndexProto.IndexKey.newBuilder()
//                .setIndexId(indexId)
//                .setKey(ByteString.copyFrom(key))
//                .setTimestamp(timestamp2)
//                .build();
//
//        rocksDBIndex.putEntry(key2,rowId2);
//
//        List<Long> result = rocksDBIndex.getRowIds(key2);
//        assertEquals(rowIds, result, "getRowIds should return the rowId of all entry");
//    }

    @Test
    public void testDeleteEntry() throws RocksDBException, SinglePointIndexException
    {
        byte[] key = "exampleKey".getBytes();
        long timestamp = System.currentTimeMillis();

        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

        rocksDBIndex.putEntry(keyProto, 0L);
        // Delete index
        List<Long> rets = rocksDBIndex.deleteEntry(keyProto);
        // Assert return value
        for(long ret : rets)
        {
            assertTrue(ret >= 0, "deleteEntry should return true");
        }
    }

    @Test
    public void testDeleteEntries() throws RocksDBException, SinglePointIndexException, MainIndexException
    {
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();
        List<IndexProto.IndexKey> keyList = new ArrayList<>();

        for (int i = 0; i < 2; i++)
        {
            byte[] key = ("exampleKey" + i).getBytes();

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

            keyList.add(keyProto);

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId).setRgId(rgId).setRgRowOffset(i).build();

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(keyProto).setRowId(i).setRowLocation(rowLocation).build();
            entries.add(entry);
        }

        rocksDBIndex.putPrimaryEntries(entries);

        // delete Indexes
        List<Long> ret = rocksDBIndex.deleteEntries(keyList);
        assertNotNull(ret, "deleteEntries should return true");
    }

    @Test
    public void testUpdatePrimaryEntry() throws Exception
    {
        byte[] key = "updateKey".getBytes();
        long timestamp = System.currentTimeMillis();
        IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();
        long initialRowId = 100L;
        long updatedRowId = 200L;

        // Put initial entry
        rocksDBIndex.putEntry(indexKey, initialRowId);

        // Update entry
        long prevRowId = rocksDBIndex.updatePrimaryEntry(indexKey, updatedRowId);
        assertEquals(initialRowId, prevRowId, "Previous rowId should match the one inserted before");

        // Verify the updated value
        long actualRowId = rocksDBIndex.getUniqueRowId(indexKey);
        assertEquals(updatedRowId, actualRowId, "RowId should be updated correctly");
    }

    @Test
    public void testUpdateSecondaryEntry() throws Exception
    {
        byte[] key = "updateKey".getBytes();
        long timestamp = System.currentTimeMillis();
        IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();
        long initialRowId = 500L;
        long updatedRowId = 600L;

        // Put initial entry
        rocksDBIndex.putEntry(indexKey, initialRowId);

        // Update entry
        List<Long> prevRowIds = rocksDBIndex.updateSecondaryEntry(indexKey, updatedRowId);
        assertEquals(1, prevRowIds.size());
        assertEquals(initialRowId, prevRowIds.get(0));

        // Verify the new value
        long actualRowId = rocksDBIndex.getUniqueRowId(indexKey);
        assertEquals(updatedRowId, actualRowId);
    }

    @Disabled("Performance test, run manually")
    @Test
    public void benchmarkPutEntry() throws RocksDBException, SinglePointIndexException
    {
        int count = 1_000_000;
        byte[] key = "putBenchmarkKey".getBytes();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++)
        {
            long timestamp = System.currentTimeMillis();
            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key))
                    .setTimestamp(timestamp)
                    .build();

            rocksDBIndex.putEntry(keyProto, i);
        }
        long end = System.nanoTime();
        double durationMs = (end - start) / 1_000_000.0;
        System.out.printf("Put %,d entries in %.2f ms (%.2f ops/sec)%n", count, durationMs, count * 1000.0 / durationMs);
    }

    @Disabled("Performance test, run manually")
    @Test
    public void benchmarkDeleteEntry() throws RocksDBException, SinglePointIndexException
    {
        int count = 1_000_000;
        byte[] key = "deleteBenchmarkKey".getBytes();

        ImmutableList.Builder<IndexProto.IndexKey> builder = ImmutableList.builder();

        // prepare data
        for (int i = 0; i < count; i++)
        {
            long timestamp = System.currentTimeMillis();

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key))
                    .setTimestamp(timestamp)
                    .build();

            rocksDBIndex.putEntry(keyProto, i);
            builder.add(keyProto);
        }

        // delete data
        long start = System.nanoTime();
        for (IndexProto.IndexKey keyProto : builder.build())
        {
            rocksDBIndex.deleteEntry(keyProto);
        }
        long end = System.nanoTime();
        double durationMs = (end - start) / 1_000_000.0;
        System.out.printf("Deleted %,d entries in %.2f ms (%.2f ops/sec)%n", count, durationMs, count * 1000.0 / durationMs);
    }

    @AfterEach
    public void tearDown() throws MainIndexException
    {
        if (rocksDB != null)
        {
            rocksDB.close();
            MainIndexFactory.Instance().closeAll();
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

        // Clear SQLite Directory for main index
        try
        {
            String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
            FileUtils.deleteDirectory(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to clean up SQLite test directory: " + e.getMessage());
        }
    }

    private static void writeLongBE(byte[] buf, int offset, long value)
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

    // Convert IndexKey to byte array
    private static byte[] toByteArray(IndexProto.IndexKey key)
    {
        byte[] keyBytes = key.getKey().toByteArray();
        int totalLength = Long.BYTES + keyBytes.length + Long.BYTES;

        byte[] compositeKey = new byte[totalLength];
        int pos = 0;

        // Write indexId (8 bytes, big endian)
        long indexId = key.getIndexId();
        writeLongBE(compositeKey, pos, indexId);
        pos += 8;
        // Write key bytes (variable length)
        System.arraycopy(keyBytes, 0, compositeKey, pos, keyBytes.length);
        pos += keyBytes.length;
        // Write timestamp (8 bytes, big endian)
        long timestamp = key.getTimestamp();
        writeLongBE(compositeKey, pos, timestamp);

        return compositeKey;
    }
}