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
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.index.rocksdb.RocksDBIndex.toKeyBuffer;
import static org.junit.jupiter.api.Assertions.*;

public class TestRocksDBIndex
{
    private RocksDB rocksDB;
    private String rocksDBpath;
    private final long tableId = 100L;
    private final long indexId = 100L;
    private SinglePointIndex uniqueIndex;
    private SinglePointIndex nonUniqueIndex;

    @BeforeEach
    public void setUp() throws RocksDBException
    {
        rocksDB = RocksDBFactory.getRocksDB();
        rocksDBpath = RocksDBFactory.getDbPath();

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
        uniqueIndex = new RocksDBIndex(tableId, indexId, true);
        nonUniqueIndex = new RocksDBIndex(tableId, indexId + 1, false);

        System.out.println("Debug: uniqueRocksDBIndex instance: " + uniqueIndex); // Check for null
        System.out.println("Debug: nonUniqueRocksDBIndex instance: " + uniqueIndex); // Check for null

        assertNotNull(uniqueIndex);
        assertNotNull(nonUniqueIndex);
    }

    @Test
    public void testPutEntry() throws RocksDBException, SinglePointIndexException
    {
        // Create Entry
        byte[] key = "testPutEntry".getBytes();
        long timestamp = 1000L;
        long rowId = 100L;

        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId).setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

        boolean success = uniqueIndex.putEntry(keyProto, rowId);
        assertTrue(success, "putEntry should return true");

        ByteBuffer keyBuffer = toKeyBuffer(keyProto);
        ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
        ReadOptions readOptions = new ReadOptions();
        // Assert index has been written to rocksDB
        int ret = rocksDB.get(readOptions, keyBuffer, valueBuffer);
        assertTrue(ret != RocksDB.NOT_FOUND);

        long storedRowId = valueBuffer.getLong();
        assertEquals(rowId, storedRowId);
    }

    @Test
    public void testPutEntries() throws RocksDBException, SinglePointIndexException, MainIndexException
    {
        long timestamp = 1000L;
        long fileId = 1L;
        int rgId = 2;

        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();

        // Create two entries
        for (int i = 0; i < 2; i++)
        {
            byte[] key = ("testPutEntries" + i).getBytes(); // use different keys

            long rowId = i * 1000L;

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId).setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId).setRgId(rgId).setRgRowOffset(i).build();

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(keyProto).setRowId(rowId).setRowLocation(rowLocation).build();
            entries.add(entry);
        }

        boolean success = uniqueIndex.putPrimaryEntries(entries);
        assertTrue(success, "putEntries should return true");

        // Assert every index has been written to rocksDB
        for (int i = 0; i < entries.size(); i++)
        {
            IndexProto.PrimaryIndexEntry entry = entries.get(i);
            ByteBuffer keyBuffer = toKeyBuffer(entry.getIndexKey());
            ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
            ReadOptions readOptions = new ReadOptions();
            int ret = rocksDB.get(readOptions, keyBuffer, valueBuffer);
            assertTrue(ret != RocksDB.NOT_FOUND);
            long storedRowId = valueBuffer.getLong();
            assertEquals(i* 1000L, storedRowId);
        }
    }

    @Test
    public void testGetUniqueRowId() throws  SinglePointIndexException
    {
        byte[] key = "testGetUniqueRowId".getBytes();
        long timestamp1 = 1000L;
        long timestamp2 = timestamp1 + 1000; // newer

        long rowId1 = 111L;
        long rowId2 = 222L; // expected

        IndexProto.IndexKey key1 = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp1)
                .build();

        uniqueIndex.putEntry(key1, rowId1);

        IndexProto.IndexKey key2 = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp2)
                .build();

        uniqueIndex.putEntry(key2, rowId2);

        long result = uniqueIndex.getUniqueRowId(key2);
        assertEquals(rowId2, result, "getUniqueRowId should return the rowId of the latest timestamp entry");
    }

    @Test
    public void testGetRowIds() throws SinglePointIndexException
    {
        byte[] key = "testGetRowIds".getBytes();
        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = timestamp1 + 1000; // newer

        long rowId1 = 111L;
        long rowId2 = 222L; // expected
        List<Long> rowIds = new ArrayList<>();
        rowIds.add(rowId2);
        rowIds.add(rowId1);

        IndexProto.IndexKey key1 = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp1)
                .build();

        nonUniqueIndex.putEntry(key1, rowId1);

        IndexProto.IndexKey key2 = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp2)
                .build();

        nonUniqueIndex.putEntry(key2,rowId2);

        List<Long> result = nonUniqueIndex.getRowIds(key2);
        System.out.println(result.size());
        System.out.println(result.toString());
        assertTrue(rowIds.containsAll(result) && result.containsAll(rowIds), "getRowIds should return the rowId of all entries");
    }

    @Test
    public void testDeleteEntry() throws SinglePointIndexException
    {
        byte[] key = "testDeleteEntry".getBytes();
        long timestamp = System.currentTimeMillis();

        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

        uniqueIndex.putEntry(keyProto, 0L);
        // Delete index
        List<Long> rets = uniqueIndex.deleteEntry(keyProto);
        // Assert return value
        for(long ret : rets)
        {
            assertTrue(ret >= 0, "deleteEntry should return true");
        }
    }

    @Test
    public void testDeleteEntries() throws SinglePointIndexException, MainIndexException
    {
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;

        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();
        List<IndexProto.IndexKey> keyList = new ArrayList<>();

        for (int i = 0; i < 2; i++)
        {
            byte[] key = ("testDeleteEntries" + i).getBytes();

            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();

            keyList.add(keyProto);

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId).setRgId(rgId).setRgRowOffset(i).build();

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(keyProto).setRowId(i).setRowLocation(rowLocation).build();
            entries.add(entry);
        }

        uniqueIndex.putPrimaryEntries(entries);

        // delete Indexes
        List<Long> ret = uniqueIndex.deleteEntries(keyList);
        assertNotNull(ret, "deleteEntries should return true");
    }

    @Test
    public void testUpdatePrimaryEntry() throws Exception
    {
        byte[] key = "testUpdatePrimaryEntry".getBytes();
        long timestamp = System.currentTimeMillis();
        IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();
        long initialRowId = 100L;
        long updatedRowId = 200L;

        // Put initial entry
        uniqueIndex.putEntry(indexKey, initialRowId);

        // Update entry
        long prevRowId = uniqueIndex.updatePrimaryEntry(indexKey, updatedRowId);
        assertEquals(initialRowId, prevRowId, "Previous rowId should match the one inserted before");

        // Verify the updated value
        long actualRowId = uniqueIndex.getUniqueRowId(indexKey);
        assertEquals(updatedRowId, actualRowId, "RowId should be updated correctly");
    }

    @Test
    public void testUpdateSecondaryEntry() throws Exception
    {
        byte[] key = "testUpdateSecondaryEntry".getBytes();
        long timestamp = System.currentTimeMillis();
        IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder().setIndexId(indexId)
                .setKey(ByteString.copyFrom(key)).setTimestamp(timestamp).build();
        long initialRowId = 500L;
        long updatedRowId = 600L;

        // Put initial entry
        uniqueIndex.putEntry(indexKey, initialRowId);

        // Update entry
        List<Long> prevRowIds = uniqueIndex.updateSecondaryEntry(indexKey, updatedRowId);
        assertEquals(1, prevRowIds.size());
        assertEquals(initialRowId, prevRowIds.get(0));

        // Verify the new value
        long actualRowId = uniqueIndex.getUniqueRowId(indexKey);
        assertEquals(updatedRowId, actualRowId);
    }

    @Disabled("Performance test, run manually")
    @Test
    public void benchmarkGetUniqueRowId() throws SinglePointIndexException
    {
        int count = 1_000_000;
        byte[] key = "benchmarkGetUniqueRowId".getBytes();
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

            uniqueIndex.putEntry(keyProto, i);
            builder.add(keyProto);
        }
        // get rowId
        long start = System.nanoTime();
        for (IndexProto.IndexKey keyProto : builder.build())
        {
            uniqueIndex.getUniqueRowId(keyProto);
        }
        long end = System.nanoTime();
        double durationMs = (end - start) / 1_000_000.0;
        System.out.printf("Get %,d unique rowIds in %.2f ms (%.2f ops/sec)%n", count, durationMs, count * 1000.0 / durationMs);
    }

    @Disabled("Performance test, run manually")
    @Test
    public void benchmarkPutEntry() throws SinglePointIndexException
    {
        int count = 1_000_000;
        byte[] key = "benchmarkPutEntry".getBytes();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++)
        {
            long timestamp = System.currentTimeMillis();
            IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                    .setIndexId(indexId)
                    .setKey(ByteString.copyFrom(key))
                    .setTimestamp(timestamp)
                    .build();

            uniqueIndex.putEntry(keyProto, i);
        }
        long end = System.nanoTime();
        double durationMs = (end - start) / 1_000_000.0;
        System.out.printf("Put %,d entries in %.2f ms (%.2f ops/sec)%n", count, durationMs, count * 1000.0 / durationMs);
    }

    @Disabled("Performance test, run manually")
    @Test
    public void benchmarkDeleteEntry() throws SinglePointIndexException
    {
        int count = 1_000_000;
        byte[] key = "benchmarkDeleteEntry".getBytes();

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

            uniqueIndex.putEntry(keyProto, i);
            builder.add(keyProto);
        }

        // delete data
        long start = System.nanoTime();
        for (IndexProto.IndexKey keyProto : builder.build())
        {
            uniqueIndex.deleteEntry(keyProto);
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
            RocksDBFactory.close();
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
}