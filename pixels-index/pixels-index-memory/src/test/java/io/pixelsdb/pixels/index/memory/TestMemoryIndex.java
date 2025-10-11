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
package io.pixelsdb.pixels.index.memory;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for MemoryIndex class.
 * Tests all public interfaces with both unique and non-unique indexes.
 *
 * @author hank
 * @create 2025-10-12
 */
public class TestMemoryIndex
{
    private static final long TABLE_ID = 1L;
    private static final long INDEX_ID = 1L;

    private MemoryIndex uniqueIndex;
    private MemoryIndex nonUniqueIndex;

    @Before
    public void setUp()
    {
        uniqueIndex = new MemoryIndex(TABLE_ID, INDEX_ID, true);
        nonUniqueIndex = new MemoryIndex(TABLE_ID, INDEX_ID + 1, false);

        // Create SQLite Directory
        try
        {
            String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
            FileUtils.forceMkdir(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to create SQLite test directory: " + e.getMessage());
        }
    }

    @After
    public void tearDown()
    {
        if (uniqueIndex != null)
        {
            uniqueIndex.closeAndRemove();
        }
        if (nonUniqueIndex != null)
        {
            nonUniqueIndex.closeAndRemove();
        }

        // Clear SQLite Directory
        try
        {
            MainIndexFactory.Instance().getMainIndex(TABLE_ID).closeAndRemove();
        }
        catch (MainIndexException e)
        {
            System.err.println("Failed to clean up SQLite test directory: " + e.getMessage());
        }
    }

    // Helper methods
    private IndexProto.IndexKey createIndexKey(String keyValue, long timestamp)
    {
        return IndexProto.IndexKey.newBuilder()
                .setIndexId(INDEX_ID)
                .setKey(ByteString.copyFromUtf8(keyValue))
                .setTimestamp(timestamp)
                .build();
    }

    private IndexProto.PrimaryIndexEntry createPrimaryEntry(String keyValue, long timestamp, long rowId)
    {
        return IndexProto.PrimaryIndexEntry.newBuilder()
                .setIndexKey(createIndexKey(keyValue, timestamp))
                .setRowId(rowId)
                .setRowLocation(IndexProto.RowLocation.newBuilder().setFileId(50L).setRgId(0).setRgRowOffset(100).build())
                .build();
    }

    private IndexProto.SecondaryIndexEntry createSecondaryEntry(String keyValue, long timestamp, long rowId)
    {
        return IndexProto.SecondaryIndexEntry.newBuilder()
                .setIndexKey(createIndexKey(keyValue, timestamp))
                .setRowId(rowId)
                .build();
    }

    // Test basic properties
    @Test
    public void testBasicProperties()
    {
        assertEquals(TABLE_ID, uniqueIndex.getTableId());
        assertEquals(INDEX_ID, uniqueIndex.getIndexId());
        assertTrue(uniqueIndex.isUnique());
        assertFalse(nonUniqueIndex.isUnique());
    }

    // Test putEntry and getUniqueRowId for unique index
    @Test
    public void testUniqueIndexPutAndGet() throws SinglePointIndexException
    {
        IndexProto.IndexKey key1 = createIndexKey("key1", 1000L);
        long rowId1 = 1L;

        // Test put and get
        assertTrue(uniqueIndex.putEntry(key1, rowId1));
        assertEquals(rowId1, uniqueIndex.getUniqueRowId(key1));

        // Test with different timestamp
        IndexProto.IndexKey key1Later = createIndexKey("key1", 2000L);
        assertEquals(rowId1, uniqueIndex.getUniqueRowId(key1Later)); // Should see latest version

        // Test non-existent key
        IndexProto.IndexKey nonExistentKey = createIndexKey("non-existent", 1000L);
        assertEquals(-1L, uniqueIndex.getUniqueRowId(nonExistentKey));
    }

    // Test putEntry and getRowIds for non-unique index
    @Test
    public void testNonUniqueIndexPutAndGet() throws SinglePointIndexException
    {
        IndexProto.IndexKey key1 = createIndexKey("key1", 1000L);
        long rowId1 = 1L;
        long rowId2 = 2L;

        // Test put and get single value
        assertTrue(nonUniqueIndex.putEntry(key1, rowId1));
        List<Long> rowIds = nonUniqueIndex.getRowIds(key1);
        assertEquals(1, rowIds.size());
        assertEquals(rowId1, (long) rowIds.get(0));

        // Test put multiple values for same key
        assertTrue(nonUniqueIndex.putEntry(key1, rowId2));
        rowIds = nonUniqueIndex.getRowIds(key1);
        assertEquals(2, rowIds.size());
        assertTrue(rowIds.contains(rowId1));
        assertTrue(rowIds.contains(rowId2));

        // Test non-existent key
        IndexProto.IndexKey nonExistentKey = createIndexKey("non-existent", 1000L);
        assertTrue(nonUniqueIndex.getRowIds(nonExistentKey).isEmpty());
    }

    // Test MVCC functionality with multiple versions
    @Test
    public void testMVCCFunctionality() throws SinglePointIndexException
    {
        String keyValue = "mvcc_key";

        // Create multiple versions
        IndexProto.IndexKey keyV1 = createIndexKey(keyValue, 1000L);
        IndexProto.IndexKey keyV2 = createIndexKey(keyValue, 2000L);
        IndexProto.IndexKey keyV3 = createIndexKey(keyValue, 3000L);

        long rowIdV1 = 1L;
        long rowIdV2 = 2L;
        long rowIdV3 = 3L;

        // Put versions in order
        uniqueIndex.putEntry(keyV1, rowIdV1);
        uniqueIndex.putEntry(keyV2, rowIdV2);
        uniqueIndex.putEntry(keyV3, rowIdV3);

        // Test visibility at different timestamps
        assertEquals(rowIdV1, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 1000L)));
        assertEquals(rowIdV1, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 1500L)));
        assertEquals(rowIdV2, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 2000L)));
        assertEquals(rowIdV2, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 2500L)));
        assertEquals(rowIdV3, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 3000L)));
        assertEquals(rowIdV3, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 3500L)));
    }

    // Test putPrimaryEntries
    @Test
    public void testPutPrimaryEntries() throws SinglePointIndexException, MainIndexException
    {
        List<IndexProto.PrimaryIndexEntry> entries = Arrays.asList(
                createPrimaryEntry("key1", 1000L, 1L),
                createPrimaryEntry("key2", 1000L, 2L),
                createPrimaryEntry("key3", 1000L, 3L)
        );

        assertTrue(uniqueIndex.putPrimaryEntries(entries));

        // Verify entries were inserted
        assertEquals(1L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 1000L)));
        assertEquals(2L, uniqueIndex.getUniqueRowId(createIndexKey("key2", 1000L)));
        assertEquals(3L, uniqueIndex.getUniqueRowId(createIndexKey("key3", 1000L)));
    }

    // Test putSecondaryEntries for unique index
    @Test
    public void testPutSecondaryEntriesUnique() throws SinglePointIndexException
    {
        List<IndexProto.SecondaryIndexEntry> entries = Arrays.asList(
                createSecondaryEntry("key1", 1000L, 1L),
                createSecondaryEntry("key2", 1000L, 2L),
                createSecondaryEntry("key3", 1000L, 3L)
        );

        assertTrue(uniqueIndex.putSecondaryEntries(entries));

        // Verify entries were inserted
        assertEquals(1L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 1000L)));
        assertEquals(2L, uniqueIndex.getUniqueRowId(createIndexKey("key2", 1000L)));
        assertEquals(3L, uniqueIndex.getUniqueRowId(createIndexKey("key3", 1000L)));
    }

    // Test putSecondaryEntries for non-unique index
    @Test
    public void testPutSecondaryEntriesNonUnique() throws SinglePointIndexException
    {
        List<IndexProto.SecondaryIndexEntry> entries = Arrays.asList(
                createSecondaryEntry("key1", 1000L, 1L),
                createSecondaryEntry("key1", 1000L, 2L), // Same key, different rowId
                createSecondaryEntry("key2", 1000L, 3L)
        );

        assertTrue(nonUniqueIndex.putSecondaryEntries(entries));

        // Verify entries were inserted
        List<Long> key1RowIds = nonUniqueIndex.getRowIds(createIndexKey("key1", 1000L));
        assertEquals(2, key1RowIds.size());
        assertTrue(key1RowIds.contains(1L));
        assertTrue(key1RowIds.contains(2L));

        List<Long> key2RowIds = nonUniqueIndex.getRowIds(createIndexKey("key2", 1000L));
        assertEquals(1, key2RowIds.size());
        assertEquals(3L, (long) key2RowIds.get(0));
    }

    // Test updatePrimaryEntry
    @Test
    public void testUpdatePrimaryEntry() throws SinglePointIndexException
    {
        IndexProto.IndexKey key = createIndexKey("key1", 1000L);
        long initialRowId = 1L;
        long updatedRowId = 2L;

        // Put initial value
        uniqueIndex.putEntry(key, initialRowId);
        assertEquals(initialRowId, uniqueIndex.getUniqueRowId(key));

        // Update value
        long previousRowId = uniqueIndex.updatePrimaryEntry(key, updatedRowId);
        assertEquals(initialRowId, previousRowId);
        assertEquals(updatedRowId, uniqueIndex.getUniqueRowId(key));
    }

    // Test updateSecondaryEntry for unique index
    @Test
    public void testUpdateSecondaryEntryUnique() throws SinglePointIndexException
    {
        IndexProto.IndexKey key = createIndexKey("key1", 1000L);
        long initialRowId = 1L;
        long updatedRowId = 2L;

        // Put initial value
        uniqueIndex.putEntry(key, initialRowId);
        assertEquals(initialRowId, uniqueIndex.getUniqueRowId(key));

        // Update value
        List<Long> previousRowIds = uniqueIndex.updateSecondaryEntry(key, updatedRowId);
        assertEquals(1, previousRowIds.size());
        assertEquals(initialRowId, (long) previousRowIds.get(0));
        assertEquals(updatedRowId, uniqueIndex.getUniqueRowId(key));
    }

    // Test updateSecondaryEntry for non-unique index
    @Test
    public void testUpdateSecondaryEntryNonUnique() throws SinglePointIndexException
    {
        IndexProto.IndexKey key = createIndexKey("key1", 1000L);

        // Put multiple values
        nonUniqueIndex.putEntry(key, 1L);
        nonUniqueIndex.putEntry(key, 2L);
        nonUniqueIndex.putEntry(key, 3L);

        List<Long> initialRowIds = nonUniqueIndex.getRowIds(key);
        assertEquals(3, initialRowIds.size());

        // Update - should replace all values with single new value
        List<Long> previousRowIds = nonUniqueIndex.updateSecondaryEntry(key, 4L);
        assertEquals(3, previousRowIds.size());

        List<Long> updatedRowIds = nonUniqueIndex.getRowIds(key);
        assertEquals(4, updatedRowIds.size());
        assertEquals(4L, (long) updatedRowIds.get(3));
    }

    // Test updatePrimaryEntries
    @Test
    public void testUpdatePrimaryEntries() throws SinglePointIndexException, MainIndexException
    {
        List<IndexProto.PrimaryIndexEntry> initialEntries = Arrays.asList(
                createPrimaryEntry("key1", 1000L, 1L),
                createPrimaryEntry("key2", 1000L, 2L)
        );

        List<IndexProto.PrimaryIndexEntry> updateEntries = Arrays.asList(
                createPrimaryEntry("key1", 2000L, 10L), // Update key1
                createPrimaryEntry("key2", 2000L, 20L)  // Update key2
        );

        // Put initial entries
        uniqueIndex.putPrimaryEntries(initialEntries);

        // Update entries
        List<Long> previousRowIds = uniqueIndex.updatePrimaryEntries(updateEntries);
        assertEquals(2, previousRowIds.size());
        assertTrue(previousRowIds.contains(1L));
        assertTrue(previousRowIds.contains(2L));

        // Verify updates
        assertEquals(10L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 2000L)));
        assertEquals(20L, uniqueIndex.getUniqueRowId(createIndexKey("key2", 2000L)));
    }

    // Test updateSecondaryEntries for unique index
    @Test
    public void testUpdateSecondaryEntriesUnique() throws SinglePointIndexException
    {
        List<IndexProto.SecondaryIndexEntry> initialEntries = Arrays.asList(
                createSecondaryEntry("key1", 1000L, 1L),
                createSecondaryEntry("key2", 1000L, 2L)
        );

        List<IndexProto.SecondaryIndexEntry> updateEntries = Arrays.asList(
                createSecondaryEntry("key1", 2000L, 10L),
                createSecondaryEntry("key2", 2000L, 20L)
        );

        // Put initial entries
        uniqueIndex.putSecondaryEntries(initialEntries);

        // Update entries
        List<Long> previousRowIds = uniqueIndex.updateSecondaryEntries(updateEntries);
        assertEquals(2, previousRowIds.size());
        assertTrue(previousRowIds.contains(1L));
        assertTrue(previousRowIds.contains(2L));

        // Verify updates
        assertEquals(10L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 2000L)));
        assertEquals(20L, uniqueIndex.getUniqueRowId(createIndexKey("key2", 2000L)));
    }

    // Test updateSecondaryEntries for non-unique index
    @Test
    public void testUpdateSecondaryEntriesNonUnique() throws SinglePointIndexException
    {
        List<IndexProto.SecondaryIndexEntry> initialEntries = Arrays.asList(
                createSecondaryEntry("key1", 1000L, 1L),
                createSecondaryEntry("key1", 1000L, 2L), // Multiple values for key1
                createSecondaryEntry("key2", 1000L, 3L)
        );

        List<IndexProto.SecondaryIndexEntry> updateEntries = Arrays.asList(
                createSecondaryEntry("key1", 2000L, 10L), // Replace multiple values with single value
                createSecondaryEntry("key2", 2000L, 20L)
        );

        // Put initial entries
        nonUniqueIndex.putSecondaryEntries(initialEntries);

        // Update entries
        List<Long> previousRowIds = nonUniqueIndex.updateSecondaryEntries(updateEntries);
        assertEquals(3, previousRowIds.size()); // Should have 3 previous rowIds (2 from key1, 1 from key2)

        // Verify updates
        List<Long> key1RowIds = nonUniqueIndex.getRowIds(createIndexKey("key1", 2000L));
        assertEquals(3, key1RowIds.size());
        assertEquals(10L, (long) key1RowIds.get(2));

        List<Long> key2RowIds = nonUniqueIndex.getRowIds(createIndexKey("key2", 2000L));
        assertEquals(2, key2RowIds.size());
        assertEquals(20L, (long) key2RowIds.get(1));
    }

    // Test deleteUniqueEntry
    @Test
    public void testDeleteUniqueEntry() throws SinglePointIndexException
    {
        IndexProto.IndexKey key = createIndexKey("key1", 1000L);
        long rowId = 1L;

        // Put entry
        uniqueIndex.putEntry(key, rowId);
        assertEquals(rowId, uniqueIndex.getUniqueRowId(key));

        // Delete entry
        long deletedRowId = uniqueIndex.deleteUniqueEntry(key);
        assertEquals(rowId, deletedRowId);

        // Verify entry is marked as deleted (tombstone)
        assertEquals(-1L, uniqueIndex.getUniqueRowId(key));
    }

    // Test deleteEntry for unique index
    @Test
    public void testDeleteEntryUnique() throws SinglePointIndexException
    {
        IndexProto.IndexKey key = createIndexKey("key1", 1000L);
        long rowId = 1L;

        // Put entry
        uniqueIndex.putEntry(key, rowId);
        assertEquals(rowId, uniqueIndex.getUniqueRowId(key));

        // Delete entry
        List<Long> deletedRowIds = uniqueIndex.deleteEntry(key);
        assertEquals(1, deletedRowIds.size());
        assertEquals(rowId, (long) deletedRowIds.get(0));

        // Verify entry is marked as deleted
        assertEquals(-1L, uniqueIndex.getUniqueRowId(key));
    }

    // Test deleteEntry for non-unique index
    @Test
    public void testDeleteEntryNonUnique() throws SinglePointIndexException
    {
        IndexProto.IndexKey key = createIndexKey("key1", 1000L);

        // Put multiple entries
        nonUniqueIndex.putEntry(key, 1L);
        nonUniqueIndex.putEntry(key, 2L);
        nonUniqueIndex.putEntry(key, 3L);

        List<Long> initialRowIds = nonUniqueIndex.getRowIds(key);
        assertEquals(3, initialRowIds.size());

        // Delete all entries for this key
        List<Long> deletedRowIds = nonUniqueIndex.deleteEntry(key);
        assertEquals(3, deletedRowIds.size());
        assertTrue(deletedRowIds.contains(1L));
        assertTrue(deletedRowIds.contains(2L));
        assertTrue(deletedRowIds.contains(3L));

        // Verify entries are marked as deleted
        List<Long> currentRowIds = nonUniqueIndex.getRowIds(key);
        assertTrue(currentRowIds.isEmpty());
    }

    // Test deleteEntries for unique index
    @Test
    public void testDeleteEntriesUnique() throws SinglePointIndexException
    {
        List<IndexProto.IndexKey> keys = Arrays.asList(
                createIndexKey("key1", 1000L),
                createIndexKey("key2", 1000L),
                createIndexKey("key3", 1000L)
        );

        // Put entries
        uniqueIndex.putEntry(keys.get(0), 1L);
        uniqueIndex.putEntry(keys.get(1), 2L);
        uniqueIndex.putEntry(keys.get(2), 3L);

        // Delete entries
        List<Long> deletedRowIds = uniqueIndex.deleteEntries(keys);
        assertEquals(3, deletedRowIds.size());
        assertTrue(deletedRowIds.contains(1L));
        assertTrue(deletedRowIds.contains(2L));
        assertTrue(deletedRowIds.contains(3L));

        // Verify entries are marked as deleted
        for (IndexProto.IndexKey key : keys)
        {
            assertEquals(-1L, uniqueIndex.getUniqueRowId(key));
        }
    }

    // Test deleteEntries for non-unique index
    @Test
    public void testDeleteEntriesNonUnique() throws SinglePointIndexException
    {
        List<IndexProto.IndexKey> keys = Arrays.asList(
                createIndexKey("key1", 1000L),
                createIndexKey("key2", 1000L)
        );

        // Put multiple entries per key
        nonUniqueIndex.putEntry(keys.get(0), 1L);
        nonUniqueIndex.putEntry(keys.get(0), 2L);
        nonUniqueIndex.putEntry(keys.get(1), 3L);
        nonUniqueIndex.putEntry(keys.get(1), 4L);

        // Delete entries
        List<Long> deletedRowIds = nonUniqueIndex.deleteEntries(keys);
        assertEquals(4, deletedRowIds.size());
        assertTrue(deletedRowIds.contains(1L));
        assertTrue(deletedRowIds.contains(2L));
        assertTrue(deletedRowIds.contains(3L));
        assertTrue(deletedRowIds.contains(4L));

        // Verify entries are marked as deleted
        for (IndexProto.IndexKey key : keys)
        {
            assertTrue(nonUniqueIndex.getRowIds(key).isEmpty());
        }
    }

    // Test purgeEntries
    @Test
    public void testPurgeEntries() throws SinglePointIndexException
    {
        // Create entries with different timestamps
        IndexProto.IndexKey keyV1 = createIndexKey("key1", 1000L);
        IndexProto.IndexKey keyV2 = createIndexKey("key1", 2000L);
        IndexProto.IndexKey keyV3 = createIndexKey("key1", 3000L);

        uniqueIndex.putEntry(keyV1, 1L);
        uniqueIndex.putEntry(keyV2, 2L);
        uniqueIndex.putEntry(keyV3, 3L);

        // Delete version at timestamp 2000
        uniqueIndex.deleteEntry(keyV2);

        // Purge entries up to timestamp 2500
        List<IndexProto.IndexKey> purgeKeys = Arrays.asList(createIndexKey("key1", 2500L));
        List<Long> purgedRowIds = uniqueIndex.purgeEntries(purgeKeys);

        // Should purge versions 1000 and 2000
        assertEquals(2, purgedRowIds.size());
        assertTrue(purgedRowIds.contains(1L));
        assertTrue(purgedRowIds.contains(2L));

        // Version 3000 should still be accessible
        assertEquals(3L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 3000L)));

        // Version 1000 and 2000 should not be accessible anymore
        assertEquals(-1L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 1000L)));
        assertEquals(-1L, uniqueIndex.getUniqueRowId(createIndexKey("key1", 2000L)));
    }

    // Test tombstone functionality with MVCC
    @Test
    public void testTombstoneWithMVCC() throws SinglePointIndexException
    {
        String keyValue = "tombstone_key";

        // Create multiple versions
        IndexProto.IndexKey keyV1 = createIndexKey(keyValue, 1000L);
        IndexProto.IndexKey keyV2 = createIndexKey(keyValue, 2000L);
        IndexProto.IndexKey keyV3 = createIndexKey(keyValue, 3000L);

        uniqueIndex.putEntry(keyV1, 1L);
        uniqueIndex.putEntry(keyV2, 2L);
        uniqueIndex.putEntry(keyV3, 3L);

        // Delete at timestamp 2000 (creates tombstone)
        uniqueIndex.deleteEntry(keyV2);

        // Test visibility:
        // - At timestamp 1500: should see version 1000 (value 1)
        assertEquals(1L, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 1500L)));

        // - At timestamp 2500: should see tombstone (value -1) because version 2000 is deleted
        assertEquals(-1L, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 2500L)));

        // - At timestamp 3500: should see tombstone (value -1) because version 2000 is deleted
        assertEquals(-1, uniqueIndex.getUniqueRowId(createIndexKey(keyValue, 3500L)));
    }

    // Test closeAndRemove
    @Test
    public void testCloseAndRemove() throws SinglePointIndexException
    {
        // Put some entries
        uniqueIndex.putEntry(createIndexKey("key1", 1000L), 1L);
        uniqueIndex.putEntry(createIndexKey("key2", 1000L), 2L);

        // Close and remove
        assertTrue(uniqueIndex.closeAndRemove());

        // Second call should return false
        assertFalse(uniqueIndex.closeAndRemove());
    }

    // Test error cases
    @Test(expected = SinglePointIndexException.class)
    public void testGetUniqueRowIdOnNonUniqueIndex() throws SinglePointIndexException
    {
        nonUniqueIndex.getUniqueRowId(createIndexKey("key1", 1000L));
    }

    @Test(expected = SinglePointIndexException.class)
    public void testPutPrimaryEntriesOnNonUniqueIndex() throws SinglePointIndexException, MainIndexException
    {
        List<IndexProto.PrimaryIndexEntry> entries = Arrays.asList(
                createPrimaryEntry("key1", 1000L, 1L)
        );
        nonUniqueIndex.putPrimaryEntries(entries);
    }

    @Test(expected = SinglePointIndexException.class)
    public void testUpdatePrimaryEntryOnNonUniqueIndex() throws SinglePointIndexException
    {
        nonUniqueIndex.updatePrimaryEntry(createIndexKey("key1", 1000L), 1L);
    }

    @Test(expected = SinglePointIndexException.class)
    public void testUpdatePrimaryEntriesOnNonUniqueIndex() throws SinglePointIndexException
    {
        List<IndexProto.PrimaryIndexEntry> entries = Arrays.asList(
                createPrimaryEntry("key1", 1000L, 1L)
        );
        nonUniqueIndex.updatePrimaryEntries(entries);
    }

    @Test(expected = SinglePointIndexException.class)
    public void testDeleteUniqueEntryOnNonUniqueIndex() throws SinglePointIndexException
    {
        nonUniqueIndex.deleteUniqueEntry(createIndexKey("key1", 1000L));
    }

    // Test size monitoring methods
    @Test
    public void testSizeMonitoring() throws SinglePointIndexException
    {
        assertEquals(0, uniqueIndex.size());
        assertEquals(0, uniqueIndex.tombstonesSize());

        // Put some entries
        uniqueIndex.putEntry(createIndexKey("key1", 1000L), 1L);
        uniqueIndex.putEntry(createIndexKey("key2", 1000L), 2L);
        uniqueIndex.putEntry(createIndexKey("key3", 1000L), 3L);

        // Should have 3 entries
        assertTrue(uniqueIndex.size() >= 3);

        // Delete one entry
        uniqueIndex.deleteEntry(createIndexKey("key1", 1000L));

        // Should have tombstone
        assertTrue(uniqueIndex.tombstonesSize() > 0);

        // Purge
        uniqueIndex.purgeEntries(Arrays.asList(createIndexKey("key1", 1000L)));

        // Tombstone should be cleared
        assertEquals(0, uniqueIndex.tombstonesSize());
    }

    // Test concurrent operations (basic stress test)
    @Test
    public void testConcurrentOperations() throws InterruptedException
    {
        final int numThreads = 10;
        final int operationsPerThread = 100;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++)
        {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try
                {
                    for (int j = 0; j < operationsPerThread; j++)
                    {
                        String key = "key_" + threadId + "_" + j;
                        long timestamp = threadId * 1000L + j;
                        long rowId = threadId * 100L + j;

                        uniqueIndex.putEntry(createIndexKey(key, timestamp), rowId);

                        if (j % 10 == 0)
                        {
                            // Occasionally read
                            uniqueIndex.getUniqueRowId(createIndexKey(key, timestamp));
                        }

                        if (j % 20 == 0)
                        {
                            // Occasionally delete
                            uniqueIndex.deleteEntry(createIndexKey(key, timestamp));
                        }
                    }
                }
                catch (SinglePointIndexException e)
                {
                    fail("Thread " + threadId + " failed: " + e.getMessage());
                }
            });
        }

        // Start all threads
        for (Thread thread : threads)
        {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads)
        {
            thread.join();
        }

        // Verify no exceptions were thrown and index is still functional
        try
        {
            assertTrue(uniqueIndex.size() > 0);
        }
        catch (Exception e)
        {
            fail("Index should still be functional after concurrent operations: " + e.getMessage());
        }
    }
}