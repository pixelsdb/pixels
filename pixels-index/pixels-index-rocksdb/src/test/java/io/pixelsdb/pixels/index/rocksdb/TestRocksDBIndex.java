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
import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TestRocksDBIndex
{
    @Mock
    private RocksDB rocksDB = mock(RocksDB.class);  // Mock RocksDB instance
    private String rocksDBpath = "tmp/rocksdb_test";
    private SecondaryIndex rocksDBIndex; // Class under test
    private final MainIndex mainIndex = new MainIndexImpl();

    @BeforeEach
    public void setUp() throws RocksDBException, IOException
    {
        MockitoAnnotations.openMocks(this);
        System.out.println("Debug: Creating RocksDBIndex.."); // Debug log
        rocksDBIndex = new RocksDBIndex(rocksDBpath, mainIndex);
        System.out.println("Debug: RocksDBIndex instance: " + rocksDBIndex); // Check for null
        assertNotNull(rocksDBIndex);
    }

    @Test
    public void testGetUniqueRowId() throws RocksDBException
    {
        // Mock RocksDB's get method
        long indexId = 1L; // Sample indexId
        byte[] key = "exampleKey".getBytes(); // Sample index key
        long timestamp = System.currentTimeMillis(); // Current timestamp

        // Construct keyBytes matching IndexKey format
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // Add indexId
        buffer.put((byte) ':');
        buffer.put(key);          // Add key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // Add timestamp
        byte[] keyBytes = buffer.array(); // Final keyBytes

        byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(1001L).array(); // Sample value
        when(rocksDB.get(keyBytes)).thenReturn(valueBytes);

        // Call getUniqueRowId
        // Create IndexKey object matching keyBytes
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        long rowId = rocksDBIndex.getUniqueRowId(keyProto);
        // Verify return value
        assertEquals(1001L, rowId);
    }

    @Test
    public void testGetRowIds()
    {
        // Mock RocksDB iterator
        RocksIterator iterator = mock(RocksIterator.class);
        when(rocksDB.newIterator()).thenReturn(iterator);

        // Mock input parameters
        long indexId = 1L; // Sample indexId
        byte[] key = "exampleKey".getBytes(); // Sample index key
        long timestamp = System.currentTimeMillis(); // Current timestamp

        // Construct prefixBytes matching IndexKey format
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // Add indexId
        buffer.put((byte) ':');
        buffer.put(key);          // Add key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // Add timestamp
        byte[] prefixBytes = buffer.array(); // Final prefixBytes

        // Mock rowId keys (containing prefix and rowId)
        long rowId1 = 1001L;
        long rowId2 = 1002L;

        ByteBuffer keyBuffer1 = ByteBuffer.allocate(prefixBytes.length + Long.BYTES);
        keyBuffer1.put(prefixBytes).putLong(rowId1);

        ByteBuffer keyBuffer2 = ByteBuffer.allocate(prefixBytes.length + Long.BYTES);
        keyBuffer2.put(prefixBytes).putLong(rowId2);

        byte[] combinedKey1 = keyBuffer1.array(); // First complete key
        byte[] combinedKey2 = keyBuffer2.array(); // Second complete key

        when(iterator.isValid()).thenReturn(true, true, false); // Mock two valid iterations
        when(iterator.key()).thenReturn(combinedKey1, combinedKey2); // Mock returning keys
        when(iterator.value()).thenReturn(null, null); // Mock values (unused)

        // Create IndexKey object matching prefixBytes
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();

        // Call getRowIds
        long[] rowIds = rocksDBIndex.getRowIds(keyProto);

        // Verify return value matches expected ({1001L, 1002L})
        assertArrayEquals(new long[]{1001L, 1002L}, rowIds);
    }

    @Test
    public void testPutEntry() throws RocksDBException
    {
        // Mock RocksDB's put method
        long indexId = 1L; // Sample indexId
        byte[] key = "exampleKey".getBytes(); // Sample index key
        long timestamp = System.currentTimeMillis(); // Current timestamp

        // Construct keyBytes matching IndexKey format
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // Add indexId
        buffer.put((byte) ':');
        buffer.put(key);          // Add key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // Add timestamp
        byte[] keyBytes = buffer.array(); // Final keyBytes
        byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(1001L).array(); // Sample value
        doNothing().when(rocksDB).put(keyBytes, valueBytes);

        // Call putEntry
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        SecondaryIndex.Entry entry = new SecondaryIndex.Entry(keyProto, 0, true); // Sample Entry
        boolean success = rocksDBIndex.putEntry(entry);

        // Verify return value
        assertTrue(success);
    }

    @Test
    public void testPutEntries() throws RocksDBException
    {
        // Mock RocksDB's put method
        long indexId = 1L; // Sample indexId
        byte[] key = "exampleKey".getBytes(); // Sample index key
        long timestamp = System.currentTimeMillis(); // Current timestamp

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // Add indexId
        buffer.put((byte) ':');
        buffer.put(key);          // Add key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // Add timestamp
        byte[] keyBytes = buffer.array(); // Final keyBytes
        byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(1001L).array(); // Sample value
        doNothing().when(rocksDB).put(keyBytes, valueBytes);

        // Call putEntries
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        List<SecondaryIndex.Entry> entries = new ArrayList<>();
        entries.add(new SecondaryIndex.Entry(keyProto, 1001L, true)); // Sample Entry
        boolean success = rocksDBIndex.putEntries(entries);

        // Verify return value
        assertTrue(success);
        verify(rocksDB, times(1)).put(keyBytes, valueBytes); // Verify put was called
    }

    @Test
    public void testDeleteEntry() throws RocksDBException
    {
        // Mock RocksDB's delete method
        long indexId = 1L; // Sample indexId
        byte[] key = "exampleKey".getBytes(); // Sample index key
        long timestamp = System.currentTimeMillis(); // Current timestamp

        // Construct keyBytes matching IndexKey format
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // Add indexId
        buffer.put((byte) ':');
        buffer.put(key);          // Add key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // Add timestamp
        byte[] keyBytes = buffer.array(); // Final keyBytes

        doNothing().when(rocksDB).delete(keyBytes);

        // Create IndexKey object matching keyBytes
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        boolean success = rocksDBIndex.deleteEntry(keyProto);

        // Verify return value
        assertTrue(success);
        verify(rocksDB, times(1)).delete(keyBytes); // Verify delete was called
    }

    @Test
    public void testDeleteEntries() throws RocksDBException
    {
        // Mock RocksDB's delete method
        long indexId = 1L; // Sample indexId
        byte[] key = "exampleKey".getBytes(); // Sample index key
        long timestamp = System.currentTimeMillis(); // Current timestamp

        // Construct keyBytes matching IndexKey format
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // Add indexId
        buffer.put((byte) ':');
        buffer.put(key);          // Add key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // Add timestamp
        byte[] keyBytes = buffer.array(); // Final keyBytes
        doNothing().when(rocksDB).delete(keyBytes);

        // Call deleteEntries
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        List<IndexProto.IndexKey> keys = new ArrayList<>();
        keys.add(keyProto); // Sample IndexKey list
        boolean success = rocksDBIndex.deleteEntries(keys);

        // Verify return value
        assertTrue(success);
        verify(rocksDB, times(1)).delete(keyBytes); // Verify delete was called
    }
}