package io.pixelsdb.pixels.index.rocksdb;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexImpl;
import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TestRocksDBIndex {

    @Mock
    private RocksDB rocksDB = mock(RocksDB.class);  // 模拟 RocksDB 实例
    private String rocksDBpath = "tmp/rocksdb_test";
    private SecondaryIndex rocksDBIndex; // 被测试的类
    private final MainIndex mainIndex = new MainIndexImpl();
    @BeforeEach
    public void setUp() throws RocksDBException, IOException {
        MockitoAnnotations.openMocks(this);
        System.out.println("Debug: Creating RocksDBIndex.."); // 调试日志
        rocksDBIndex = new RocksDBIndex(rocksDBpath, mainIndex);
        System.out.println("Debug: RocksDBIndex instance: " + rocksDBIndex); // 检查是否为 null
        assertNotNull(rocksDBIndex);
    }

    @Test
    public void testGetUniqueRowId() throws RocksDBException {
        // 模拟 RocksDB 的 get 方法
        long indexId = 1L; // 假设的 indexId
        byte[] key = "exampleKey".getBytes(); // 假设的索引键
        long timestamp = System.currentTimeMillis(); // 当前时间戳

        // 构造 keyBytes，与 IndexKey 一致
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // 放入 indexId
        buffer.put((byte) ':');
        buffer.put(key);          // 放入 key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // 放入 timestamp
        byte[] keyBytes = buffer.array(); // 生成最终的 keyBytes

        byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(1001L).array(); // 假设的值
        when(rocksDB.get(keyBytes)).thenReturn(valueBytes);

        // 调用 getUniqueRowId
        // 创建 IndexKey 对象，确保与 keyBytes 一致
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        long rowId = rocksDBIndex.getUniqueRowId(keyProto);
        // 验证返回值
        assertEquals(1001L, rowId);
    }

    @Test
    public void testGetRowIds() {
        // 模拟 RocksDB 的迭代器
        RocksIterator iterator = mock(RocksIterator.class);
        when(rocksDB.newIterator()).thenReturn(iterator);

        // 模拟输入参数
        long indexId = 1L; // 假设的 indexId
        byte[] key = "exampleKey".getBytes(); // 假设的索引键
        long timestamp = System.currentTimeMillis(); // 当前时间戳

        // 构造 prefixBytes，与 IndexKey 一致
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // 放入 indexId
        buffer.put((byte) ':');
        buffer.put(key);          // 放入 key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // 放入 timestamp
        byte[] prefixBytes = buffer.array(); // 生成最终的 prefixBytes

        // 模拟 rowId 键（包含前缀和 rowId）
        long rowId1 = 1001L;
        long rowId2 = 1002L;

        ByteBuffer keyBuffer1 = ByteBuffer.allocate(prefixBytes.length + Long.BYTES);
        keyBuffer1.put(prefixBytes).putLong(rowId1);

        ByteBuffer keyBuffer2 = ByteBuffer.allocate(prefixBytes.length + Long.BYTES);
        keyBuffer2.put(prefixBytes).putLong(rowId2);

        byte[] combinedKey1 = keyBuffer1.array(); // 第一个完整键
        byte[] combinedKey2 = keyBuffer2.array(); // 第二个完整键

        when(iterator.isValid()).thenReturn(true, true, false); // 模拟两次有效迭代
        when(iterator.key()).thenReturn(combinedKey1, combinedKey2); // 模拟返回完整键
        when(iterator.value()).thenReturn(null, null); // 模拟值（未使用）

        // 创建 IndexKey 对象，确保与 prefixBytes 一致
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();

        // 调用 getRowIds
        long[] rowIds = rocksDBIndex.getRowIds(keyProto);

        // 验证返回值是否符合预期（应为 {1001L, 1002L}）
        assertArrayEquals(new long[]{1001L, 1002L}, rowIds);
    }

    @Test
    public void testPutEntry() throws RocksDBException {
        // 模拟 RocksDB 的 put 方法
        long indexId = 1L; // 假设的 indexId
        byte[] key = "exampleKey".getBytes(); // 假设的索引键
        long timestamp = System.currentTimeMillis(); // 当前时间戳

        // 构造 keyBytes，与 IndexKey 一致
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // 放入 indexId
        buffer.put((byte) ':');
        buffer.put(key);          // 放入 key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // 放入 timestamp
        byte[] keyBytes = buffer.array(); // 生成最终的 keyBytes
        byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(1001L).array(); // 假设的值
        doNothing().when(rocksDB).put(keyBytes, valueBytes);

        // 调用 putEntry
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        SecondaryIndex.Entry entry = new SecondaryIndex.Entry(keyProto, 0, true); // 假设的 Entry
        boolean success = rocksDBIndex.putEntry(entry);

        // 验证返回值
        assertTrue(success);
    }

    @Test
    public void testPutEntries() throws RocksDBException {
        // 模拟 RocksDB 的 put 方法
        long indexId = 1L; // 假设的 indexId
        byte[] key = "exampleKey".getBytes(); // 假设的索引键
        long timestamp = System.currentTimeMillis(); // 当前时间戳

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // 放入 indexId
        buffer.put((byte) ':');
        buffer.put(key);          // 放入 key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // 放入 timestamp
        byte[] keyBytes = buffer.array(); // 生成最终的 keyBytes
        byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(1001L).array(); // 假设的值
        doNothing().when(rocksDB).put(keyBytes, valueBytes);

        // 调用 putEntries
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        List<SecondaryIndex.Entry> entries = new ArrayList<>();
        entries.add(new SecondaryIndex.Entry(keyProto, 1001L, true)); // 假设的 Entry
        boolean success = rocksDBIndex.putEntries(entries);

        // 验证返回值
        assertTrue(success);
        verify(rocksDB, times(1)).put(keyBytes, valueBytes); // 验证 put 方法被调用
    }

    @Test
    public void testDeleteEntry() throws RocksDBException {
        // 模拟 RocksDB 的 delete 方法
        long indexId = 1L; // 假设的 indexId
        byte[] key = "exampleKey".getBytes(); // 假设的索引键
        long timestamp = System.currentTimeMillis(); // 当前时间戳

        // 构造 keyBytes，与 IndexKey 一致
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // 放入 indexId
        buffer.put((byte) ':');
        buffer.put(key);          // 放入 key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // 放入 timestamp
        byte[] keyBytes = buffer.array(); // 生成最终的 keyBytes

        doNothing().when(rocksDB).delete(keyBytes);

        // 创建 IndexKey 对象，确保与 keyBytes 一致
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        boolean success = rocksDBIndex.deleteEntry(keyProto);

        // 验证返回值
        assertTrue(success);
        verify(rocksDB, times(1)).delete(keyBytes); // 验证 delete 方法被调用
    }

    @Test
    public void testDeleteEntries() throws RocksDBException {
        // 模拟 RocksDB 的 delete 方法
        long indexId = 1L; // 假设的 indexId
        byte[] key = "exampleKey".getBytes(); // 假设的索引键
        long timestamp = System.currentTimeMillis(); // 当前时间戳

        // 构造 keyBytes，与 IndexKey 一致
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + key.length + Long.BYTES + 2);
        buffer.putLong(indexId); // 放入 indexId
        buffer.put((byte) ':');
        buffer.put(key);          // 放入 key
        buffer.put((byte) ':');
        buffer.putLong(timestamp); // 放入 timestamp
        byte[] keyBytes = buffer.array(); // 生成最终的 keyBytes
        doNothing().when(rocksDB).delete(keyBytes);

        // 调用 deleteEntries
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        List<IndexProto.IndexKey> keys = new ArrayList<>();
        keys.add(keyProto); // 假设的 IndexKey 列表
        boolean success = rocksDBIndex.deleteEntries(keys);

        // 验证返回值
        assertTrue(success);
        verify(rocksDB, times(1)).delete(keyBytes); // 验证 delete 方法被调用
    }
}