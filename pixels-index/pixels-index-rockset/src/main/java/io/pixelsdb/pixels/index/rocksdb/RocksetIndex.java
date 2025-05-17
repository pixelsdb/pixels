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
 * @create 2025-02-19
 */
public class RocksetIndex implements SecondaryIndex
{
    // load RocksetJni
    static {
        System.loadLibrary("RocksetJni");
    }

    // Native method
    private native long CreateCloudFileSystem0(
            String bucketName,
            String s3Prefix,
            long[] baseEnvPtrOut);
    private native long OpenDBCloud0(
            long cloudEnvPtr,
            String localDbPath,
            String persistentCachePath,
            long persistentCacheSizeGB,
            boolean readOnly);
    private native void DBput0(long dbHandle, byte[] key, byte[] value);
    private native byte[] DBget0(long dbHandle, byte[] key);
    private native void DBdelete0(long dbHandle, byte[] key);
    private native void CloseDB0(long dbHandle, long baseEnvPtr, long cloudEnvPtr);

    // 包装native方法的public方法
    private long CreateDBCloud(
            String bucketName,
            String s3Prefix,
            String localDbPath,
            String persistentCachePath,
            long persistentCacheSizeGB,
            boolean readOnly,
            long[] baseEnvPtrOut,
            long cloudEnvPtr) {
        cloudEnvPtr = CreateCloudFileSystem0(
                bucketName, s3Prefix, baseEnvPtrOut);
        if (cloudEnvPtr == 0) {
            throw new RuntimeException("Failed to create CloudFileSystem");
        }

        long dbHandle = OpenDBCloud0(
                cloudEnvPtr, localDbPath, persistentCachePath, persistentCacheSizeGB, readOnly);
        if (dbHandle == 0) {
            CloseDB0(0, baseEnvPtrOut[0], cloudEnvPtr); // 清理 base_env
            throw new RuntimeException("Failed to open DBCloud");
        }

        return dbHandle;
    }
    private void DBput(long dbHandle, byte[] key, byte[] value) {
        DBput0(dbHandle, key, value);
    }
    private byte[] DBget(long dbHandle, byte[] key) {
        return DBget0(dbHandle, key);
    }
    private void DBdelete(long dbHandle, byte[] key) {
        DBdelete0(dbHandle, key);
    }
    private void CloseDB(long dbHandle, long baseEnvPtr, long cloudEnvPtr) {
        if (dbHandle != 0) {
            CloseDB0(dbHandle, baseEnvPtr, cloudEnvPtr);
        }
    }

    // TODO: implement
    private long dbHandle = 0;
    // private final RocksDB rocksDB;
    public static final Logger LOGGER = LogManager.getLogger(RocksetIndex.class);
    private final MainIndex mainIndex;
    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private static String bucketName = "pixels-turbo-public";
    private static String s3Prefix = "test/rocksdb-cloud/";
    private static String localDbPath = "/tmp/rocksdb_cloud_test";
    private static String persistentCachePath = "/tmp/cache";
    private static long persistentCacheSizeGB = 1L;
    private static boolean readOnly = false;
    private static long[] baseEnvPtrOut = new long[1];
    private static long cloudEnvPtr = 0;

    public RocksetIndex(MainIndex mainIndex){
        this.dbHandle = CreateDBCloud(bucketName, s3Prefix, localDbPath,
                persistentCachePath, persistentCacheSizeGB, readOnly, baseEnvPtrOut, cloudEnvPtr);
        this.mainIndex = mainIndex;
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key) {
        try {
            // 生成复合键
            byte[] compositeKey = toByteArray(key);

            // 从 RocksDB 中获取值
            byte[] valueBytes = DBget(this.dbHandle, compositeKey);

            if (valueBytes != null) {
                return ByteBuffer.wrap(valueBytes).getLong();
            } else {
                System.out.println("No value found for composite key: " + key);
            }
        } catch (RuntimeException e) {
            LOGGER.error("Failed to get unique row ID for key: {}", key, e);
        }
        // 如果键不存在或发生异常，返回默认值（如 0）
        return 0;
    }

    @Override
    public long[] getRowIds(IndexProto.IndexKey key)
    {
        // List<Long> rowIdList = new ArrayList<>();

        // try {
        //     // 将 IndexKey 转换为字节数组（不包含 rowId）
        //     byte[] prefixBytes = toByteArray(key);

        //     // 使用 RocksDB 的迭代器进行前缀查找
        //     try (RocksIterator iterator = rocksDB.newIterator()) {
        //         for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
        //             byte[] currentKeyBytes = iterator.key();

        //             // 检查当前键是否以 prefixBytes 开头
        //             if (startsWith(currentKeyBytes, prefixBytes)) {
        //                 // 从键中提取 rowId
        //                 long rowId = extractRowIdFromKey(currentKeyBytes, prefixBytes.length);
        //                 rowIdList.add(rowId);
        //             } else {
        //                 break; // 如果不再匹配前缀，结束查找
        //             }
        //         }
        //     }
        //     // 将 List<Long> 转换为 long[]
        //     return parseRowIds(rowIdList);
        // } catch (Exception e) {
        //     LOGGER.error("Failed to get row IDs for key: {}", key, e);
        // }
        // 如果键不存在或发生异常，返回空数组
        return new long[0];
    }

    @Override
    public boolean putEntry(Entry entry)
    {
        try {
            // 为 Entry 获取 rowId
            mainIndex.getRowId(entry);
            // 从 Entry 对象中提取 key 和 rowId
            IndexProto.IndexKey key = entry.getKey();
            long rowId = entry.getRowId();
            boolean unique = entry.getIsUnique();
            // 将 IndexKey 转换为字节数组
            byte[] keyBytes = toByteArray(key);
            // 将 rowId 转换为字节数组
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
            // 检查 dbHandle 是否有效
            if (this.dbHandle == 0) {
                throw new IllegalStateException("RocksDB not initialized");
            }
            if (keyBytes.length == 0 || (unique && valueBytes.length == 0)) {
                throw new IllegalArgumentException("Key/Value cannot be empty");
            }
            if (unique) {
                // 写入 RocksDB
                DBput(this.dbHandle, keyBytes, valueBytes);
            } else {
                // 复合键值
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                //存储到 RocksDB
                DBput(this.dbHandle, nonUniqueKey, null);
            }
            return true;
        } catch (RuntimeException e) {
            LOGGER.error("Failed to put Entry: {} by entry", entry, e);
            return false;
        }
    }

    @Override
    public boolean putEntries(List<Entry> entries)
    {
        // 为 Entry 获取 rowId
        mainIndex.getRgOfRowIds(entries);
        try {
            // 遍历每个 Entry 对象
            for (Entry entry : entries) {
                // 从 Entry 对象中提取 key 和 rowId
                IndexProto.IndexKey key = entry.getKey();
                long rowId = entry.getRowId();
                boolean unique = entry.getIsUnique();
                // 将 IndexKey 转换为字节数组
                byte[] keyBytes = toByteArray(key);
                // 将 rowId 转换为字节数组
                byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(rowId).array();
                if(unique) {
                    // 写入 RocksDB
                    DBput(this.dbHandle, keyBytes, valueBytes);
                } else {
                    byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                    DBput(this.dbHandle, nonUniqueKey, null);
                }
            }
            return true; // 所有条目成功写入
        } catch (RuntimeException e) {
            LOGGER.error("Failed to put Entries: {} by entries", entries, e);
            return false; // 返回失败状态
        }
    }

    @Override
    public boolean deleteEntry(IndexProto.IndexKey key)
    {
        try {
            // 将 IndexKey 转换为字节数组
            byte[] keyBytes = toByteArray(key);

            // 从 RocksDB 中删除对应的键值对
            DBdelete(this.dbHandle, keyBytes);
            return true;
        } catch (RuntimeException e) {
            LOGGER.error("Failed to delete Entry: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean deleteEntries(List<IndexProto.IndexKey> keys)
    {
        try {
            for(IndexProto.IndexKey key : keys) {
                // 将 IndexKey 转换为字节数组
                byte[] keyBytes = toByteArray(key);

                // 从 RocksDB 中删除对应的键值对
                DBdelete(this.dbHandle, keyBytes);
            }
            return true;
        } catch (RuntimeException e) {
            LOGGER.error("Failed to delete Entries: {}", keys, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        CloseDB(this.dbHandle, this.baseEnvPtrOut[0], this.cloudEnvPtr); // 关闭 RocksDB 实例
    }
    private static byte[] toByteArray(IndexProto.IndexKey key) {
        byte[] indexIdBytes = ByteBuffer.allocate(Long.BYTES).putLong(key.getIndexId()).array(); // 获取 indexId 的二进制数据
        byte[] keyBytes = key.getKey().toByteArray(); // 获取 key 的二进制数据
        byte[] timestampBytes = ByteBuffer.allocate(Long.BYTES).putLong(key.getTimestamp()).array(); // 获取 timestamp 的二进制数据
        // 组合 indexId、key 和 timestamp
        byte[] compositeKey = new byte[indexIdBytes.length + 1 + keyBytes.length + 1 + timestampBytes.length];
        // 复制 indexId
        System.arraycopy(indexIdBytes, 0, compositeKey, 0, indexIdBytes.length);
        // 添加分隔符
        compositeKey[indexIdBytes.length] = ':';
        // 复制 key
        System.arraycopy(keyBytes, 0, compositeKey, indexIdBytes.length + 1, keyBytes.length);
        // 添加分隔符
        compositeKey[indexIdBytes.length + 1 + keyBytes.length] = ':';
        // 复制 timestamp
        System.arraycopy(timestampBytes, 0, compositeKey, indexIdBytes.length + 1 + keyBytes.length + 1, timestampBytes.length);

        return compositeKey;
    }
    // 复合键和rowId
    private static byte[] toNonUniqueKey(byte[] keyBytes, byte[] valueBytes) {
        byte[] nonUniqueKey = new byte[keyBytes.length + 1 + valueBytes.length];
        System.arraycopy(keyBytes, 0, nonUniqueKey, 0, keyBytes.length);
        nonUniqueKey[keyBytes.length] = ':';
        System.arraycopy(valueBytes, 0, nonUniqueKey, keyBytes.length + 1, valueBytes.length);
        return nonUniqueKey;
    }
    // 检查字节数组是否以指定前缀开头
    private boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
    // 从键中提取 rowId
    private long extractRowIdFromKey(byte[] keyBytes, int prefixLength) {
        // 提取 rowId 部分（键的最后 8 个字节）
        byte[] rowIdBytes = new byte[Long.BYTES];
        System.arraycopy(keyBytes, keyBytes.length - Long.BYTES, rowIdBytes, 0, Long.BYTES);

        // 将 rowId 转换为 long
        return ByteBuffer.wrap(rowIdBytes).getLong();
    }
    // 解析多个 rowId 的辅助方法
    private long[] parseRowIds(List<Long> rowIdList) {
        long[] rowIds = new long[rowIdList.size()];
        for (int i = 0; i < rowIdList.size(); i++) {
            rowIds[i] = rowIdList.get(i);
        }
        return rowIds;
    }
}