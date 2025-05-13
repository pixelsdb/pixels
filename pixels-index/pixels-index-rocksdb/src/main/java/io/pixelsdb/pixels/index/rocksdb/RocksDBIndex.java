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
 * @create 2025-02-09
 */
public class RocksDBIndex implements SecondaryIndex
{
    // TODO: implement
    private final RocksDB rocksDB;
    public static final Logger LOGGER = LogManager.getLogger(RocksDBIndex.class);
    private final MainIndex mainIndex;

    public RocksDBIndex(String rocksDBPath, MainIndex mainIndex) throws RocksDBException {
        // 初始化 RocksDB 实例
        this.rocksDB = createRocksDB(rocksDBPath);
        this.mainIndex = mainIndex;
    }
    // 测试专用构造函数（直接注入 RocksDB）
    protected RocksDBIndex(RocksDB rocksDB, MainIndex mainIndex) {
        this.rocksDB = rocksDB;  // 直接使用注入的 Mock
        this.mainIndex = mainIndex;
    }

    protected RocksDB createRocksDB(String path) throws RocksDBException {
        // 1. 获取现有列族列表（如果是新数据库则返回空列表）
        List<byte[]> existingColumnFamilies;
        try {
            existingColumnFamilies = RocksDB.listColumnFamilies(new Options(), path);
        } catch (RocksDBException e) {
            // 如果是新数据库，返回只包含默认列族的列表
            existingColumnFamilies = Arrays.asList(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        // 2. 确保包含默认列族
        if (!existingColumnFamilies.contains(RocksDB.DEFAULT_COLUMN_FAMILY)) {
            existingColumnFamilies = new ArrayList<>(existingColumnFamilies);
            existingColumnFamilies.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        // 3. 准备列族描述
        List<ColumnFamilyDescriptor> descriptors = existingColumnFamilies.stream()
                .map(name -> new ColumnFamilyDescriptor(
                        name,
                        new ColumnFamilyOptions()
                ))
                .collect(Collectors.toList());

        // 4. 打开数据库
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true);

        return RocksDB.open(
                dbOptions,
                path,
                descriptors,
                handles
        );
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key) {
        try {
            // 生成复合键
            byte[] compositeKey = toByteArray(key);

            // 从 RocksDB 中获取值
            byte[] valueBytes = rocksDB.get(compositeKey);

            if (valueBytes != null) {
                return ByteBuffer.wrap(valueBytes).getLong();
            } else {
                System.out.println("No value found for composite key: " + key);
            }
        } catch (RocksDBException e) {
            LOGGER.error("Failed to get unique row ID for key: {}", key, e);
        }
        // 如果键不存在或发生异常，返回默认值（如 0）
        return 0;
    }

    @Override
    public long[] getRowIds(IndexProto.IndexKey key)
    {
        List<Long> rowIdList = new ArrayList<>();

        try {
            // 将 IndexKey 转换为字节数组（不包含 rowId）
            byte[] prefixBytes = toByteArray(key);

            // 使用 RocksDB 的迭代器进行前缀查找
            try (RocksIterator iterator = rocksDB.newIterator()) {
                for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
                    byte[] currentKeyBytes = iterator.key();

                    // 检查当前键是否以 prefixBytes 开头
                    if (startsWith(currentKeyBytes, prefixBytes)) {
                        // 从键中提取 rowId
                        long rowId = extractRowIdFromKey(currentKeyBytes, prefixBytes.length);
                        rowIdList.add(rowId);
                    } else {
                        break; // 如果不再匹配前缀，结束查找
                    }
                }
            }
            // 将 List<Long> 转换为 long[]
            return parseRowIds(rowIdList);
        } catch (Exception e) {
            LOGGER.error("Failed to get row IDs for key: {}", key, e);
        }
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
            if (unique) {
                // 写入 RocksDB
                rocksDB.put(keyBytes, valueBytes);
            } else {
                // 复合键值
                byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                //存储到 RocksDB
                rocksDB.put(nonUniqueKey, null);
            }
            return true;
        } catch (RocksDBException e) {
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
                    rocksDB.put(keyBytes, valueBytes);
                } else {
                    byte[] nonUniqueKey = toNonUniqueKey(keyBytes, valueBytes);
                    rocksDB.put(nonUniqueKey, null);
                }
            }
            return true; // 所有条目成功写入
        } catch (RocksDBException e) {
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
            rocksDB.delete(keyBytes);
            return true;
        } catch (RocksDBException e) {
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
                rocksDB.delete(keyBytes);
            }
            return true;
        } catch (RocksDBException e) {
            LOGGER.error("Failed to delete Entries: {}", keys, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        if (rocksDB != null) {
            rocksDB.close(); // 关闭 RocksDB 实例
        }
    }
    // 将 IndexKey 转换为字节数组
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
