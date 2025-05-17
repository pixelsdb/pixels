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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.index.IndexProto;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author hank
 * @create 2025-02-19
 */
public class MainIndexImpl implements MainIndex {
    private static final Logger logger = LogManager.getLogger(MainIndexImpl.class);
    // 缓存，用于存放生成的 rowId
    private final List<Long> rowIdCache = new ArrayList<>();
    // 假设每次生成的 rowId 数量
    private static final int BATCH_SIZE = 100000;
    // 获取 EtcdUtil 单例
    EtcdUtil etcdUtil = EtcdUtil.Instance();
    private boolean dirty = false; // 脏标记

    public static class Entry {
        private final RowIdRange rowIdRange;
        private final RgLocation rgLocation;

        public Entry(RowIdRange rowIdRange, RgLocation rgLocation) {
            this.rowIdRange = rowIdRange;
            this.rgLocation = rgLocation;
        }

        public RowIdRange getRowIdRange() {
            return rowIdRange;
        }

        public RgLocation getRgLocation() {
            return rgLocation;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    @Override
    public IndexProto.RowLocation getLocation(long rowId) {
        // 使用二分查找确定 rowId 所属的 Entry
        int index = binarySearch(rowId);
        if (index >= 0) {
            Entry entry = entries.get(index);
            RgLocation rgLocation = entry.getRgLocation();
            return IndexProto.RowLocation.newBuilder()
                    .setFileId(rgLocation.getFileId())
                    .setRgId(rgLocation.getRowGroupId())
                    .setRgRowId((int) (rowId - entry.getRowIdRange().getStartRowId())) // 计算行在行组中的偏移量
                    .build();
        }
        return null; // 如果未找到，返回 null
    }

    @Override
    public boolean putRowIdsOfRg(RowIdRange rowIdRangeOfRg, RgLocation rgLocation) {
        // 检查 rowIdRangeOfRg 是否与现有范围重叠
        if (isOverlapping(rowIdRangeOfRg)) {
            return false; // 如果重叠，插入失败
        }

        // 创建新的 Entry 并插入到 entries 中
        Entry newEntry = new Entry(rowIdRangeOfRg, rgLocation);
        entries.add(newEntry);

        // 按 startRowId 排序，确保 entries 有序
        entries.sort((e1, e2) -> Long.compare(e1.getRowIdRange().getStartRowId(), e2.getRowIdRange().getStartRowId()));

        // 设置脏标记
        dirty = true;

        return true;
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange rowIdRange) {
        // 查找并删除与 rowIdRange 重叠的 Entry
        boolean removed = entries.removeIf(entry -> isOverlapping(entry.getRowIdRange(), rowIdRange));

        // 设置脏标记
        if (removed) {
            dirty = true;
        }

        return true;
    }

    @Override
    public boolean getRowId(SecondaryIndex.Entry entry) {
        // 检查缓存是否为空
        if (rowIdCache.isEmpty()) {
            // 生成一批新的 rowId 到缓存
            List<Long> newRowIds = loadRowIdsFromEtcd(BATCH_SIZE);
            if (newRowIds.isEmpty()) {
                logger.error("Failed to generate row ids");
                return false;
            }
            rowIdCache.addAll(newRowIds);
        }
        // 从缓存中取出第一个 rowId
        long rowId = rowIdCache.remove(0);
        // 将 rowId 设置到 IndexEntry 中
        entry.setRowId(rowId);
        return true;
    }

    @Override
    public boolean getRgOfRowIds(List<SecondaryIndex.Entry> entries) {
        // 检查缓存中剩余的 rowId 数量是否足够分配
        if (rowIdCache.size() < entries.size()) {
            // 如果缓存中的 rowId 数量不足，生成一批新的 rowId
            int requiredCount = entries.size() - rowIdCache.size();
            List<Long> newRowIds = loadRowIdsFromEtcd(Math.max(requiredCount, BATCH_SIZE));
            if (newRowIds.isEmpty()) {
                logger.error("Failed to generate row ids");
                return false;
            }
            rowIdCache.addAll(newRowIds);
        }
        // 为每个 IndexEntry 分配一个 rowId
        for (SecondaryIndex.Entry entry : entries) {
            long rowId = rowIdCache.remove(0); // 从缓存中取出一个 rowId
            entry.setRowId(rowId);  // 设置 rowId
        }
        return true;
    }

    @Override
    public boolean persist()
    {
        try {
            // 遍历 entries，将每个 Entry 持久化到 etcd
            for (Entry entry : entries) {
                String key = "/mainindex/" + entry.getRowIdRange().getStartRowId();
                String value = serializeEntry(entry); // 将 Entry 序列化为字符串
                etcdUtil.putKeyValue(key, value);
            }
            logger.info("Persisted {} entries to etcd", entries.size());
            return true;
        } catch (Exception e) {
            logger.error("Failed to persist entries to etcd", e);
            return false;
        }
    }

    public boolean persistIfDirty() {
        if (dirty) {
            if (persist()) {
                dirty = false; // 重置脏标记
                return true;
            }
            return false;
        }
        return true; // 数据未变化，无需持久化
    }

    @Override
    public void close() throws IOException {
        try {
            // 检查脏标记，如果为 true，则持久化数据到 etcd
            if (!persistIfDirty()) {
                logger.error("Failed to persist data to etcd before closing");
                throw new IOException("Failed to persist data to etcd before closing");
            }
            logger.info("Data persisted to etcd successfully before closing");
        } catch (Exception e) {
            logger.error("Error occurred while closing MainIndexImpl", e);
            throw new IOException("Error occurred while closing MainIndexImpl", e);
        }
    }

    private int binarySearch(long rowId) {
        int low = 0;
        int high = entries.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Entry entry = entries.get(mid);
            RowIdRange range = entry.getRowIdRange();

            if (rowId >= range.getStartRowId() && rowId <= range.getEndRowId()) {
                return mid; // 找到所属的 Entry
            } else if (rowId < range.getStartRowId()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return -1; // 未找到
    }

    // 检查两个 RowIdRange 是否重叠
    private boolean isOverlapping(RowIdRange range1, RowIdRange range2) {
        return range1.getStartRowId() <= range2.getEndRowId() && range1.getEndRowId() >= range2.getStartRowId();
    }

    // 检查 RowIdRange 是否与现有范围重叠
    private boolean isOverlapping(RowIdRange newRange) {
        for (Entry entry : entries) {
            if (isOverlapping(entry.getRowIdRange(), newRange)) {
                return true;
            }
        }
        return false;
    }

    // 从etcd中加载一批rowId
    private List<Long> loadRowIdsFromEtcd(int count) {
        List<Long> rowIds = new ArrayList<>();
        // 从 etcd 中读取前缀为 /rowId/ 的所有键值对
        List<KeyValue> keyValues = etcdUtil.getKeyValuesByPrefix("/rowId/");

        // 遍历键值对，提取 rowId
        for (KeyValue kv : keyValues) {
            String key = kv.getKey().toString(StandardCharsets.UTF_8);
            // 假设 key 的格式为 /rowId/{rowId}
            try {
                long rowId = Long.parseLong(key.substring("/rowId/".length()));
                rowIds.add(rowId);
                if (rowIds.size() >= count) {
                    break; // 达到需要的数量后停止
                }
            } catch (NumberFormatException e) {
                // 如果 key 的格式不符合预期，跳过
                logger.error("Invalid rowId format in etcd key: {}", key, e);
            }
        }
        // 批量删除这些 rowId
        if (!rowIds.isEmpty()) {
            for (long rowId : rowIds) {
                etcdUtil.delete("/rowId/" + rowId);
            }
        }
        return rowIds;
    }

    // 序列化Entry
    private String serializeEntry(Entry entry) {
        return String.format("{\"startRowId\": %d, \"endRowId\": %d, \"fieldId\": %d, \"rowGroupId\": %d}",
                entry.getRowIdRange().getStartRowId(),
                entry.getRowIdRange().getEndRowId(),
                entry.getRgLocation().getFileId(),
                entry.getRgLocation().getRowGroupId());
    }
}