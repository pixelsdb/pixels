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
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class MainIndexImpl implements MainIndex
{
    private static final Logger logger = LogManager.getLogger(MainIndexImpl.class);
    // Cache for storing generated rowIds
    private final List<Long> rowIdCache = new ArrayList<>();
    // Assumed batch size for generating rowIds
    private static final int BATCH_SIZE = 100000;
    // Get the singleton instance of EtcdUtil
    EtcdUtil etcdUtil = EtcdUtil.Instance();
    private boolean dirty = false; // Dirty flag

    public static class Entry
    {
        private final RowIdRange rowIdRange;
        private final RgLocation rgLocation;

        public Entry(RowIdRange rowIdRange, RgLocation rgLocation)
        {
            this.rowIdRange = rowIdRange;
            this.rgLocation = rgLocation;
        }

        public RowIdRange getRowIdRange()
        {
            return rowIdRange;
        }

        public RgLocation getRgLocation()
        {
            return rgLocation;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    @Override
    public IndexProto.RowLocation getLocation(long rowId)
    {
        // Use binary search to find the Entry containing the rowId
        int index = binarySearch(rowId);
        if (index >= 0) {
            Entry entry = entries.get(index);
            RgLocation rgLocation = entry.getRgLocation();
            return IndexProto.RowLocation.newBuilder()
                    .setFileId(rgLocation.getFileId())
                    .setRgId(rgLocation.getRowGroupId())
                    .setRgRowId((int) (rowId - entry.getRowIdRange().getStartRowId())) // Calculate the offset within the row group
                    .build();
        }
        return null; // Return null if not found
    }

    @Override
    public boolean putRowIdsOfRg(RowIdRange rowIdRangeOfRg, RgLocation rgLocation)
    {
        // Check if rowIdRangeOfRg overlaps with existing ranges
        if (isOverlapping(rowIdRangeOfRg)) {
            return false; // Insertion fails if overlapping
        }

        // Create new Entry and add to entries
        Entry newEntry = new Entry(rowIdRangeOfRg, rgLocation);
        entries.add(newEntry);

        // Sort entries by startRowId to maintain order
        entries.sort((e1, e2) -> Long.compare(e1.getRowIdRange().getStartRowId(), e2.getRowIdRange().getStartRowId()));

        // Set dirty flag
        dirty = true;

        return true;
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange rowIdRange)
    {
        // Find and remove Entries overlapping with rowIdRange
        boolean removed = entries.removeIf(entry -> isOverlapping(entry.getRowIdRange(), rowIdRange));

        // Set dirty flag
        if (removed) {
            dirty = true;
        }

        return true;
    }

    @Override
    public boolean getRowId(SecondaryIndex.Entry entry)
    {
        // Check if cache is empty
        if (rowIdCache.isEmpty()) {
            // Generate a new batch of rowIds into cache
            List<Long> newRowIds = loadRowIdsFromEtcd(BATCH_SIZE);
            if (newRowIds.isEmpty()) {
                logger.error("Failed to generate row ids");
                return false;
            }
            rowIdCache.addAll(newRowIds);
        }
        // Get the first rowId from cache
        long rowId = rowIdCache.remove(0);
        // Set the rowId in IndexEntry
        entry.setRowId(rowId);
        return true;
    }

    @Override
    public boolean getRgOfRowIds(List<SecondaryIndex.Entry> entries)
    {
        // Check if remaining rowIds in cache are sufficient
        if (rowIdCache.size() < entries.size()) {
            // If cache is insufficient, generate a new batch of rowIds
            int requiredCount = entries.size() - rowIdCache.size();
            List<Long> newRowIds = loadRowIdsFromEtcd(Math.max(requiredCount, BATCH_SIZE));
            if (newRowIds.isEmpty()) {
                logger.error("Failed to generate row ids");
                return false;
            }
            rowIdCache.addAll(newRowIds);
        }
        // Assign a rowId to each IndexEntry
        for (SecondaryIndex.Entry entry : entries) {
            long rowId = rowIdCache.remove(0); // Get a rowId from cache
            entry.setRowId(rowId);  // Set the rowId
        }
        return true;
    }

    @Override
    public boolean persist()
    {
        try {
            // Iterate through entries and persist each to etcd
            for (Entry entry : entries) {
                String key = "/mainindex/" + entry.getRowIdRange().getStartRowId();
                String value = serializeEntry(entry); // Serialize Entry to string
                etcdUtil.putKeyValue(key, value);
            }
            logger.info("Persisted {} entries to etcd", entries.size());
            return true;
        } catch (Exception e) {
            logger.error("Failed to persist entries to etcd", e);
            return false;
        }
    }

    public boolean persistIfDirty()
    {
        if (dirty) {
            if (persist()) {
                dirty = false; // Reset dirty flag
                return true;
            }
            return false;
        }
        return true; // No changes, no need to persist
    }

    @Override
    public void close() throws IOException
    {
        try {
            // Check dirty flag and persist to etcd if true
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

    private int binarySearch(long rowId)
    {
        int low = 0;
        int high = entries.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Entry entry = entries.get(mid);
            RowIdRange range = entry.getRowIdRange();

            if (rowId >= range.getStartRowId() && rowId <= range.getEndRowId()) {
                return mid; // Found the containing Entry
            } else if (rowId < range.getStartRowId()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return -1; // Not found
    }

    // Check if two RowIdRanges overlap
    private boolean isOverlapping(RowIdRange range1, RowIdRange range2)
    {
        return range1.getStartRowId() <= range2.getEndRowId() && range1.getEndRowId() >= range2.getStartRowId();
    }

    // Check if RowIdRange overlaps with existing ranges
    private boolean isOverlapping(RowIdRange newRange)
    {
        for (Entry entry : entries) {
            if (isOverlapping(entry.getRowIdRange(), newRange)) {
                return true;
            }
        }
        return false;
    }

    // Load a batch of rowIds from etcd
    private List<Long> loadRowIdsFromEtcd(int count)
    {
        List<Long> rowIds = new ArrayList<>();
        // Read all key-values with prefix /rowId/ from etcd
        List<KeyValue> keyValues = etcdUtil.getKeyValuesByPrefix("/rowId/");

        // Iterate through key-values to extract rowIds
        for (KeyValue kv : keyValues) {
            String key = kv.getKey().toString(StandardCharsets.UTF_8);
            // Assume key format is /rowId/{rowId}
            try {
                long rowId = Long.parseLong(key.substring("/rowId/".length()));
                rowIds.add(rowId);
                if (rowIds.size() >= count) {
                    break; // Stop when required count is reached
                }
            } catch (NumberFormatException e) {
                // Skip if key format is invalid
                logger.error("Invalid rowId format in etcd key: {}", key, e);
            }
        }
        // Batch delete these rowIds
        if (!rowIds.isEmpty()) {
            for (long rowId : rowIds) {
                etcdUtil.delete("/rowId/" + rowId);
            }
        }
        return rowIds;
    }

    // Serialize Entry
    private String serializeEntry(Entry entry)
    {
        return String.format("{\"startRowId\": %d, \"endRowId\": %d, \"fieldId\": %d, \"rowGroupId\": %d}",
                entry.getRowIdRange().getStartRowId(),
                entry.getRowIdRange().getEndRowId(),
                entry.getRgLocation().getFileId(),
                entry.getRgLocation().getRowGroupId());
    }
}