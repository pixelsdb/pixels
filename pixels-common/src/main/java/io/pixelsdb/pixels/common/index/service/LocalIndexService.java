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
package io.pixelsdb.pixels.common.index.service;

import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.*;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LocalIndexService implements IndexService
{
    private static final LocalIndexService defaultInstance = new LocalIndexService();
    private static boolean upsertMode;

    public static LocalIndexService Instance()
    {
        return defaultInstance;
    }

    private LocalIndexService()
    {
        upsertMode = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("retina.upsert-mode.enabled"));
    }

    @Override
    public IndexProto.RowIdBatch allocateRowIdBatch(long tableId, int numRowIds) throws IndexException
    {
        try
        {
            return MainIndexFactory.Instance().getMainIndex(tableId).allocateRowIdBatch(tableId, numRowIds);
        }
        catch (RowIdException | MainIndexException e)
        {
            throw new IndexException("Failed to allocate row ids for tableId=" + tableId, e);
        }
    }

    @Override
    public IndexProto.RowLocation lookupUniqueIndex(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException
    {
        // Delegates to resolvePrimary; only backend errors throw, everything else returns null.
        List<Optional<ResolvedPrimary>> resolved = resolvePrimary(
                key.getTableId(), key.getIndexId(), Collections.singletonList(key), indexOption);
        return resolved.get(0).map(ResolvedPrimary::getRowLocation).orElse(null);
    }

    @Override
    public List<IndexProto.RowLocation> lookupNonUniqueIndex(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException
    {
        try
        {
            long tableId = key.getTableId();
            long indexId = key.getIndexId();
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // Get all row IDs for the given index key
            List<Long> rowIds = singlePointIndex.getRowIds(key);
            List<IndexProto.RowLocation> rowLocations = new ArrayList<>();
            if (!rowIds.isEmpty())
            {
                // Iterate and resolve each rowId into a RowLocation
                for (long rowId : rowIds)
                {
                    IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);
                    if (rowLocation != null)
                    {
                        rowLocations.add(rowLocation);
                    }
                    else
                    {
                        // If any row location fails, stop and throw an exception
                        throw new IndexException("Failed to get row location for rowId=" + rowId);
                    }
                }
                return rowLocations;
            }
            else
            {
                return null;
            }
        }
        catch (SinglePointIndexException | MainIndexException e)
        {
            throw new IndexException("Failed to lookup non-unique index for key=" + key, e);
        }
    }

    @Override
    public boolean putPrimaryIndexEntry(IndexProto.PrimaryIndexEntry entry, IndexOption indexOption) throws IndexException
    {
        // Delegates to putPrimaryIndexEntries.
        IndexProto.IndexKey key = entry.getIndexKey();
        return putPrimaryIndexEntries(key.getTableId(), key.getIndexId(),
                Collections.singletonList(entry), indexOption);
    }

    @Override
    public boolean putPrimaryIndexEntries(long tableId, long indexId, List<IndexProto.PrimaryIndexEntry> entries, IndexOption indexOption) throws IndexException
    {
        if (entries == null || entries.isEmpty())
        {
            return true;
        }
        // Crash-safe order: MainIndex first (rowId -> RowLocation), then primary (IndexKey -> rowId).
        putMainIndexEntriesOnly(tableId, entries);
        putPrimaryIndexEntriesOnly(tableId, indexId, entries, indexOption);
        return true;
    }

    @Override
    public boolean putSecondaryIndexEntry(IndexProto.SecondaryIndexEntry entry, IndexOption indexOption) throws IndexException
    {
        try
        {
            IndexProto.IndexKey key = entry.getIndexKey();
            long tableId = key.getTableId();
            long indexId = key.getIndexId();
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // Insert into secondary index
            boolean success = singlePointIndex.putEntry(entry.getIndexKey(), entry.getRowId());
            if (!success)
            {
                throw new IndexException("Failed to put entry into secondary index for key=" + key);
            }
            return true;
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to put entry into secondary index for key=" + entry.getIndexKey(), e);
        }
    }

    @Override
    public boolean putSecondaryIndexEntries(long tableId, long indexId, List<IndexProto.SecondaryIndexEntry> entries, IndexOption indexOption) throws IndexException
    {
        try
        {
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            boolean success = singlePointIndex.putSecondaryEntries(entries);
            if (!success)
            {
                throw new IndexException("Failed to put secondary index entries for tableId=" + tableId + ", indexId=" + indexId);
            }
            return true;
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Error putting secondary index entries for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public IndexProto.RowLocation deletePrimaryIndexEntry(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException
    {
        try
        {
            long tableId = key.getTableId();
            long indexId = key.getIndexId();
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            long prevRowId = singlePointIndex.deleteUniqueEntry(key);
            if (prevRowId < 0)
            {
                if (upsertMode)
                {
                    return null;
                }
                throw new IndexException("Primary index entry not found for tableId=" + tableId + ", indexId=" + indexId);
            }
            IndexProto.RowLocation location = mainIndex.getLocation(prevRowId);
            if (location == null)
            {
                throw new IndexException("Failed to get row location for rowId=" + prevRowId + " (tableId=" + tableId + ")");
            }
            return location;
        }
        catch (MainIndexException | SinglePointIndexException e)
        {
            throw new IndexException("Error deleting primary index entry for tableId="
                    + key.getTableId() + ", indexId=" + key.getIndexId(), e);
        }
    }

    @Override
    public List<IndexProto.RowLocation> deletePrimaryIndexEntries(
            long tableId, long indexId, List<IndexProto.IndexKey> keys, IndexOption indexOption) throws IndexException
    {
        try
        {
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            List<Long> prevRowIds = singlePointIndex.deleteEntries(keys);
            if (prevRowIds == null || prevRowIds.isEmpty())
            {
                if (upsertMode)
                {
                    return new ArrayList<>();
                }
                throw new IndexException("Primary index entries not found for tableId="
                        + tableId + ", indexId=" + indexId);
            }
            List<IndexProto.RowLocation> locations = mainIndex.getLocations(prevRowIds);
            if (locations == null || locations.isEmpty())
            {
                throw new IndexException("Failed to get row locations for tableId=" + tableId + ", indexId=" + indexId);
            }
            return locations;
        }
        catch (MainIndexException | SinglePointIndexException e)
        {
            throw new IndexException("Error deleting primary index entries for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public List<Long> deleteSecondaryIndexEntry(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException
    {
        try
        {
            long tableId = key.getTableId();
            long indexId = key.getIndexId();
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            List<Long> prevRowIds = singlePointIndex.deleteEntry(key);
            if (prevRowIds == null || prevRowIds.isEmpty())
            {
                throw new IndexException("Failed to get previous row ids for tableId=" + tableId + ", indexId=" + indexId);
            }
            return prevRowIds;
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to delete secondary index entry for key=" + key, e);
        }
    }

    @Override
    public List<Long> deleteSecondaryIndexEntries(long tableId, long indexId, List<IndexProto.IndexKey> keys, IndexOption indexOption) throws IndexException
    {
        try
        {
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            List<Long> prevRowIds = singlePointIndex.deleteEntries(keys);
            if (prevRowIds == null || prevRowIds.isEmpty())
            {
                throw new IndexException("Failed to get previous row ids for tableId=" + tableId + ", indexId=" + indexId);
            }
            return prevRowIds;
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException(
                    "Failed to delete secondary index entries for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public IndexProto.RowLocation updatePrimaryIndexEntry(IndexProto.PrimaryIndexEntry indexEntry, IndexOption indexOption) throws IndexException
    {
        IndexProto.IndexKey key = indexEntry.getIndexKey();
        long tableId = key.getTableId();
        long indexId = key.getIndexId();
        try
        {
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // update the entry in the single point index and get the previous row ID
            long prevRowId = singlePointIndex.updatePrimaryEntry(key, indexEntry.getRowId());
            IndexProto.RowLocation prevLocation = null;
            if (prevRowId >= 0)
            {
                // retrieve the previous RowLocation from the main index
                prevLocation = mainIndex.getLocation(prevRowId);
                if (prevLocation == null)
                {
                    throw new IndexException("Failed to get previous row location for rowId=" + prevRowId);
                }
            }
            else
            {
                if (!upsertMode)
                {
                    throw new IndexException("Failed to get previous row id for tableId=" + tableId + ", indexId=" + indexId);
                }
            }
            boolean mainSuccess = mainIndex.putEntry(indexEntry.getRowId(), indexEntry.getRowLocation());
            if (!mainSuccess)
            {
                throw new MainIndexException("Failed to put entry into main index for rowId=" + indexEntry.getRowId());
            }
            return prevLocation;
        }
        catch (MainIndexException e)
        {
            throw new IndexException(
                    "Failed to update primary index entry in main index for tableId=" + tableId, e);
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException(
                    "Failed to update primary index entry in single point index for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public List<IndexProto.RowLocation> updatePrimaryIndexEntries
            (long tableId, long indexId, List<IndexProto.PrimaryIndexEntry> indexEntries, IndexOption indexOption) throws IndexException
    {
        try
        {
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // update multiple entries in the single point index, returning previous row IDs
            List<Long> prevRowIds = singlePointIndex.updatePrimaryEntries(indexEntries);
            if (prevRowIds == null || prevRowIds.isEmpty())
            {
                if (!upsertMode)
                {
                    throw new IndexException("Failed to get previous row ids for tableId=" + tableId + ", indexId=" + indexId);
                }
                prevRowIds = new ArrayList<>();
            }
            List<IndexProto.RowLocation> prevRowLocations = mainIndex.getLocations(prevRowIds);
            if (prevRowLocations == null || prevRowLocations.isEmpty())
            {
                if (!(upsertMode && prevRowIds.isEmpty()))
                {
                    throw new IndexException("Failed to get previous row locations for tableId=" +
                            tableId + ", indexId=" + indexId);
                }

            }
            for (Boolean mainSuccess : mainIndex.putEntries(indexEntries))
            {
                if(!mainSuccess)
                {
                    throw new MainIndexException("Failed to update entries in main index: " + tableId);
                }
            }
            return prevRowLocations;
        }
        catch (MainIndexException e)
        {
            throw new IndexException("Failed to update primary index entries in main index for tableId=" + tableId, e);
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to update primary index entries in single point index for tableId=" +
                    tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public List<Long> updateSecondaryIndexEntry(IndexProto.SecondaryIndexEntry indexEntry, IndexOption indexOption) throws IndexException
    {
        IndexProto.IndexKey key = indexEntry.getIndexKey();
        long tableId = key.getTableId();
        long indexId = key.getIndexId();
        try
        {
            // get the single point index for the table and index ID
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // update the secondary index entry and return previous row IDs
            List<Long> prevRowIds = singlePointIndex.updateSecondaryEntry(key, indexEntry.getRowId());
            if (prevRowIds == null || prevRowIds.isEmpty())
            {
                throw new IndexException("Failed to get previous row ids for tableId=" + tableId + ", indexId=" + indexId);
            }
            return prevRowIds;
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException(
                    "Failed to update secondary index entry for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public List<Long> updateSecondaryIndexEntries
            (long tableId, long indexId, List<IndexProto.SecondaryIndexEntry> indexEntries, IndexOption indexOption) throws IndexException
    {
        try
        {
            // get the single point index for the table and index ID
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // update all secondary index entries and return previous row IDs
            List<Long> prevRowIds = singlePointIndex.updateSecondaryEntries(indexEntries);
            if (prevRowIds == null || prevRowIds.isEmpty())
            {
                throw new IndexException("Failed to get previous row ids for tableId=" + tableId + ", indexId=" + indexId);
            }
            return prevRowIds;
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException(
                    "Failed to update secondary index entries for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public boolean purgeIndexEntries
            (long tableId, long indexId, List<IndexProto.IndexKey> indexKeys, boolean isPrimary, IndexOption indexOption) throws IndexException
    {
        try
        {
            // get the single point index for the table and index
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // purge the entries from the index
            List<Long> rowIds = singlePointIndex.purgeEntries(indexKeys);
            if (rowIds == null || rowIds.isEmpty())
            {
                // no entries found to purge
                return false;
            }
            if (isPrimary)
            {
                // if primary index, delete corresponding rows from MainIndex
                MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
                int last = rowIds.size() - 1;
                IndexProto.RowLocation rowLocationFirst = mainIndex.getLocation(rowIds.get(0));
                IndexProto.RowLocation rowLocationLast = mainIndex.getLocation(rowIds.get(last));

                RowIdRange rowIdRange = new RowIdRange(
                        rowIds.get(0), rowIds.get(last),
                        rowLocationFirst.getFileId(),
                        rowLocationFirst.getRgId(),
                        rowLocationFirst.getRgRowOffset(),
                        rowLocationLast.getRgRowOffset()
                );
                if (mainIndex.hasCache())
                {
                    mainIndex.flushCache(rowLocationFirst.getFileId());
                }
                mainIndex.deleteRowIdRange(rowIdRange);
            }
            return true;
        }
        catch (MainIndexException e)
        {
            throw new IndexException("Failed to purge main index entries for tableId=" + tableId, e);
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to purge single point index entries for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public boolean flushIndexEntriesOfFile
            (long tableId, long indexId, long fileId, boolean isPrimary, IndexOption indexOption) throws IndexException
    {
        try
        {
            if (isPrimary)
            {
                // get the MainIndex for the table
                MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
                if (mainIndex == null)
                {
                    // MainIndex not found
                    return false;
                }
                // flush cache of the specified file
                mainIndex.flushCache(fileId);
            }
            return true;
        }
        catch (MainIndexException e)
        {
            throw new IndexException("Failed to flush main index for tableId=" + tableId + ", fileId=" + fileId, e);
        }
    }

    @Override
    public boolean openIndex(long tableId, long indexId, boolean isPrimary, IndexOption indexOption) throws IndexException
    {
        try
        {
            // get the single-point index
            SinglePointIndex singlePointIndex = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            if (singlePointIndex == null)
            {
                throw new IndexException("Failed to open single-point index for tableId=" + tableId + ", indexId=" + indexId);
            }
            // if it's a primary index, ensure the main index exists
            if (isPrimary)
            {
                MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
                if (mainIndex == null)
                {
                    throw new IndexException("Failed to open main index for tableId=" + tableId);
                }
            }
            return true;
        }
        catch (SinglePointIndexException | MainIndexException e)
        {
            throw new IndexException("Failed to open index for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public boolean closeIndex(long tableId, long indexId, boolean isPrimary, IndexOption option) throws IndexException
    {
        try
        {
            // close the single-point index
            SinglePointIndexFactory.Instance().closeIndex(tableId, indexId, false, option);
            // if it's a primary index, also close the main index
            if (isPrimary)
            {
                MainIndexFactory.Instance().closeIndex(tableId, false);
            }
            return true;
        }
        catch (SinglePointIndexException | MainIndexException e)
        {
            throw new IndexException("Failed to close index for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public boolean removeIndex(long tableId, long indexId, boolean isPrimary, IndexOption option) throws IndexException
    {
        try
        {
            // close and remove the single-point index completely
            SinglePointIndexFactory.Instance().closeIndex(tableId, indexId, true, option);
            // if it's a primary index, also remove the main index completely
            if (isPrimary)
            {
                MainIndexFactory.Instance().closeIndex(tableId, true);
            }
            return true;
        }
        catch (SinglePointIndexException | MainIndexException e)
        {
            throw new IndexException("Failed to remove index for tableId=" + tableId + ", indexId=" + indexId, e);
        }
    }

    // ==================================================================================
    // Staged primary-index APIs. Contracts live on the matching IndexService methods.
    // ==================================================================================

    @Override
    public List<Optional<ResolvedPrimary>> resolvePrimary(long tableId, long indexId,
            List<IndexProto.IndexKey> keys, IndexOption indexOption) throws IndexException
    {
        if (keys == null || keys.isEmpty())
        {
            return Collections.emptyList();
        }
        try
        {
            SinglePointIndex sp = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            MainIndex mi = MainIndexFactory.Instance().getMainIndex(tableId);
            List<Optional<ResolvedPrimary>> result = new ArrayList<>(keys.size());
            for (IndexProto.IndexKey key : keys)
            {
                long rowId = sp.getUniqueRowId(key);
                if (rowId < 0)
                {
                    // missing or tombstoned in primary
                    result.add(Optional.empty());
                    continue;
                }
                IndexProto.RowLocation location = mi.getLocation(rowId);
                if (location == null)
                {
                    // MainIndex orphan rowId
                    result.add(Optional.empty());
                    continue;
                }
                result.add(Optional.of(new ResolvedPrimary(rowId, location)));
            }
            return result;
        }
        catch (SinglePointIndexException | MainIndexException e)
        {
            throw new IndexException("Failed to resolve primary for tableId=" + tableId
                    + ", indexId=" + indexId, e);
        }
    }

    @Override
    public void putMainIndexEntriesOnly(long tableId, List<IndexProto.PrimaryIndexEntry> entries) throws IndexException
    {
        if (entries == null || entries.isEmpty())
        {
            return;
        }
        try
        {
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            List<Boolean> results = mainIndex.putEntries(entries);
            for (Boolean ok : results)
            {
                if (ok == null || !ok)
                {
                    throw new IndexException("Failed to put main index entry, tableId=" + tableId);
                }
            }
        }
        catch (MainIndexException e)
        {
            throw new IndexException("Failed to put main index entries for tableId=" + tableId, e);
        }
    }

    @Override
    public void putPrimaryIndexEntriesOnly(long tableId, long indexId,
            List<IndexProto.PrimaryIndexEntry> entries, IndexOption indexOption) throws IndexException
    {
        if (entries == null || entries.isEmpty())
        {
            return;
        }
        try
        {
            SinglePointIndex sp = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            if (!sp.putPrimaryEntries(entries))
            {
                throw new IndexException("Failed to put primary entries into single point index for tableId="
                        + tableId + ", indexId=" + indexId);
            }
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to put primary entries into single point index for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public void deletePrimaryIndexEntriesOnly(long tableId, long indexId,
            List<IndexProto.IndexKey> resolvedKeys, IndexOption indexOption) throws IndexException
    {
        if (resolvedKeys == null || resolvedKeys.isEmpty())
        {
            return;
        }
        try
        {
            SinglePointIndex sp = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // TODO: avoid the repeated primary lookup by adding a tombstone-only index API.
            sp.deleteEntries(resolvedKeys);
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to delete primary entries for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public void updatePrimaryIndexEntriesOnly(long tableId, long indexId,
            List<IndexProto.PrimaryIndexEntry> entries, IndexOption indexOption) throws IndexException
    {
        if (entries == null || entries.isEmpty())
        {
            return;
        }
        try
        {
            SinglePointIndex sp = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            // TODO: avoid the repeated primary lookup by adding an update API that accepts resolved rowIds.
            sp.updatePrimaryEntries(entries);
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to update primary entries for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public void restorePrimaryIndexEntries(long tableId, long indexId,
            List<RollbackEntry> entries, IndexOption indexOption) throws IndexException
    {
        if (entries == null || entries.isEmpty())
        {
            return;
        }
        // RECOVERING is single-threaded for these entries; read-then-write needs no CAS.
        try
        {
            SinglePointIndex sp = SinglePointIndexFactory.Instance().getSinglePointIndex(tableId, indexId, indexOption);
            List<IndexProto.PrimaryIndexEntry> toRestore = new ArrayList<>();
            for (RollbackEntry entry : entries)
            {
                long current = sp.getUniqueRowId(entry.getIndexKey());
                if (current == entry.getNewRowId())
                {
                    toRestore.add(IndexProto.PrimaryIndexEntry.newBuilder()
                            .setIndexKey(entry.getIndexKey())
                            .setRowId(entry.getOldRowId())
                            .build());
                }
                // else: primary already tombstoned, reverted, or moved on; skip.
            }
            if (!toRestore.isEmpty())
            {
                sp.updatePrimaryEntries(toRestore);
            }
        }
        catch (SinglePointIndexException e)
        {
            throw new IndexException("Failed to restore primary entries for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
    }

    @Override
    public void deleteMainIndexRange(long tableId, long fileId, long rowIdStart, int rowCount)
            throws IndexException
    {
        if (rowCount <= 0)
        {
            return;
        }
        try
        {
            MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
            if (mainIndex.hasCache())
            {
                mainIndex.flushCache(fileId);
            }
            RowIdRange rowIdRange = new RowIdRange(rowIdStart, rowIdStart + rowCount,
                    fileId, 0, 0, rowCount);
            if (!mainIndex.deleteRowIdRange(rowIdRange))
            {
                throw new IndexException("Failed to delete main index range for tableId="
                        + tableId + ", fileId=" + fileId);
            }
        }
        catch (MainIndexException e)
        {
            throw new IndexException("Failed to delete main index range for tableId="
                    + tableId + ", fileId=" + fileId, e);
        }
    }
}
