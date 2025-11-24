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
package io.pixelsdb.pixels.index.mapdb;

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MapDB-based implementation of SinglePointIndex
 *
 * @author hank
 * @create 2025-11-24
 */
public class MapDBIndex implements SinglePointIndex
{

    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private final DB db;
    private final HTreeMap<byte[], byte[]> indexMap;
    private final String dbFilePath;
    private final boolean isPersistent;

    /**
     * Constructor for persistent MapDB index
     */
    public MapDBIndex(long tableId, long indexId, boolean unique, String dbFilePath)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.unique = unique;
        this.dbFilePath = dbFilePath;
        this.isPersistent = true;

        // Create parent directory if it doesn't exist
        File dbFile = new File(dbFilePath);
        File parentDir = dbFile.getParentFile();
        if (parentDir != null && !parentDir.exists())
        {
            parentDir.mkdirs();
        }

        this.db = DBMaker.fileDB(dbFile)
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .transactionEnable()
                .make();

        String mapName = "index_" + tableId + "_" + indexId;
        this.indexMap = db.hashMap(mapName)
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    /**
     * Constructor for in-memory MapDB index
     */
    public MapDBIndex(long tableId, long indexId, boolean unique)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.unique = unique;
        this.dbFilePath = null;
        this.isPersistent = false;

        this.db = DBMaker.memoryDB()
                .closeOnJvmShutdown()
                .transactionEnable()
                .make();

        String mapName = "index_" + tableId + "_" + indexId;
        this.indexMap = db.hashMap(mapName)
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @Override
    public long getTableId()
    {
        return tableId;
    }

    @Override
    public long getIndexId()
    {
        return indexId;
    }

    @Override
    public boolean isUnique()
    {
        return unique;
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("getUniqueRowId should only be called on unique index");
        }

        byte[] keyBytes = serializeIndexKey(key);
        byte[] valueBytes = indexMap.get(keyBytes);

        if (valueBytes == null)
        {
            return -1L;
        }

        try
        {
            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.parseFrom(valueBytes);
            return entry.getRowId();
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse primary index entry", e);
        }
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        byte[] keyBytes = serializeIndexKey(key);
        byte[] valueBytes = indexMap.get(keyBytes);

        if (valueBytes == null)
        {
            return Collections.emptyList();
        }

        try
        {
            List<Long> rowIds = new ArrayList<>();
            if (unique)
            {
                // For unique index, parse as PrimaryIndexEntry
                IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.parseFrom(valueBytes);
                rowIds.add(entry.getRowId());
            } else
            {
                // For non-unique index, parse as SecondaryIndexEntry
                IndexProto.SecondaryIndexEntry entry = IndexProto.SecondaryIndexEntry.parseFrom(valueBytes);
                rowIds.add(entry.getRowId());
                // Note: For multiple row IDs with same key in non-unique index,
                // we need to handle this differently - see implementation notes below
            }
            return rowIds;
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse index entry", e);
        }
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        byte[] keyBytes = serializeIndexKey(key);

        try
        {
            byte[] valueBytes;
            if (unique)
            {
                // For unique index, store PrimaryIndexEntry
                IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                        .setIndexKey(key)
                        .setRowId(rowId)
                        // RowLocation is not available at this level, will be set by higher level
                        .build();
                valueBytes = entry.toByteArray();
            } else
            {
                // For non-unique index, store SecondaryIndexEntry
                IndexProto.SecondaryIndexEntry entry = IndexProto.SecondaryIndexEntry.newBuilder()
                        .setIndexKey(key)
                        .setRowId(rowId)
                        .build();
                valueBytes = entry.toByteArray();
            }

            indexMap.put(keyBytes, valueBytes);

            if (isPersistent)
            {
                db.commit();
            }
            return true;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to put index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries)
            throws MainIndexException, SinglePointIndexException
    {
        try
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                byte[] keyBytes = serializeIndexKey(key);
                byte[] valueBytes = entry.toByteArray();
                indexMap.put(keyBytes, valueBytes);
            }

            if (isPersistent)
            {
                db.commit();
            }
            return true;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to put primary index entries", e);
        }
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        try
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                byte[] keyBytes = serializeIndexKey(key);
                byte[] valueBytes = entry.toByteArray();
                indexMap.put(keyBytes, valueBytes);
            }

            if (isPersistent)
            {
                db.commit();
            }
            return true;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to put secondary index entries", e);
        }
    }

    @Override
    public long updatePrimaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("updatePrimaryEntry should only be called on unique index");
        }

        byte[] keyBytes = serializeIndexKey(key);

        try
        {
            byte[] existingValue = indexMap.get(keyBytes);
            long previousRowId = -1L;

            if (existingValue != null)
            {
                IndexProto.PrimaryIndexEntry existingEntry = IndexProto.PrimaryIndexEntry.parseFrom(existingValue);
                previousRowId = existingEntry.getRowId();
            }

            IndexProto.PrimaryIndexEntry newEntry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(key)
                    .setRowId(rowId)
                    .build();
            byte[] valueBytes = newEntry.toByteArray();
            indexMap.put(keyBytes, valueBytes);

            if (isPersistent)
            {
                db.commit();
            }
            return previousRowId;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to update primary index entry", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        byte[] keyBytes = serializeIndexKey(key);

        try
        {
            byte[] existingValue = indexMap.get(keyBytes);
            List<Long> previousRowIds = new ArrayList<>();

            if (existingValue != null)
            {
                IndexProto.SecondaryIndexEntry existingEntry = IndexProto.SecondaryIndexEntry.parseFrom(existingValue);
                previousRowIds.add(existingEntry.getRowId());
            }

            IndexProto.SecondaryIndexEntry newEntry = IndexProto.SecondaryIndexEntry.newBuilder()
                    .setIndexKey(key)
                    .setRowId(rowId)
                    .build();
            byte[] valueBytes = newEntry.toByteArray();
            indexMap.put(keyBytes, valueBytes);

            if (isPersistent)
            {
                db.commit();
            }
            return previousRowIds;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to update secondary index entry", e);
        }
    }

    @Override
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        List<Long> previousRowIds = new ArrayList<>();

        try
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                long previousRowId = updatePrimaryEntry(key, rowId);
                if (previousRowId >= 0)
                {
                    previousRowIds.add(previousRowId);
                }
            }

            if (isPersistent)
            {
                db.commit();
            }
            return previousRowIds;
        } catch (SinglePointIndexException e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw e;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to update primary index entries", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        List<Long> previousRowIds = new ArrayList<>();

        try
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                List<Long> entryPreviousRowIds = updateSecondaryEntry(key, rowId);
                previousRowIds.addAll(entryPreviousRowIds);
            }

            if (isPersistent)
            {
                db.commit();
            }
            return previousRowIds;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to update secondary index entries", e);
        }
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey indexKey) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("deleteUniqueEntry should only be called on unique index");
        }

        byte[] keyBytes = serializeIndexKey(indexKey);

        try
        {
            byte[] existingValue = indexMap.get(keyBytes);
            long deletedRowId = -1L;

            if (existingValue != null)
            {
                IndexProto.PrimaryIndexEntry existingEntry = IndexProto.PrimaryIndexEntry.parseFrom(existingValue);
                deletedRowId = existingEntry.getRowId();
                indexMap.remove(keyBytes);
            }

            if (isPersistent)
            {
                db.commit();
            }
            return deletedRowId;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to delete unique index entry", e);
        }
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey indexKey) throws SinglePointIndexException
    {
        byte[] keyBytes = serializeIndexKey(indexKey);

        try
        {
            byte[] existingValue = indexMap.get(keyBytes);
            List<Long> deletedRowIds = new ArrayList<>();

            if (existingValue != null)
            {
                if (unique)
                {
                    IndexProto.PrimaryIndexEntry existingEntry = IndexProto.PrimaryIndexEntry.parseFrom(existingValue);
                    deletedRowIds.add(existingEntry.getRowId());
                } else
                {
                    IndexProto.SecondaryIndexEntry existingEntry = IndexProto.SecondaryIndexEntry.parseFrom(existingValue);
                    deletedRowIds.add(existingEntry.getRowId());
                }
                indexMap.remove(keyBytes);
            }

            if (isPersistent)
            {
                db.commit();
            }
            return deletedRowIds;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to delete index entry", e);
        }
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        List<Long> deletedRowIds = new ArrayList<>();

        try
        {
            for (IndexProto.IndexKey indexKey : indexKeys)
            {
                List<Long> entryDeletedRowIds = deleteEntry(indexKey);
                deletedRowIds.addAll(entryDeletedRowIds);
            }

            if (isPersistent)
            {
                db.commit();
            }
            return deletedRowIds;
        } catch (SinglePointIndexException e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw e;
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to delete index entries", e);
        }
    }

    @Override
    public List<Long> purgeEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        // For MapDB implementation, purge is the same as delete since we don't have a separate tombstone mechanism
        return deleteEntries(indexKeys);
    }

    @Override
    @Deprecated
    public void close() throws IOException
    {
        if (db != null && !db.isClosed())
        {
            db.close();
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        try
        {
            if (db != null && !db.isClosed())
            {
                db.close();
            }

            if (isPersistent && dbFilePath != null)
            {
                File dbFile = new File(dbFilePath);
                if (dbFile.exists())
                {
                    return dbFile.delete();
                }
            }

            return true;
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to close and remove MapDB index", e);
        }
    }

    /**
     * Serialize IndexKey to byte array for use as MapDB key
     */
    private byte[] serializeIndexKey(IndexProto.IndexKey key)
    {
        return key.toByteArray();
    }

    /**
     * Get the number of entries in this index
     *
     * @return the size of the index
     */
    public long size()
    {
        return indexMap.size();
    }

    /**
     * Check if the index is empty
     *
     * @return true if the index is empty
     */
    public boolean isEmpty()
    {
        return indexMap.isEmpty();
    }

    /**
     * Clear all entries from the index
     *
     * @throws SinglePointIndexException
     */
    public void clear() throws SinglePointIndexException
    {
        try
        {
            indexMap.clear();
            if (isPersistent)
            {
                db.commit();
            }
        } catch (Exception e)
        {
            if (isPersistent)
            {
                db.rollback();
            }
            throw new SinglePointIndexException("Failed to clear index", e);
        }
    }

    /**
     * Get primary index entry by key
     */
    public IndexProto.PrimaryIndexEntry getPrimaryEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("getPrimaryEntry should only be called on unique index");
        }

        byte[] keyBytes = serializeIndexKey(key);
        byte[] valueBytes = indexMap.get(keyBytes);

        if (valueBytes == null)
        {
            return null;
        }

        try
        {
            return IndexProto.PrimaryIndexEntry.parseFrom(valueBytes);
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse primary index entry", e);
        }
    }

    /**
     * Get secondary index entry by key
     */
    public IndexProto.SecondaryIndexEntry getSecondaryEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (unique)
        {
            throw new SinglePointIndexException("getSecondaryEntry should only be called on non-unique index");
        }

        byte[] keyBytes = serializeIndexKey(key);
        byte[] valueBytes = indexMap.get(keyBytes);

        if (valueBytes == null)
        {
            return null;
        }

        try
        {
            return IndexProto.SecondaryIndexEntry.parseFrom(valueBytes);
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse secondary index entry", e);
        }
    }
}