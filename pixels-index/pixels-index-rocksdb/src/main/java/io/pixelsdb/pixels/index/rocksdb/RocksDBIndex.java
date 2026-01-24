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
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.CachingSinglePointIndex;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.pixelsdb.pixels.index.rocksdb.RocksDBThreadResources.EMPTY_VALUE_BUFFER;

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndex extends CachingSinglePointIndex
{
    /**
     * Issue #1214: We use Long.MAX_VALUE instead of -1 as the tombstone row id, hence we can ensure the tombstone record
     * is always stored before the other versions of the same index entry.
     */
    private static final long TOMBSTONE_ROW_ID = Long.MAX_VALUE;
    private final RocksDB rocksDB;
    private final String rocksDBPath;
    private final WriteOptions writeOptions;
    private final ColumnFamilyHandle columnFamilyHandle;
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean removed = new AtomicBoolean(false);

    public RocksDBIndex(long tableId, long indexId, boolean unique, IndexOption indexOption) throws RocksDBException
    {
        super();
        this.tableId = tableId;
        this.indexId = indexId;
        // initialize RocksDB instance
        this.rocksDBPath = RocksDBFactory.getDbPath();
        this.rocksDB = RocksDBFactory.getRocksDB();
        this.unique = unique;
        this.writeOptions = new WriteOptions();
        this.columnFamilyHandle = RocksDBFactory.getOrCreateColumnFamily(tableId, indexId, indexOption.getVNodeId());
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
    public long getUniqueRowIdInternal(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("getUniqueRowId should only be called on unique index");
        }
        ReadOptions readOptions = RocksDBThreadResources.getReadOptions();
        readOptions.setPrefixSameAsStart(true)
                    .setTotalOrderSeek(false)
                    .setVerifyChecksums(false);
        ByteBuffer keyBuffer = toKeyBuffer(key);
        long rowId = -1L;
        try (RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle, readOptions))
        {
            iterator.seek(keyBuffer);
            if (iterator.isValid())
            {
                ByteBuffer keyFound = ByteBuffer.wrap(iterator.key());
                if (startsWith(keyFound, keyBuffer))
                {
                    ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                    iterator.value(valueBuffer);
                    rowId = valueBuffer.getLong();
                    rowId = rowId == TOMBSTONE_ROW_ID ? -1L : rowId;
                }
            }
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Error reading from RocksDB CF for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
        return rowId;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (unique)
        {
            return ImmutableList.of(getUniqueRowId(key));
        }
        Set<Long> rowIds = new HashSet<>();
        ReadOptions readOptions = RocksDBThreadResources.getReadOptions();
        readOptions.setPrefixSameAsStart(true);
        ByteBuffer keyBuffer = toKeyBuffer(key);
        // use RocksDB iterator for prefix search
        try (RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle, readOptions))
        {
            iterator.seek(keyBuffer);
            while (iterator.isValid())
            {
                ByteBuffer keyFound = ByteBuffer.wrap(iterator.key());
                if (startsWith(keyFound, keyBuffer))
                {
                    long rowId = extractRowIdFromKey(keyFound);
                    if (rowId == TOMBSTONE_ROW_ID)
                    {
                        break;
                    }
                    // Issue #1186: index keys with the same row id are considered as different versions of the same entry
                    rowIds.add(rowId);
                    iterator.next();
                }
                else
                {
                    break;
                }
            }
        }
        return ImmutableList.copyOf(rowIds);
    }

    @Override
    public boolean putEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            if (unique)
            {
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                rocksDB.put(columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                rocksDB.put(columnFamilyHandle, writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to put rocksdb index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("putPrimaryEntries can only be called on unique indexes");
        }
        try (WriteBatch writeBatch = new WriteBatch())
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                writeBatch.put(columnFamilyHandle, keyBuffer, valueBuffer);
            }
            rocksDB.write(writeOptions, writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to put primary index entries", e);
        }
    }

    @Override
    public boolean putSecondaryEntriesInternal(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if(unique)
                {
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    writeBatch.put(columnFamilyHandle, keyBuffer, valueBuffer);
                }
                else
                {
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    writeBatch.put(columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return true;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to put secondary index entries", e);
        }
    }

    @Override
    public long updatePrimaryEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("updatePrimaryEntry can only be called on unique indexes");
        }
        try
        {
            long prevRowId = getUniqueRowId(key);
            ByteBuffer keyBuffer = toKeyBuffer(key);
            ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
            valueBuffer.putLong(rowId).position(0);
            rocksDB.put(columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            return prevRowId;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to update primary entry", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            if(unique)
            {
                long prevRowId = getUniqueRowId(key);
                if (prevRowId >= 0)
                {
                    builder.add(prevRowId);
                }
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                rocksDB.put(columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                builder.addAll(this.getRowIds(key));
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                rocksDB.put(columnFamilyHandle, writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to update secondary entry", e);
        }
    }

    @Override
    public List<Long> updatePrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("updatePrimaryEntries can only be called on unique indexes");
        }
        try (WriteBatch writeBatch = new WriteBatch())
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                long prevRowId = getUniqueRowId(key);
                if (prevRowId >= 0)
                {
                    builder.add(prevRowId);
                }
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                writeBatch.put(columnFamilyHandle, keyBuffer, valueBuffer);
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to update primary index entries", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntriesInternal(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();

                if(unique)
                {
                    long prevRowId = getUniqueRowId(key);
                    if (prevRowId >= 0)
                    {
                        builder.add(prevRowId);
                    }
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    writeBatch.put(columnFamilyHandle, keyBuffer, valueBuffer);
                }
                else
                {
                    builder.addAll(this.getRowIds(key));
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    writeBatch.put(columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to update secondary index entries", e);
        }
    }

    @Override
    public long deleteUniqueEntryInternal(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("deleteUniqueEntry can only be called on unique indexes");
        }
        try
        {
            long rowId = getUniqueRowId(key);
            if (rowId < 0)
            {
                return rowId;
            }
            ByteBuffer keyBuffer = toKeyBuffer(key);
            ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
            valueBuffer.putLong(TOMBSTONE_ROW_ID).position(0);
            rocksDB.put(columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            return rowId;
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to delete unique index entry", e);
        }
    }

    @Override
    public List<Long> deleteEntryInternal(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            if(unique)
            {
                long rowId = getUniqueRowId(key);
                if(rowId < 0)
                {
                    return ImmutableList.of();
                }
                builder.add(rowId);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                valueBuffer.putLong(TOMBSTONE_ROW_ID).position(0);
                rocksDB.put(columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                List<Long> rowIds = getRowIds(key);
                if (rowIds.isEmpty())
                {
                    return rowIds;
                }
                builder.addAll(rowIds);
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, TOMBSTONE_ROW_ID);
                rocksDB.put(columnFamilyHandle, writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to delete index entry", e);
        }
    }

    @Override
    public List<Long> deleteEntriesInternal(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            // delete single point index
            for(IndexProto.IndexKey key : keys)
            {
                if(unique)
                {
                    long rowId = getUniqueRowId(key);
                    if(rowId >= 0)
                    {
                        builder.add(rowId);
                        ByteBuffer keyBuffer = toKeyBuffer(key);
                        ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                        valueBuffer.putLong(TOMBSTONE_ROW_ID).position(0);
                        writeBatch.put(columnFamilyHandle, keyBuffer, valueBuffer);
                    }
                }
                else
                {
                    List<Long> rowIds = getRowIds(key);
                    if(!rowIds.isEmpty())
                    {
                        builder.addAll(rowIds);
                        ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, TOMBSTONE_ROW_ID);
                        writeBatch.put(columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                    }
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to delete index entries", e);
        }
    }

    @Override
    public List<Long> purgeEntriesInternal(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        try (WriteBatch writeBatch = new WriteBatch())
        {
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (IndexProto.IndexKey key : indexKeys)
            {
                ReadOptions readOptions = RocksDBThreadResources.getReadOptions();
                readOptions.setPrefixSameAsStart(true);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                try (RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle, readOptions))
                {
                    iterator.seek(keyBuffer);
                    boolean foundTombstone = false;
                    while (iterator.isValid())
                    {
                        ByteBuffer keyFound = ByteBuffer.wrap(iterator.key());
                        if (startsWith(keyFound, keyBuffer))
                        {
                            long rowId;
                            if(unique)
                            {
                                ByteBuffer valueBuffer = RocksDBThreadResources.getValueBuffer();
                                iterator.value(valueBuffer);
                                rowId = valueBuffer.getLong();
                            }
                            else
                            {
                                rowId = extractRowIdFromKey(keyFound);
                            }
                            iterator.next();
                            if (rowId == TOMBSTONE_ROW_ID)
                            {
                                foundTombstone = true;
                            }
                            else if(foundTombstone)
                            {
                                builder.add(rowId);
                            }
                            else
                            {
                                continue;
                            }
                            // keyFound is not direct, must use its backing array
                            writeBatch.delete(columnFamilyHandle, keyFound.array());
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            rocksDB.write(writeOptions, writeBatch);
            return builder.build();
        }
        catch (RocksDBException e)
        {
            throw new SinglePointIndexException("Failed to purge index entries by prefix", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (closed.compareAndSet(false, true))
        {
            // Issue #1158: do not directly close the rocksDB instance as it is shared by other indexes
            RocksDBFactory.close();
            writeOptions.close();
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        if (closed.compareAndSet(false, true) && removed.compareAndSet(false, true))
        {
            try
            {
                // Issue #1158: do not directly close the rocksDB instance as it is shared by other indexes
                RocksDBFactory.close();
                writeOptions.close();
                FileUtils.deleteDirectory(new File(rocksDBPath));
            }
            catch (IOException e)
            {
                throw new SinglePointIndexException("Failed to close and cleanup the RocksDB index", e);
            }
            return true;
        }
        return false;
    }

    protected static ByteBuffer toBuffer(long indexId, ByteString key, int bufferNum, long... postValues)
            throws SinglePointIndexException
    {
        int keySize = key.size();
        int totalLength = Long.BYTES + keySize + Long.BYTES * postValues.length;
        ByteBuffer compositeKey;
        if (bufferNum == 1)
        {
            compositeKey = RocksDBThreadResources.getKeyBuffer(totalLength);
        }
        else if (bufferNum == 2)
        {
            compositeKey = RocksDBThreadResources.getKeyBuffer2(totalLength);
        }
        else if (bufferNum == 3)
        {
            compositeKey = RocksDBThreadResources.getKeyBuffer3(totalLength);
        }
        else
        {
            throw new SinglePointIndexException("Invalid buffer number");
        }
        // Write indexId (8 bytes, big endian)
        compositeKey.putLong(indexId);
        // Write key bytes (variable length)
        key.copyTo(compositeKey);
        // Write post values (8 bytes each, big endian)
        for (long postValue : postValues)
        {
            compositeKey.putLong(postValue);
        }
        compositeKey.position(0);
        return compositeKey;
    }

    protected static ByteBuffer toKeyBuffer(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 1, Long.MAX_VALUE - key.getTimestamp());
    }

    protected static ByteBuffer toNonUniqueKeyBuffer(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        return toBuffer(key.getIndexId(), key.getKey(), 1,
                Long.MAX_VALUE - key.getTimestamp(), Long.MAX_VALUE - rowId);
    }

    // check if byte array starts with specified prefix
    protected static boolean startsWith(ByteBuffer keyFound, ByteBuffer keyCurrent)
    {
        // prefix is indexId + key, without timestamp
        int prefixLength = keyCurrent.limit() - Long.BYTES;
        if (keyFound.limit() < prefixLength)
        {
            return false;
        }
        keyFound.position(0);
        keyCurrent.position(0);
        ByteBuffer keyFound1 = keyFound.slice();
        keyFound1.limit(prefixLength);
        ByteBuffer keyCurrent1 = keyCurrent.slice();
        keyCurrent1.limit(prefixLength);
        return keyFound1.compareTo(keyCurrent1) == 0;
    }

    // extract rowId from non-unique key
    protected static long extractRowIdFromKey(ByteBuffer keyBuffer)
    {
        // extract rowId portion (last 8 bytes of key)
        return Long.MAX_VALUE - keyBuffer.getLong(keyBuffer.limit() - Long.BYTES);
    }
}