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
package io.pixelsdb.pixels.index.rockset;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.CachingSinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.rockset.jni.*;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.pixelsdb.pixels.index.rockset.RocksetThreadResources.EMPTY_VALUE_BUFFER;

public class RocksetIndex extends CachingSinglePointIndex
{
    private final RocksetIndexStub stub = new RocksetIndexStub();

    // --------- Small Java wrappers (null-safety & lifecycle) ----------
    protected long CreateDBCloud(@Nonnull CloudDBOptions dbOptions)
    {
        long cloudEnvPtr = stub.CreateCloudFileSystem0(dbOptions.getBucketName(), dbOptions.getS3Prefix());
        if (cloudEnvPtr == 0)
        {
            throw new RuntimeException("Failed to create CloudFileSystem");
        }

        long dbHandle = stub.OpenDBCloud0(cloudEnvPtr, dbOptions.getLocalDbPath(), dbOptions.getPersistentCachePath(),
                dbOptions.getPersistentCacheSizeGB(), dbOptions.isReadOnly());
        if (dbHandle == 0)
        {
            stub.CloseDB0(0);
            throw new RuntimeException("Failed to open DBCloud");
        }

        return dbHandle;
    }

    protected void DBput(long dbHandle, RocksetColumnFamilyHandle cf, RocksetWriteOptions wp, ByteBuffer key, ByteBuffer value)
    {
        stub.DBput0(dbHandle, cf.handle(), wp.handle(), byteBufferToByteArray(key), byteBufferToByteArray(value));
    }

    protected byte[] DBget(long dbHandle, byte[] key)
    {
        return stub.DBget0(dbHandle, key);
    }

    protected void DBdelete(long dbHandle, byte[] key)
    {
        stub.DBdelete0(dbHandle, key);
    }

    protected void CloseDB(long dbHandle)
    {
        if (dbHandle != 0)
        {
            stub.CloseDB0(dbHandle);
        }
    }

    // ---------------- Iterator wrapper methods ----------------
    protected long DBNewIterator(long dbHandle, RocksetColumnFamilyHandle cf, RocksetReadOptions readOptions)
    {
        return stub.DBNewIterator0(dbHandle, cf.handle(), readOptions.handle());
    }

    protected void IteratorSeekForPrev(long itHandle, byte[] targetKey)
    {
        stub.IteratorSeekForPrev0(itHandle, targetKey);
    }

    protected void IteratorSeek(long itHandle, ByteBuffer targetKey)
    {
        stub.IteratorSeek0(itHandle, byteBufferToByteArray(targetKey));
    }

    protected boolean IteratorIsValid(long itHandle)
    {
        return stub.IteratorIsValid0(itHandle);
    }

    protected byte[] IteratorKey(long itHandle)
    {
        return stub.IteratorKey0(itHandle);
    }

    protected byte[] IteratorValue(long itHandle)
    {
        return stub.IteratorValue0(itHandle);
    }

    protected void IteratorPrev(long itHandle)
    {
        stub.IteratorPrev0(itHandle);
    }

    protected void IteratorNext(long itHandle) {
        stub.IteratorNext0(itHandle);
    }

    protected void IteratorClose(long itHandle)
    {
        stub.IteratorClose0(itHandle);
    }

    // ---------------- WriteBatch wrapper methods ----------------
    protected long WriteBatchCreate()
    {
        return stub.WriteBatchCreate0();
    }

    protected void WriteBatchPut(long wbHandle, RocksetColumnFamilyHandle cf, ByteBuffer key, ByteBuffer value)
    {
        stub.WriteBatchPut0(wbHandle, cf.handle(), byteBufferToByteArray(key), byteBufferToByteArray(value));
    }

    protected void WriteBatchDelete(long wbHandle, RocksetColumnFamilyHandle cf, ByteBuffer key)
    {
        stub.WriteBatchDelete0(wbHandle, cf.handle(), byteBufferToByteArray(key));
    }

    protected boolean DBWrite(long dbHandle, long wbHandle)
    {
        return stub.DBWrite0(dbHandle, wbHandle);
    }

    protected void WriteBatchClear(long wbHandle)
    {
        stub.WriteBatchClear0(wbHandle);
    }

    protected void WriteBatchDestroy(long wbHandle)
    {
        stub.WriteBatchDestroy0(wbHandle);
    }

    private static final Logger LOGGER = LogManager.getLogger(RocksetIndex.class);

    private static final long TOMBSTONE_ROW_ID = Long.MAX_VALUE;
    private final long dbHandle;
    private final String rocksDBPath;
    private final RocksetWriteOptions writeOptions;
    private final RocksetColumnFamilyHandle columnFamilyHandle;
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean removed = new AtomicBoolean(false);

    public RocksetIndex(long tableId, long indexId, CloudDBOptions options, boolean unique) throws Exception {
        this.tableId = tableId;
        this.indexId = indexId;
        this.rocksDBPath = RocksetFactory.getDbPath();
        this.unique = unique;
        this.dbHandle = CreateDBCloud(options);
        this.writeOptions = RocksetWriteOptions.create();
        this.columnFamilyHandle = RocksetFactory.getOrCreateColumnFamily(tableId, indexId);
    }

    // ---------------- SinglePointIndex interface ----------------
    protected long getDbHandle()
    {
        return dbHandle;
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
    public long getUniqueRowIdInternal(IndexProto.IndexKey key) throws SinglePointIndexException {
        if (!unique)
        {
            throw new SinglePointIndexException("getUniqueRowId should only be called on unique index");
        }
        RocksetReadOptions readOptions = RocksetThreadResources.getReadOptions();
        readOptions.setPrefixSameAsStart(true);
        ByteBuffer keyBuffer = toKeyBuffer(key);
        long rowId = -1L;
        long it = 0;
        try
        {
            it = DBNewIterator(this.dbHandle, columnFamilyHandle, readOptions);
            IteratorSeek(it, keyBuffer);
            if (IteratorIsValid(it))
            {
                ByteBuffer keyFound = ByteBuffer.wrap(IteratorKey(it));
                if (startsWith(keyFound, keyBuffer))
                {
                    ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                    valueBuffer = ByteBuffer.wrap(IteratorValue(it));
                    rowId = valueBuffer.getLong();
                    rowId = rowId == TOMBSTONE_ROW_ID ? -1L : rowId;
                }
            }
        } catch (Exception e)
        {
            throw new SinglePointIndexException("Error reading from Rockset CF for tableId="
                    + tableId + ", indexId=" + indexId, e);
        }
        return rowId;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException {
        if (unique)
        {
            return ImmutableList.of(getUniqueRowId(key));
        }
        Set<Long> rowIds = new HashSet<>();
        RocksetReadOptions readOptions = RocksetThreadResources.getReadOptions();
        readOptions.setPrefixSameAsStart(true);
        ByteBuffer keyBuffer = toKeyBuffer(key);
        long it = 0;
        try
        {
            it = DBNewIterator(dbHandle, columnFamilyHandle, readOptions);
            IteratorSeek(it, keyBuffer);
            while (IteratorIsValid(it))
            {
                ByteBuffer keyFound = ByteBuffer.wrap(IteratorKey(it));
                if (startsWith(keyFound, keyBuffer))
                {
                    long rowId = extractRowIdFromKey(keyFound);
                    if (rowId == TOMBSTONE_ROW_ID)
                    {
                        break;
                    }
                    rowIds.add(rowId);
                    IteratorNext(it);
                }
                else
                {
                    break;
                }
            }
        } catch (Exception e) {
            throw new SinglePointIndexException("Error reading from Rockset CF for tableId="
                    + tableId + ", indexId=" + indexId, e);
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
                ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                DBput(dbHandle, columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                DBput(dbHandle, columnFamilyHandle, writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return true;
        }
        catch (RuntimeException e)
        {
            throw new SinglePointIndexException("Failed to put rockset index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        if (!unique)
        {
            throw new SinglePointIndexException("putPrimaryEntries can only be called on unique indexes");
        }
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                WriteBatchPut(wb, columnFamilyHandle, keyBuffer, valueBuffer);
            }
            DBWrite(dbHandle, wb);
            return true;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to put rockset primary index entries", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
        }
    }

    @Override
    public boolean putSecondaryEntriesInternal(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if (unique)
                {
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    WriteBatchPut(wb, columnFamilyHandle, keyBuffer, valueBuffer);
                }
                else
                {
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    WriteBatchPut(wb, columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            DBWrite(dbHandle, wb);
            return true;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to put rockset secondary index entries", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
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
            ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
            valueBuffer.putLong(rowId).position(0);
            DBput(this.dbHandle, columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            return prevRowId;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update primary entry", e);
        }
    }

    @Override
    public List<Long> updateSecondaryEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            ImmutableList.Builder<Long> prev = ImmutableList.builder();
            if (unique)
            {
                long prevRowId = getUniqueRowId(key);
                if (prevRowId >= 0)
                {
                    prev.add(prevRowId);
                }
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                DBput(this.dbHandle, columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            }
            else
            {
                prev.addAll(this.getRowIds(key));
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                DBput(this.dbHandle, columnFamilyHandle, writeOptions, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            return prev.build();
        }
        catch (Exception e)
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
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            ImmutableList.Builder<Long> prevRowIds = ImmutableList.builder();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                long prevRowId = getUniqueRowId(key);
                if (prevRowId >= 0)
                {
                    prevRowIds.add(prevRowId);
                }
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                valueBuffer.putLong(rowId).position(0);
                WriteBatchPut(wb, columnFamilyHandle, keyBuffer, valueBuffer);
            }
            DBWrite(this.dbHandle, wb);
            return prevRowIds.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update primary index entries", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
        }
    }

    @Override
    public List<Long> updateSecondaryEntriesInternal(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            ImmutableList.Builder<Long> prevRowIds = ImmutableList.builder();
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                if (unique)
                {
                    long prevRowId = getUniqueRowId(key);
                    if (prevRowId >= 0)
                    {
                        prevRowIds.add(prevRowId);
                    }
                    ByteBuffer keyBuffer = toKeyBuffer(key);
                    ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                    valueBuffer.putLong(rowId).position(0);
                    WriteBatchPut(wb, columnFamilyHandle, keyBuffer, valueBuffer);
                }
                else
                {
                    prevRowIds.addAll(this.getRowIds(key));
                    ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, rowId);
                    WriteBatchPut(wb, columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                }
            }
            DBWrite(this.dbHandle, wb);
            return prevRowIds.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to update secondary index entries", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
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
            ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
            valueBuffer.putLong(TOMBSTONE_ROW_ID).position(0);
            DBput(dbHandle, columnFamilyHandle, writeOptions, keyBuffer, valueBuffer);
            return rowId;
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to delete unique entry", e);
        }
    }

    @Override
    public List<Long> deleteEntryInternal(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        long wb = 0;
        try
        {
            ImmutableList.Builder<Long> prev = ImmutableList.builder();
            wb = WriteBatchCreate();
            if (unique)
            {
                long rowId = getUniqueRowId(key);
                if (rowId < 0)
                {
                    return ImmutableList.of();
                }
                prev.add(rowId);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                valueBuffer.putLong(TOMBSTONE_ROW_ID).position(0);
                WriteBatchPut(wb, columnFamilyHandle, keyBuffer, valueBuffer);
            }
            else
            {
                List<Long> rowIds = getRowIds(key);
                if (rowIds.isEmpty())
                {
                    return ImmutableList.of();
                }
                prev.addAll(rowIds);
                ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, TOMBSTONE_ROW_ID);
                WriteBatchPut(wb, columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
            }
            DBWrite(dbHandle, wb);
            return prev.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to delete entry", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
        }
    }

    @Override
    public List<Long> deleteEntriesInternal(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            ImmutableList.Builder<Long> prev = ImmutableList.builder();
            for (IndexProto.IndexKey key : keys)
            {
                if (unique)
                {
                    long rowId = getUniqueRowId(key);
                    if(rowId >= 0)
                    {
                        prev.add(rowId);
                        ByteBuffer keyBuffer = toKeyBuffer(key);
                        ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                        valueBuffer.putLong(TOMBSTONE_ROW_ID).position(0);
                        WriteBatchPut(wb, columnFamilyHandle, keyBuffer, valueBuffer);
                    }
                }
                else
                {
                    List<Long> rowIds = getRowIds(key);
                    if(!rowIds.isEmpty())
                    {
                        prev.addAll(rowIds);
                        ByteBuffer nonUniqueKeyBuffer = toNonUniqueKeyBuffer(key, TOMBSTONE_ROW_ID);
                        WriteBatchPut(wb, columnFamilyHandle, nonUniqueKeyBuffer, EMPTY_VALUE_BUFFER);
                    }
                }
            }
            DBWrite(this.dbHandle, wb);
            return prev.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to delete entries", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
        }
    }

    @Override
    public List<Long> purgeEntriesInternal(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            for (IndexProto.IndexKey key : indexKeys)
            {
                RocksetReadOptions readOptions = RocksetThreadResources.getReadOptions();
                readOptions.setPrefixSameAsStart(true);
                ByteBuffer keyBuffer = toKeyBuffer(key);
                long it = 0;
                try
                {
                    it = DBNewIterator(dbHandle, columnFamilyHandle, readOptions);
                    IteratorSeek(it, keyBuffer);
                    boolean foundTombstone = false;
                    while (IteratorIsValid(it))
                    {
                        ByteBuffer keyFound = ByteBuffer.wrap(IteratorKey(it));
                        if (startsWith(keyFound, keyBuffer))
                        {
                            long rowId;
                            if(unique)
                            {
                                ByteBuffer valueBuffer = RocksetThreadResources.getValueBuffer();
                                valueBuffer = ByteBuffer.wrap(IteratorValue(it));
                                rowId = valueBuffer.getLong();
                            }
                            else
                            {
                                rowId = extractRowIdFromKey(keyFound);
                            }
                            IteratorNext(it);
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
                            WriteBatchDelete(wb, columnFamilyHandle, ByteBuffer.wrap(keyFound.array()));
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                catch (Throwable t)
                {
                    LOGGER.error("purgeEntries failed", t);
                    return ImmutableList.of();
                }
                finally
                {
                    if (it != 0)
                        IteratorClose(it);
                }
            }
            DBWrite(this.dbHandle, wb);
            return builder.build();
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to purge entries by prefix", e);
        }
        finally
        {
            if (wb != 0)
            {
                WriteBatchClear(wb);
                WriteBatchDestroy(wb);
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        if (closed.compareAndSet(false, true))
        {
            // Issue #1158: do not directly close the rocksDB instance as it is shared by other indexes
            RocksetFactory.close();
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
                RocksetFactory.close();
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

    // ----------------- Encoding helpers -----------------
    protected static ByteBuffer toBuffer(long indexId, ByteString key, int bufferNum, long... postValues)
            throws SinglePointIndexException
    {
        int keySize = key.size();
        int totalLength = Long.BYTES + keySize + Long.BYTES * postValues.length;
        ByteBuffer compositeKey;
        if (bufferNum == 1)
        {
            compositeKey = RocksetThreadResources.getKeyBuffer(totalLength);
        }
        else if (bufferNum == 2)
        {
            compositeKey = RocksetThreadResources.getKeyBuffer2(totalLength);
        }
        else if (bufferNum == 3)
        {
            compositeKey = RocksetThreadResources.getKeyBuffer3(totalLength);
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

    private static byte[] writeLongBE(long v)
    {
        return ByteBuffer.allocate(Long.BYTES).putLong(v).array();
    }

    public static byte[] byteBufferToByteArray(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        byte[] byteArray = new byte[byteBuffer.remaining()];
        byteBuffer.get(byteArray);
        return byteArray;
    }
}
