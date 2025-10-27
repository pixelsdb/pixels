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
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.CachingSinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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

    protected void DBput(long dbHandle, byte[] key, byte[] value)
    {
        stub.DBput0(dbHandle, key, value);
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
    protected long DBNewIterator(long dbHandle)
    {
        return stub.DBNewIterator0(dbHandle);
    }

    protected void IteratorSeekForPrev(long itHandle, byte[] targetKey)
    {
        stub.IteratorSeekForPrev0(itHandle, targetKey);
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

    protected void IteratorClose(long itHandle)
    {
        stub.IteratorClose0(itHandle);
    }

    // ---------------- WriteBatch wrapper methods ----------------
    protected long WriteBatchCreate()
    {
        return stub.WriteBatchCreate0();
    }

    protected void WriteBatchPut(long wbHandle, byte[] key, byte[] value)
    {
        stub.WriteBatchPut0(wbHandle, key, value);
    }

    protected void WriteBatchDelete(long wbHandle, byte[] key)
    {
        stub.WriteBatchDelete0(wbHandle, key);
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

    private long dbHandle;
    private final long tableId;
    private final long indexId;
    private final boolean unique;
    private volatile boolean closed = false;
    private volatile boolean removed = false;

    public RocksetIndex(long tableId, long indexId, CloudDBOptions options, boolean unique)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.unique = unique;
        this.dbHandle = CreateDBCloud(options);
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
    public long getUniqueRowIdInternal(IndexProto.IndexKey key)
    {
        byte[] prefix = toByteArray(key); // indexId + key (NO timestamp)
        long ts = key.getTimestamp();
        byte[] upperKey = concat(prefix, writeLongBE(ts + 1));
        long it = 0;
        try
        {
            it = DBNewIterator(this.dbHandle);
            IteratorSeekForPrev(it, upperKey);
            if (!IteratorIsValid(it))
                return -1L;
            byte[] k = IteratorKey(it);
            if (!startsWith(k, prefix))
                return -1L;
            byte[] v = IteratorValue(it);
            if (v == null || v.length < Long.BYTES) return -1L;
            return ByteBuffer.wrap(v).getLong();
        }
        catch (Throwable t)
        {
            LOGGER.error("getUniqueRowId failed", t);
            return -1L;
        }
        finally
        {
            if (it != 0)
                IteratorClose(it);
        }
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key)
    {
        ImmutableList.Builder<Long> out = ImmutableList.builder();
        byte[] prefix = toByteArray(key);
        long ts = key.getTimestamp();
        byte[] upperKey = concat(prefix, writeLongBE(ts + 1));
        long it = 0;
        try
        {
            it = DBNewIterator(this.dbHandle);
            IteratorSeekForPrev(it, upperKey);
            while (IteratorIsValid(it))
            {
                byte[] k = IteratorKey(it);
                if (!startsWith(k, prefix))
                    break;
                long rowId = extractRowIdFromKey(k);
                if (rowId < 0) break;
                out.add(rowId);
                IteratorPrev(it);
            }
        }
        catch (Throwable t)
        {
            LOGGER.error("getRowIds failed", t);
            return ImmutableList.of();
        }
        finally
        {
            if (it != 0)
                IteratorClose(it);
        }
        return out.build();
    }

    @Override
    public boolean putEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        try
        {
            if (unique)
            {
                byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                byte[] val = writeLongBE(rowId);
                DBput(this.dbHandle, fullKey, val);
            }
            else
            {
                byte[] nonUniqueKey = toNonUniqueKey(key, rowId);
                DBput(this.dbHandle, nonUniqueKey, new byte[0]);
            }
            return true;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("failed to put rockset index entry", e);
            throw new SinglePointIndexException("failed to put rockset index entry", e);
        }
    }

    @Override
    public boolean putPrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException
    {
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long rowId = entry.getRowId();
                byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                byte[] val = writeLongBE(rowId);
                WriteBatchPut(wb, fullKey, val);
            }
            DBWrite(this.dbHandle, wb);
            return true;
        }
        catch (Exception e)
        {
            LOGGER.error("failed to put rockset primary index entries", e);
            throw new SinglePointIndexException("failed to put rockset primary index entries", e);
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
                    byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                    byte[] val = writeLongBE(rowId);
                    WriteBatchPut(wb, fullKey, val);
                }
                else
                {
                    byte[] nonUniqueKey = toNonUniqueKey(key, rowId);
                    WriteBatchPut(wb, nonUniqueKey, new byte[0]);
                }
            }
            DBWrite(this.dbHandle, wb);
            return true;
        }
        catch (Exception e)
        {
            LOGGER.error("failed to put rockset secondary index entries", e);
            throw new SinglePointIndexException("failed to put rockset secondary index entries", e);
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
        try
        {
            long prev = getUniqueRowId(key);
            byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
            byte[] val = writeLongBE(rowId);
            DBput(this.dbHandle, fullKey, val);
            return prev;
        }
        catch (Exception e)
        {
            LOGGER.error("failed to update primary entry", e);
            throw new SinglePointIndexException("failed to update primary entry", e);
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
                prev.add(getUniqueRowId(key));
                byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                DBput(this.dbHandle, fullKey, writeLongBE(rowId));
            }
            else
            {
                List<Long> rowIds = getRowIds(key);
                prev.addAll(rowIds);
                byte[] nonUniqueKey = toNonUniqueKey(key, rowId);
                DBput(this.dbHandle, nonUniqueKey, new byte[0]);
            }
            return prev.build();
        }
        catch (Exception e)
        {
            LOGGER.error("failed to update secondary entry", e);
            throw new SinglePointIndexException("failed to update secondary entry", e);
        }
    }

    @Override
    public List<Long> updatePrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        long wb = 0;
        try
        {
            wb = WriteBatchCreate();
            ImmutableList.Builder<Long> prevRowIds = ImmutableList.builder();
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                IndexProto.IndexKey key = entry.getIndexKey();
                long prevRowId = getUniqueRowId(key);
                if (prevRowId < 0)
                {
                    return ImmutableList.of();
                }
                prevRowIds.add(prevRowId);
                byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                WriteBatchPut(wb, fullKey, writeLongBE(entry.getRowId()));
            }
            DBWrite(this.dbHandle, wb);
            return prevRowIds.build();
        }
        catch (Exception e)
        {
            LOGGER.error("failed to update primary index entries", e);
            throw new SinglePointIndexException("failed to update primary index entries", e);
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
                    if (prevRowId < 0)
                    {
                        return ImmutableList.of();
                    }
                    prevRowIds.add(prevRowId);
                    byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                    WriteBatchPut(wb, fullKey, writeLongBE(rowId));
                }
                else
                {
                    List<Long> rowIds = getRowIds(key);
                    if (rowIds.isEmpty())
                    {
                        return ImmutableList.of();
                    }
                    prevRowIds.addAll(rowIds);
                    byte[] nonUniqueKey = toNonUniqueKey(key, rowId);
                    WriteBatchPut(wb, nonUniqueKey, new byte[0]);
                }
            }
            DBWrite(this.dbHandle, wb);
            return prevRowIds.build();
        }
        catch (Exception e)
        {
            LOGGER.error("failed to update secondary index entries", e);
            throw new SinglePointIndexException("failed to update secondary index entries", e);
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
        try
        {
            long prev = getUniqueRowId(key);
            byte[] tomb = writeLongBE(-1L);  // tombstone marker
            byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
            DBput(this.dbHandle, fullKey, tomb);

            return prev;
        }
        catch (Exception e)
        {
            LOGGER.error("failed to delete unique entry", e);
            throw new SinglePointIndexException("failed to delete unique entry", e);
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
                byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                WriteBatchPut(wb, fullKey, writeLongBE(-1L));
            }
            else
            {
                List<Long> rowIds = getRowIds(key);
                if (rowIds.isEmpty())
                {
                    return ImmutableList.of();
                }
                prev.addAll(rowIds);
                // mark tombstone entry for this (key, -1L)
                byte[] nonUniqueKeyTomb = toNonUniqueKey(key, -1L);
                WriteBatchPut(wb, nonUniqueKeyTomb, new byte[0]);
            }
            DBWrite(this.dbHandle, wb);
            return prev.build();
        }
        catch (Exception e)
        {
            LOGGER.error("failed to delete entry", e);
            throw new SinglePointIndexException("failed to delete entry", e);
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
                    if (rowId < 0)
                    {
                        return ImmutableList.of();
                    }
                    prev.add(rowId);
                    byte[] fullKey = concat(toByteArray(key), writeLongBE(key.getTimestamp()));
                    WriteBatchPut(wb, fullKey, writeLongBE(-1L));
                }
                else
                {
                    List<Long> rowIds = getRowIds(key);
                    if (rowIds.isEmpty())
                    {
                        return ImmutableList.of();
                    }
                    prev.addAll(rowIds);
                    byte[] nonUniqueKeyTomb = toNonUniqueKey(key, -1L);
                    WriteBatchPut(wb, nonUniqueKeyTomb, new byte[0]);
                }
            }
            DBWrite(this.dbHandle, wb);
            return prev.build();
        }
        catch (Exception e)
        {
            LOGGER.error("failed to delete entries", e);
            throw new SinglePointIndexException("failed to delete entries", e);
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
                byte[] prefix = toByteArray(key); // indexId + key (NO timestamp)
                long ts = key.getTimestamp();
                byte[] upperKey = concat(prefix, writeLongBE(ts + 1));
                long it = 0;
                try
                {
                    it = DBNewIterator(this.dbHandle);
                    IteratorSeekForPrev(it, upperKey);
                    while (IteratorIsValid(it))
                    {
                        byte[] k = IteratorKey(it);
                        if (startsWith(k, prefix))
                        {
                            if(unique)
                            {
                                long rowId = ByteBuffer.wrap(IteratorValue(it)).getLong();
                                if(rowId > 0)
                                    builder.add(rowId);
                            }
                            WriteBatchDelete(wb, k);
                            IteratorPrev(it);
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
            LOGGER.error("failed to purge entries by prefix", e);
            throw new SinglePointIndexException("failed to purge entries by prefix", e);
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
        if (dbHandle != 0)
        {
            closed = true;
            this.dbHandle = 0;
        }
    }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException
    {
        try
        {
            close();
            removed = true; // no local RocksDB folder to delete for cloud; mark removed
            return true;
        }
        catch (IOException e)
        {
            throw new SinglePointIndexException("failed to close rockset index", e);
        }
    }

    // ----------------- Encoding helpers -----------------
    private byte[] toByteArray(IndexProto.IndexKey key)
    {
        // prefix = indexId(8 bytes, BE) + raw key bytes
        byte[] indexIdBytes = writeLongBE(this.indexId);
        byte[] rawKey = key.getKey().toByteArray();
        return concat(indexIdBytes, rawKey);
    }

    private byte[] toNonUniqueKey(IndexProto.IndexKey key, long rowId)
    {
        // prefix + timestamp + rowId
        return concat(concat(toByteArray(key), writeLongBE(key.getTimestamp())), writeLongBE(rowId));
    }

    private static byte[] concat(byte[] a, byte[] b)
    {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static boolean startsWith(byte[] key, byte[] prefix)
    {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++)
        {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    private static long extractRowIdFromKey(byte[] fullKey)
    {
        if (fullKey.length < Long.BYTES) return -1L;
        int off = fullKey.length - Long.BYTES;
        return ByteBuffer.wrap(fullKey, off, Long.BYTES).getLong();
    }

    private static byte[] writeLongBE(long v)
    {
        return ByteBuffer.allocate(Long.BYTES).putLong(v).array();
    }
}
