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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.IndexServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author hank, Rolland1944
 * @create 2025-02-16
 */
public class RPCIndexService implements IndexService
{
    private static final Logger logger = LogManager.getLogger(RPCIndexService.class);
    private static final RPCIndexService defaultInstance;
    private static final Map<HostAddress, RPCIndexService> otherInstances = new HashMap<>();

    static
    {
        String indexHost = ConfigFactory.Instance().getProperty("index.server.host");
        int indexPort = Integer.parseInt(ConfigFactory.Instance().getProperty("index.server.port"));
        defaultInstance = new RPCIndexService(indexHost, indexPort);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run() {
                try
                {
                    defaultInstance.shutdown();
                    for (RPCIndexService otherTransService : otherInstances.values())
                    {
                        otherTransService.shutdown();
                    }
                    otherInstances.clear();
                } catch (InterruptedException e)
                {
                    logger.error("failed to shut down index service", e);
                }
            }
        }));
    }

    /**
     * Get the default index service instance connecting to the index host:port configured in
     * PIXELS_HOME/etc/pixels.properties. This default instance will be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     * @return
     */
    public static RPCIndexService Instance()
    {
        return defaultInstance;
    }

    /**
     * This method should only be used to connect to a index server that is not configured through
     * PIXELS_HOME/etc/pixels.properties. <b>No need</b> to manually shut down the returned index service.
     * @param host the host name of the index server
     * @param port the port of the index server
     * @return the created index service instance
     */
    public static RPCIndexService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        RPCIndexService RPCIndexService = otherInstances.get(address);
        if (RPCIndexService != null)
        {
            return RPCIndexService;
        }
        RPCIndexService = new RPCIndexService(host, port);
        otherInstances.put(address, RPCIndexService);
        return RPCIndexService;
    }

    private final ManagedChannel channel;
    private final IndexServiceGrpc.IndexServiceBlockingStub stub;
    private boolean isShutDown;

    private RPCIndexService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = IndexServiceGrpc.newBlockingStub(channel);
        this.isShutDown = false;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutDown)
        {
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutDown = true;
        }
    }

    /**
     * Allocate a batch of continuous row ids for the primary index on a table.
     * These row ids are to be put into the primary index by the client (e.g., retina or sink)
     * @param tableId the table id of the table
     * @param numRowIds the number of row ids to allocate
     * @return the allocated row ids
     */
    public IndexProto.RowIdBatch allocateRowIdBatch (long tableId, int numRowIds)
    {
        IndexProto.AllocateRowIdBatchRequest request = IndexProto.AllocateRowIdBatchRequest.newBuilder()
                .setTableId(tableId).setNumRowIds(numRowIds).build();

        IndexProto.AllocateRowIdBatchResponse response = stub.allocateRowIdBatch(request);
        return response.getRowIdBatch();
    }

    /**
     * Lookup a unique index.
     * @param key the index key
     * @return the row location or null if the index entry is not found.
     */
    public IndexProto.RowLocation lookupUniqueIndex (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.LookupUniqueIndexRequest request = IndexProto.LookupUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.LookupUniqueIndexResponse response = stub.lookupUniqueIndex(request);
        if (response.getErrorCode() == ErrorCode.SUCCESS)
        {
            return response.getRowLocation();
        }
        else if (response.getErrorCode() == ErrorCode.INDEX_ENTRY_NOT_FOUND)
        {
            return null;
        }
        else
        {
            throw new IndexException("failed to lookup unique index, error code=" + response.getErrorCode());
        }
    }

    /**
     * Lookup a non-unique index.
     * @param key the index key
     * @return the row locations or null if the index entry is not found.
     */
    public List<IndexProto.RowLocation> lookupNonUniqueIndex (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.LookupNonUniqueIndexRequest request = IndexProto.LookupNonUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.LookupNonUniqueIndexResponse response = stub.lookupNonUniqueIndex(request);
        if (response.getErrorCode() == ErrorCode.INDEX_ENTRY_NOT_FOUND)
        {
            return null;
        }
        else if (response.getErrorCode() == ErrorCode.SUCCESS)
        {
            return response.getRowLocationsList();
        }
        else
        {
            throw new IndexException("failed to lookup non-unique index, error code=" + response.getErrorCode());
        }
    }

    /**
     * Put an index entry into the primary index.
     * @param entry the index entry
     * @return true on success
     */
    public boolean putPrimaryIndexEntry (IndexProto.PrimaryIndexEntry entry) throws IndexException
    {
        IndexProto.PutPrimaryIndexEntryRequest request = IndexProto.PutPrimaryIndexEntryRequest.newBuilder()
                .setIndexEntry(entry).build();

        IndexProto.PutPrimaryIndexEntryResponse response = stub.putPrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put primary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Put an index entry into the secondary index.
     * @param entry the index entry
     * @return true on success
     */
    public boolean putSecondaryIndexEntry (IndexProto.SecondaryIndexEntry entry) throws IndexException
    {
        IndexProto.PutSecondaryIndexEntryRequest request = IndexProto.PutSecondaryIndexEntryRequest.newBuilder()
                .setIndexEntry(entry).build();

        IndexProto.PutSecondaryIndexEntryResponse response = stub.putSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put secondary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Put a batch of index entries into the primary index.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param entries the index entries
     * @return true on success
     */
    public boolean putPrimaryIndexEntries (long tableId, long indexId, List<IndexProto.PrimaryIndexEntry> entries)
            throws IndexException
    {
        IndexProto.PutPrimaryIndexEntriesRequest request = IndexProto.PutPrimaryIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexEntries(entries).build();

        IndexProto.PutPrimaryIndexEntriesResponse response = stub.putPrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put primary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Put a batch of index entries into the secondary index.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param entries the index entries
     * @return true on success
     */
    public boolean putSecondaryIndexEntries (long tableId, long indexId, List<IndexProto.SecondaryIndexEntry> entries)
            throws IndexException
    {
        IndexProto.PutSecondaryIndexEntriesRequest request = IndexProto.PutSecondaryIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexEntries(entries).build();

        IndexProto.PutSecondaryIndexEntriesResponse response = stub.putSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put secondary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Delete an entry from the primary index. The deleted index entry is marked as deleted using a tombstone.
     * @param key the index key
     * @return the row location of the deleted index entry
     */
    public IndexProto.RowLocation deletePrimaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.DeletePrimaryIndexEntryRequest request = IndexProto.DeletePrimaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.DeletePrimaryIndexEntryResponse response = stub.deletePrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete primary index entry, error code=" + response.getErrorCode());
        }
        return response.getRowLocation();
    }

    /**
     * Delete entry(ies) from the secondary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param key the index key
     * @return the row id(s) of the deleted index entry(ies)
     */
    public List<Long> deleteSecondaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.DeleteSecondaryIndexEntryRequest request = IndexProto.DeleteSecondaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.DeleteSecondaryIndexEntryResponse response = stub.deleteSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete secondary index entry, error code=" + response.getErrorCode());
        }
        return response.getRowIdsList();
    }

    /**
     * Delete entries from the primary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param keys the keys of the entries to delete
     * @return the row locations of the deleted index entries
     */
    public List<IndexProto.RowLocation> deletePrimaryIndexEntries (long tableId, long indexId, List<IndexProto.IndexKey> keys)
            throws IndexException
    {
        IndexProto.DeletePrimaryIndexEntriesRequest request = IndexProto.DeletePrimaryIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexKeys(keys).build();

        IndexProto.DeletePrimaryIndexEntriesResponse response = stub.deletePrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete primary index entries, error code=" + response.getErrorCode());
        }
        return response.getRowLocationsList();
    }

    /**
     * Delete entries from the secondary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param keys the keys of the entries to delete
     * @return the row ids of the deleted index entries
     */
    public List<Long> deleteSecondaryIndexEntries (long tableId, long indexId, List<IndexProto.IndexKey> keys)
            throws IndexException
    {
        IndexProto.DeleteSecondaryIndexEntriesRequest request = IndexProto.DeleteSecondaryIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexKeys(keys).build();

        IndexProto.DeleteSecondaryIndexEntriesResponse response = stub.deleteSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete secondary index entries, error code=" + response.getErrorCode());
        }
        return response.getRowIdsList();
    }

    /**
     * Update the entry of a primary index.
     * @param indexEntry the index entry to update
     * @return the previous row location of the index entry
     */
    public IndexProto.RowLocation updatePrimaryIndexEntry (IndexProto.PrimaryIndexEntry indexEntry) throws IndexException
    {
        IndexProto.UpdatePrimaryIndexEntryRequest request = IndexProto.UpdatePrimaryIndexEntryRequest.newBuilder()
                .setIndexEntry(indexEntry).build();

        IndexProto.UpdatePrimaryIndexEntryResponse response = stub.updatePrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update primary index entry, error code=" + response.getErrorCode());
        }
        return response.getPrevRowLocation();
    }

    /**
     * Update the entry of a secondary index.
     * @param indexEntry the index entry to update
     * @return the previous row id(s) of the index entry
     */
    public List<Long> updateSecondaryIndexEntry (IndexProto.SecondaryIndexEntry indexEntry) throws IndexException
    {
        IndexProto.UpdateSecondaryIndexEntryRequest request = IndexProto.UpdateSecondaryIndexEntryRequest.newBuilder()
                .setIndexEntry(indexEntry).build();

        IndexProto.UpdateSecondaryIndexEntryResponse response = stub.updateSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update secondary index entry, error code=" + response.getErrorCode());
        }
        return response.getPrevRowIdsList();
    }

    /**
     * Update the entries of a primary index.
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param indexEntries the index entries to update
     * @return the previous row locations of the index entries
     */
    public List<IndexProto.RowLocation> updatePrimaryIndexEntries (long tableId, long indexId, List<IndexProto.PrimaryIndexEntry> indexEntries)
            throws IndexException
    {
        IndexProto.UpdatePrimaryIndexEntriesRequest request = IndexProto.UpdatePrimaryIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexEntries(indexEntries).build();

        IndexProto.UpdatePrimaryIndexEntriesResponse response = stub.updatePrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update primary index entries, error code=" + response.getErrorCode());
        }
        return response.getPrevRowLocationsList();
    }

    /**
     * Update the entries of a secondary index.
     * @param tableId the table id of the secondary index
     * @param indexId the index id of the secondary index
     * @param indexEntries the index entries to update
     * @return the previous row ids of the index entries
     */
    public List<Long> updateSecondaryIndexEntries (long tableId, long indexId, List<IndexProto.SecondaryIndexEntry> indexEntries)
            throws IndexException
    {
        IndexProto.UpdateSecondaryIndexEntriesRequest request = IndexProto.UpdateSecondaryIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexEntries(indexEntries).build();

        IndexProto.UpdateSecondaryIndexEntriesResponse response = stub.updateSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update secondary index entries, error code=" + response.getErrorCode());
        }
        return response.getPrevRowIdsList();
    }

    /**
     * Purge (remove) the index entries of an index permanently. This should only be done asynchronously by the garbage
     * collection process.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param indexKeys the index keys of the index entries
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    public boolean purgeIndexEntries (long tableId, long indexId, List<IndexProto.IndexKey> indexKeys, boolean isPrimary)
            throws IndexException
    {
        IndexProto.PurgeIndexEntriesRequest request = IndexProto.PurgeIndexEntriesRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).addAllIndexKeys(indexKeys).setIsPrimary(isPrimary).build();

        IndexProto.PurgeIndexEntriesResponse response = stub.purgeIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to purge index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Flush the index entries of an index corresponding to a buffered Pixels data file.
     * In Pixels, the index entries corresponding to a write-buffered data file (usually stored in the write buffer)
     * may be buffered in memory by the index server. This method tells to index server to flush such buffered index
     * entries for a write-buffered data file. This methods should be called by retina when flushing a write buffer.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param fileId the file id of the write buffer
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    public boolean flushIndexEntriesOfFile (long tableId, long indexId, long fileId, boolean isPrimary) throws IndexException
    {
        IndexProto.FlushIndexEntriesOfFileRequest request = IndexProto.FlushIndexEntriesOfFileRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).setFileId(fileId).setIsPrimary(isPrimary).build();

        IndexProto.FlushIndexEntriesOfFileResponse response = stub.flushIndexEntriesOfFile(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to flush index entries of file, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Open an index in the index server. This method is optional and is used to pre-warm an index instance in
     * the index server. Even without calling this method, an index will be opened on its first access.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    public boolean openIndex (long tableId, long indexId, boolean isPrimary) throws IndexException
    {
        IndexProto.OpenIndexRequest request = IndexProto.OpenIndexRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).setIsPrimary(isPrimary).build();

        IndexProto.OpenIndexResponse response = stub.openIndex(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to open index, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Close an index in the index server.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    public boolean closeIndex (long tableId, long indexId, boolean isPrimary) throws IndexException
    {
        IndexProto.CloseIndexRequest request = IndexProto.CloseIndexRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).setIsPrimary(isPrimary).build();

        IndexProto.CloseIndexResponse response = stub.closeIndex(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to close index, error code=" + response.getErrorCode());
        }
        return true;
    }

    /**
     * Close an index and remove its persistent storage content in the index server.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    public boolean removeIndex (long tableId, long indexId, boolean isPrimary) throws IndexException
    {
        IndexProto.RemoveIndexRequest request = IndexProto.RemoveIndexRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).setIsPrimary(isPrimary).build();

        IndexProto.RemoveIndexResponse response = stub.removeIndex(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to remove index, error code=" + response.getErrorCode());
        }
        return true;
    }
}
