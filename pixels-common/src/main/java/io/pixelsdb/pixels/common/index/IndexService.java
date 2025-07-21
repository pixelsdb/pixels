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
public class IndexService
{
    private static final Logger logger = LogManager.getLogger(IndexService.class);
    private static final IndexService defaultInstance;
    private static final Map<HostAddress, IndexService> otherInstances = new HashMap<>();

    static
    {
        String indexHost = ConfigFactory.Instance().getProperty("index.server.host");
        int indexPort = Integer.parseInt(ConfigFactory.Instance().getProperty("index.server.port"));
        defaultInstance = new IndexService(indexHost, indexPort);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run() {
                try
                {
                    defaultInstance.shutdown();
                    for (IndexService otherTransService : otherInstances.values())
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
    public static IndexService Instance()
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
    public static IndexService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        IndexService indexService = otherInstances.get(address);
        if (indexService != null)
        {
            return indexService;
        }
        indexService = new IndexService(host, port);
        otherInstances.put(address, indexService);
        return indexService;
    }

    private final ManagedChannel channel;
    private final IndexServiceGrpc.IndexServiceBlockingStub stub;
    private boolean isShutDown;

    private IndexService(String host, int port)
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

    public IndexProto.RowIdBatch allocateRowIdBatch (long tableId, int numRowIds)
    {
        IndexProto.AllocateRowIdBatchRequest request = IndexProto.AllocateRowIdBatchRequest.newBuilder()
                .setTableId(tableId).setNumRowIds(numRowIds).build();

        IndexProto.AllocateRowIdBatchResponse response = stub.allocateRowIdBatch(request);
        return response.getRowIdBatch();
    }

    public IndexProto.RowLocation lookupUniqueIndex (IndexProto.IndexKey key)
    {
        IndexProto.LookupUniqueIndexRequest request = IndexProto.LookupUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.LookupUniqueIndexResponse response = stub.lookupUniqueIndex(request);
        return response.getRowLocation();
    }

    public List<IndexProto.RowLocation> lookupNonUniqueIndex (IndexProto.IndexKey key)
    {
        IndexProto.LookupNonUniqueIndexRequest request = IndexProto.LookupNonUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.LookupNonUniqueIndexResponse response = stub.lookupNonUniqueIndex(request);
        return response.getRowLocationsList();
    }

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

    public boolean putPrimaryIndexEntries (List<IndexProto.PrimaryIndexEntry> entries) throws IndexException
    {
        IndexProto.PutPrimaryIndexEntriesRequest request = IndexProto.PutPrimaryIndexEntriesRequest.newBuilder()
                .addAllIndexEntries(entries).build();

        IndexProto.PutPrimaryIndexEntriesResponse response = stub.putPrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put primary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean putSecondaryIndexEntries (List<IndexProto.SecondaryIndexEntry> entries) throws IndexException
    {
        IndexProto.PutSecondaryIndexEntriesRequest request = IndexProto.PutSecondaryIndexEntriesRequest.newBuilder()
                .addAllIndexEntries(entries).build();

        IndexProto.PutSecondaryIndexEntriesResponse response = stub.putSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put secondary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deletePrimaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.DeletePrimaryIndexEntryRequest request = IndexProto.DeletePrimaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.DeletePrimaryIndexEntryResponse response = stub.deletePrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete primary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deleteSecondaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.DeleteSecondaryIndexEntryRequest request = IndexProto.DeleteSecondaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.DeleteSecondaryIndexEntryResponse response = stub.deleteSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete secondary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deletePrimaryIndexEntries (List<IndexProto.IndexKey> keys) throws IndexException
    {
        IndexProto.DeletePrimaryIndexEntriesRequest request = IndexProto.DeletePrimaryIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();

        IndexProto.DeletePrimaryIndexEntriesResponse response = stub.deletePrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete primary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deleteSecondaryIndexEntries (List<IndexProto.IndexKey> keys) throws IndexException
    {
        IndexProto.DeleteSecondaryIndexEntriesRequest request = IndexProto.DeleteSecondaryIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();

        IndexProto.DeleteSecondaryIndexEntriesResponse response = stub.deleteSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete secondary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean updatePrimaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.UpdatePrimaryIndexEntryRequest request = IndexProto.UpdatePrimaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.UpdatePrimaryIndexEntryResponse response = stub.updatePrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update primary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean updateSecondaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        IndexProto.UpdateSecondaryIndexEntryRequest request = IndexProto.UpdateSecondaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();

        IndexProto.UpdateSecondaryIndexEntryResponse response = stub.updateSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update secondary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean updatePrimaryIndexEntries (List<IndexProto.IndexKey> keys) throws IndexException
    {
        IndexProto.UpdatePrimaryIndexEntriesRequest request = IndexProto.UpdatePrimaryIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();

        IndexProto.UpdatePrimaryIndexEntriesResponse response = stub.updatePrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update primary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean updateSecondaryIndexEntries (List<IndexProto.IndexKey> keys) throws IndexException
    {
        IndexProto.UpdateSecondaryIndexEntriesRequest request = IndexProto.UpdateSecondaryIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();

        IndexProto.UpdateSecondaryIndexEntriesResponse response = stub.updateSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to update secondary index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean flushIndexEntriesOfFile (long fileId) throws IndexException
    {
        IndexProto.FlushIndexEntriesOfFileRequest request = IndexProto.FlushIndexEntriesOfFileRequest.newBuilder()
                .setFileId(fileId).build();

        IndexProto.FlushIndexEntriesOfFileResponse response = stub.flushIndexEntriesOfFile(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to flush index entries of file, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean openIndex (long tableId, long indexId) throws IndexException
    {
        IndexProto.OpenIndexRequest request = IndexProto.OpenIndexRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).build();

        IndexProto.OpenIndexResponse response = stub.openIndex(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to open index, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean closeIndex (long tableId, long indexId) throws IndexException
    {
        IndexProto.CloseIndexRequest request = IndexProto.CloseIndexRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).build();

        IndexProto.CloseIndexResponse response = stub.closeIndex(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to close index, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean removeIndex (long tableId, long indexId) throws IndexException
    {
        IndexProto.RemoveIndexRequest request = IndexProto.RemoveIndexRequest.newBuilder()
                .setTableId(tableId).setIndexId(indexId).build();

        IndexProto.RemoveIndexResponse response = stub.removeIndex(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to remove index, error code=" + response.getErrorCode());
        }
        return true;
    }
}
