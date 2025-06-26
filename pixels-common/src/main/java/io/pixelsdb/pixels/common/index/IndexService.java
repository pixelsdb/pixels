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
        // Create gRPC channel
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        // Create gRPC client stub
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
        // Create gRPC request
        IndexProto.AllocateRowIdBatchRequest request = IndexProto.AllocateRowIdBatchRequest.newBuilder()
                .setTableId(tableId).setNumRowIds(numRowIds).build();
        // Send request and get response
        IndexProto.AllocateRowIdBatchResponse response = stub.allocateRowIdBatch(request);
        // Return RowLocation
        return response.getRowIdBatch();
    }

    public IndexProto.RowLocation lookupUniqueIndex (IndexProto.IndexKey key)
    {
        // Create gRPC request
        IndexProto.LookupUniqueIndexRequest request = IndexProto.LookupUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();
        // Send request and get response
        IndexProto.LookupUniqueIndexResponse response = stub.lookupUniqueIndex(request);
        // Return RowLocation
        return response.getRowLocation();
    }

    public List<IndexProto.RowLocation> lookupNonUniqueIndex (IndexProto.IndexKey key)
    {
        // Create gRPC request
        IndexProto.LookupNonUniqueIndexRequest request = IndexProto.LookupNonUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();
        // Send request and get response
        IndexProto.LookupNonUniqueIndexResponse response = stub.lookupNonUniqueIndex(request);
        // Return RowLocation list
        return response.getRowLocationList();
    }

    public boolean putPrimaryIndexEntry (IndexProto.PrimaryIndexEntry entry) throws IndexException
    {
        // Create gRPC request
        IndexProto.PutPrimaryIndexEntryRequest request = IndexProto.PutPrimaryIndexEntryRequest.newBuilder()
                .setIndexEntry(entry).build();
        // Send request and get response
        IndexProto.PutPrimaryIndexEntryResponse response = stub.putPrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put primary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean putSecondaryIndexEntry (IndexProto.SecondaryIndexEntry entry) throws IndexException
    {
        // Create gRPC request
        IndexProto.PutSecondaryIndexEntryRequest request = IndexProto.PutSecondaryIndexEntryRequest.newBuilder()
                .setIndexEntry(entry).build();
        // Send request and get response
        IndexProto.PutSecondaryIndexEntryResponse response = stub.putSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put secondary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deletePrimaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        // Create gRPC request
        IndexProto.DeletePrimaryIndexEntryRequest request = IndexProto.DeletePrimaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();
        // Send request and get response
        IndexProto.DeletePrimaryIndexEntryResponse response = stub.deletePrimaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete primary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deleteSecondaryIndexEntry (IndexProto.IndexKey key) throws IndexException
    {
        // Create gRPC request
        IndexProto.DeleteSecondaryIndexEntryRequest request = IndexProto.DeleteSecondaryIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();
        // Send request and get response
        IndexProto.DeleteSecondaryIndexEntryResponse response = stub.deleteSecondaryIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete secondary index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean putPrimaryIndexEntries (List<IndexProto.PrimaryIndexEntry> entries) throws IndexException
    {
        // Create gRPC request
        IndexProto.PutPrimaryIndexEntriesRequest request = IndexProto.PutPrimaryIndexEntriesRequest.newBuilder()
                .addAllIndexEntries(entries).build();
        // Send request and get response
        IndexProto.PutPrimaryIndexEntriesResponse response = stub.putPrimaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put primary index entries, error code=" + response.getErrorCode());
        }
        // Return operation success status
        return true;
    }

    public boolean putSecondaryIndexEntries (List<IndexProto.SecondaryIndexEntry> entries) throws IndexException
    {
        // Create gRPC request
        IndexProto.PutSecondaryIndexEntriesRequest request = IndexProto.PutSecondaryIndexEntriesRequest.newBuilder()
                .addAllIndexEntries(entries).build();
        // Send request and get response
        IndexProto.PutSecondaryIndexEntriesResponse response = stub.putSecondaryIndexEntries(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put secondary index entries, error code=" + response.getErrorCode());
        }
        // Return operation success status
        return true;
    }

    public boolean deletePrimaryIndexEntries (List<IndexProto.IndexKey> keys) throws IndexException
    {
        // Create gRPC request
        IndexProto.DeletePrimaryIndexEntriesRequest request = IndexProto.DeletePrimaryIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();
        // Send request and get response
        IndexProto.DeletePrimaryIndexEntriesResponse response = stub.deletePrimaryIndexEntries(request);
        // Return operation success status
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete index primary entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deleteSecondaryIndexEntries (List<IndexProto.IndexKey> keys) throws IndexException
    {
        // Create gRPC request
        IndexProto.DeleteSecondaryIndexEntriesRequest request = IndexProto.DeleteSecondaryIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();
        // Send request and get response
        IndexProto.DeleteSecondaryIndexEntriesResponse response = stub.deleteSecondaryIndexEntries(request);
        // Return operation success status
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete index secondary entries, error code=" + response.getErrorCode());
        }
        return true;
    }
}
