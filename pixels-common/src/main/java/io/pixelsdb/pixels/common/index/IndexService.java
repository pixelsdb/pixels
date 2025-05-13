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

import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * @author hank
 * @create 2025-02-16
 */
public class IndexService
{
    // TODO: implement
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
        // 创建 gRPC 通道
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        // 创建 gRPC 客户端存根
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

    public IndexProto.RowLocation lookupUniqueIndex(IndexProto.IndexKey key)
    {
        // 创建 gRPC 请求
        IndexProto.LookupUniqueIndexRequest request = IndexProto.LookupUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();
        // 发送请求并获取响应
        IndexProto.LookupUniqueIndexResponse response = stub.lookupUniqueIndex(request);
        // 返回 RowLocation
        return response.getRowLocation();
    }

    public List<IndexProto.RowLocation> lookupNonUniqueIndex(IndexProto.IndexKey key)
    {
        // 创建 gRPC 请求
        IndexProto.LookupNonUniqueIndexRequest request = IndexProto.LookupNonUniqueIndexRequest.newBuilder()
                .setIndexKey(key).build();
        // 发送请求并获取响应
        IndexProto.LookupNonUniqueIndexResponse response = stub.lookupNonUniqueIndex(request);
        // 返回 RowLocation
        return response.getRowLocationList();
    }

    public boolean putIndexEntry(IndexProto.IndexEntry entry) throws IndexException {
        // 创建 gRPC 请求
        IndexProto.PutIndexEntryRequest request = IndexProto.PutIndexEntryRequest.newBuilder()
                .setIndexEntry(entry).build();
        // 发送请求并获取响应
        IndexProto.PutIndexEntryResponse response = stub.putIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deleteIndexEntry(IndexProto.IndexKey key) throws IndexException {
        // 创建 gRPC 请求
        IndexProto.DeleteIndexEntryRequest request = IndexProto.DeleteIndexEntryRequest.newBuilder()
                .setIndexKey(key).build();
        // 发送请求并获取响应
        IndexProto.DeleteIndexEntryResponse response = stub.deleteIndexEntry(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete index entry, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean putIndexEntries(List<IndexProto.IndexEntry> entries) throws IndexException {
        // 创建 gRPC 请求
        IndexProto.PutIndexEntriesRequest request = IndexProto.PutIndexEntriesRequest.newBuilder()
                .addAllIndexEntries(entries).build();
        // 发送请求并获取响应
        IndexProto.PutIndexEntriesResponse response = stub.putIndexEntries(request);
        // 返回操作是否成功
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to put index entries, error code=" + response.getErrorCode());
        }
        return true;
    }

    public boolean deleteIndexEntries(List<IndexProto.IndexKey> keys) throws IndexException {
        // 创建 gRPC 请求
        IndexProto.DeleteIndexEntriesRequest request = IndexProto.DeleteIndexEntriesRequest.newBuilder()
                .addAllIndexKeys(keys).build();
        // 发送请求并获取响应
        IndexProto.DeleteIndexEntriesResponse response = stub.deleteIndexEntries(request);
        // 返回操作是否成功
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new IndexException("failed to delete index entries, error code=" + response.getErrorCode());
        }
        return true;
    }
}
