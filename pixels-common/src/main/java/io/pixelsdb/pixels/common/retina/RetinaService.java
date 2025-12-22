/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.common.retina;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RetinaService
{
    private static final Logger logger = LogManager.getLogger(RetinaService.class);
    private static final RetinaService defaultInstance;
    private static final Map<HostAddress, RetinaService> otherInstances = new ConcurrentHashMap<>();

    static
    {
        ConfigFactory config = ConfigFactory.Instance();
        String retinaHost = config.getProperty("retina.server.host");
        int retinaPort = Integer.parseInt(config.getProperty("retina.server.port"));
        boolean enabled = Boolean.parseBoolean(config.getProperty("retina.enable"));
        defaultInstance = new RetinaService(retinaHost, retinaPort, enabled);
        ShutdownHookManager.Instance().registerShutdownHook(RetinaService.class, false, () -> {
            try
            {
                defaultInstance.shutdown();
                for (RetinaService otherRetinaService : otherInstances.values())
                {
                    otherRetinaService.shutdown();
                }
                otherInstances.clear();
            } catch (InterruptedException e)
            {
                logger.error("failed to shut down retina service", e);
            }
        });
    }

    /**
     * Get the default retina service instance connecting to the retina host:port configured in
     * PIXELS_HOME/pixels.properties. This default instance will be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     *
     * @return
     */
    public static RetinaService Instance()
    {
        return defaultInstance;
    }

    /**
     * This method should only be used to connect to a retina server that is not configured through
     * PIXELS_HOME/pixels.properties. <b>No need</b> to manually shut down the returned retina service.
     *
     * @param host the host name of the retina server
     * @param port the port of the retina server
     * @return the created retina service instance
     */
    public static synchronized RetinaService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        // For other instances, we also follow the global configuration.
        String retinaEnable = ConfigFactory.Instance().getProperty("retina.enable");
        boolean enabled = Boolean.parseBoolean(retinaEnable);
        return otherInstances.computeIfAbsent(
                address, addr -> new RetinaService(addr.getHostText(), addr.getPort(), enabled)
        );
    }

    private final ManagedChannel channel;
    private final RetinaWorkerServiceGrpc.RetinaWorkerServiceBlockingStub stub;
    private final RetinaWorkerServiceGrpc.RetinaWorkerServiceStub asyncStub;
    private boolean isShutdown;
    private final boolean enabled;

    private RetinaService(String host, int port, boolean enabled)
    {
        this.enabled = enabled;
        if (enabled)
        {
            assert (host != null);
            assert (port > 0 && port <= 65535);
            this.channel = ManagedChannelBuilder.forAddress(host, port).
                    usePlaintext().build();
            this.stub = RetinaWorkerServiceGrpc.newBlockingStub(this.channel);
            this.asyncStub = RetinaWorkerServiceGrpc.newStub(this.channel);
        } else
        {
            this.channel = null;
            this.stub = null;
            this.asyncStub = null;
        }
        this.isShutdown = false;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutdown)
        {
            if (this.channel != null)
            {
                // Wait for at most 5 seconds, this should be enough to shut down an RPC client.
                this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
            this.isShutdown = true;
        }
    }

    public boolean updateRecord(String schemaName, int virtualNodeId, List<RetinaProto.TableUpdateData> tableUpdateData) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.UpdateRecordRequest request = RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setVirtualNodeId(virtualNodeId)
                .addAllTableUpdateData(tableUpdateData)
                .build();
        RetinaProto.UpdateRecordResponse response = this.stub.updateRecord(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to update record: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return true;
    }

    public static class StreamHandler implements AutoCloseable
    {
        private final Logger logger = LogManager.getLogger(StreamHandler.class);
        private StreamObserver<RetinaProto.UpdateRecordRequest> requestObserver;
        private final CountDownLatch finishLatch;
        private volatile boolean isClosed = false;
        protected final Map<String, CompletableFuture<RetinaProto.UpdateRecordResponse>> pendingRequests =
                new ConcurrentHashMap<>();

        StreamHandler(CountDownLatch finishLatch)
        {
            this.finishLatch = finishLatch;
        }

        void setRequestObserver(StreamObserver<RetinaProto.UpdateRecordRequest> requestObserver)
        {
            this.requestObserver = requestObserver;
        }

        public CompletableFuture<RetinaProto.UpdateRecordResponse> updateRecord(String schemaName, int vNodeId, List<RetinaProto.TableUpdateData> tableUpdateData) throws RetinaException
        {
            if (isClosed)
            {
                throw new RetinaException("Stream is already closed");
            }

            String token = UUID.randomUUID().toString();
            CompletableFuture<RetinaProto.UpdateRecordResponse> future = new CompletableFuture<>();
            pendingRequests.put(token, future);

            RetinaProto.UpdateRecordRequest request = RetinaProto.UpdateRecordRequest.newBuilder()
                    .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                    .setSchemaName(schemaName)
                    .setVirtualNodeId(vNodeId)
                    .addAllTableUpdateData(tableUpdateData)
                    .build();
            
            try
            {
                requestObserver.onNext(request);
            } catch (Exception e)
            {
                throw new RetinaException("Failed to send update record request", e);
            }

            return future;
        }

        public void completeResponse(RetinaProto.UpdateRecordResponse response)
        {
            String token = response.getHeader().getToken();
            CompletableFuture<RetinaProto.UpdateRecordResponse> future = pendingRequests.remove(token);
            if (future != null)
            {
                if (response.getHeader().getErrorCode() == 0)
                {
                    future.complete(response);
                } else
                {
                    future.completeExceptionally(
                            new RuntimeException("Server error: " + response.getHeader().getErrorMsg()));
                }
            } else
            {
                logger.warn("Received response for unknown token: {}", token);
            }
        }

        @Override
        public void close()
        {
            if (!isClosed)
            {
                isClosed = true;
                requestObserver.onCompleted();
                try
                {
                    if (!finishLatch.await(5, TimeUnit.SECONDS))
                    {
                        logger.warn("Stream completion did not finish in time");
                    }
                } catch (InterruptedException e)
                {
                    logger.error("Interrupted while waiting for stream completion", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public StreamHandler startUpdateStream()
    {
        CountDownLatch latch = new CountDownLatch(1);
        StreamHandler handler = new StreamHandler(latch);

        StreamObserver<RetinaProto.UpdateRecordResponse> responseObserver = new StreamObserver<RetinaProto.UpdateRecordResponse>()
        {
            @Override
            public void onNext(RetinaProto.UpdateRecordResponse response)
            {
                if (response.getHeader().getErrorCode() != 0)
                {
                    logger.error("Stream update record failed: {}", response.getHeader().getErrorMsg());
                }
                handler.completeResponse(response);
            }

            @Override
            public void onError(Throwable t)
            {
                logger.error("Retina Stream update failed from server side", t);
                latch.countDown();
            }

            @Override
            public void onCompleted()
            {
                latch.countDown();
            }
        };

        StreamObserver<RetinaProto.UpdateRecordRequest> requestObserver = asyncStub.streamUpdateRecord(responseObserver);
        handler.setRequestObserver(requestObserver);
        return handler;
    }

    public boolean addVisibility(String filePath) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.AddVisibilityRequest request = RetinaProto.AddVisibilityRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setFilePath(filePath)
                .build();
        RetinaProto.AddVisibilityResponse response = this.stub.addVisibility(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to add visibility: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return true;
    }

    public long[][] queryVisibility(long fileId, int[] rgIds, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.QueryVisibilityRequest request = RetinaProto.QueryVisibilityRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setFileId(fileId)
                .addAllRgIds(Arrays.stream(rgIds).boxed().collect(Collectors.toList()))
                .setTimestamp(timestamp)
                .build();
        RetinaProto.QueryVisibilityResponse response = this.stub.queryVisibility(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to query visibility: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        long[][] visibilityBitmaps = new long[rgIds.length][];
        for (int i = 0; i < response.getBitmapsCount(); i++)
        {
            RetinaProto.VisibilityBitmap bitmap = response.getBitmaps(i);
            visibilityBitmaps[i] = bitmap.getBitmapList().stream().mapToLong(Long::longValue).toArray();
        }
        return visibilityBitmaps;
    }

    public boolean reclaimVisibility(long fileId, int[] rgIds, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.ReclaimVisibilityRequest request = RetinaProto.ReclaimVisibilityRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setFileId(fileId)
                .addAllRgIds(Arrays.stream(rgIds).boxed().collect(Collectors.toList()))
                .setTimestamp(timestamp)
                .build();
        RetinaProto.ReclaimVisibilityResponse response = this.stub.reclaimVisibility(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to garbage collect: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return true;
    }

    public RetinaProto.GetWriteBufferResponse getWriteBuffer(String schemaName, String tableName, int virtualNodeId, long timeStamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.GetWriteBufferRequest request = RetinaProto.GetWriteBufferRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setVirtualNodeId(virtualNodeId)
                .setTimestamp(timeStamp)
                .build();
        RetinaProto.GetWriteBufferResponse response = this.stub.getWriteBuffer(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Schema: " + schemaName + "\tTable: " + tableName + ", failed to get superversion: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return response;
    }

    public boolean addWriteBuffer(String schemaName, String tableName) throws RetinaException
    {
        /**
         * Since pixels-core was not introduced, TypeDescription cannot be used to represent the schema.
         * Ultimately, it is converted to a string and transmitted via bytes, so it does not matter.
         */
        String token = UUID.randomUUID().toString();
        RetinaProto.AddWriteBufferRequest request = RetinaProto.AddWriteBufferRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();
        RetinaProto.AddWriteBufferResponse response = this.stub.addWriteBuffer(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to add writer: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return true;
    }

    /**
     * Register a long-running query to be offloaded to disk checkpoint.
     *
     * @param timestamp the transaction timestamp
     * @return true on success
     * @throws RetinaException if the operation fails
     */
    public boolean registerOffload(long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.RegisterOffloadRequest request = RetinaProto.RegisterOffloadRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setTimestamp(timestamp)
                .build();
        RetinaProto.RegisterOffloadResponse response = this.stub.registerOffload(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to register offload: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return true;
    }

    /**
     * Unregister a long-running query's offload checkpoint when the query completes.
     *
     * @param timestamp the transaction timestamp
     * @return true on success
     * @throws RetinaException if the operation fails
     */
    public boolean unregisterOffload(long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.UnregisterOffloadRequest request = RetinaProto.UnregisterOffloadRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setTimestamp(timestamp)
                .build();
        RetinaProto.UnregisterOffloadResponse response = this.stub.unregisterOffload(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Failed to unregister offload: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("Response token does not match");
        }
        return true;
    }
}
