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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RetinaService
{
    private static final Logger logger = LogManager.getLogger(RetinaService.class);
    private static final RetinaService defaultInstance;
    private static final Map<HostAddress, RetinaService> otherInstances = new HashMap<>();

    static
    {
        String retinaHost = ConfigFactory.Instance().getProperty("retina.server.host");
        int retinaPort = Integer.parseInt(ConfigFactory.Instance().getProperty("retina.server.port"));
        defaultInstance = new RetinaService(retinaHost, retinaPort);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
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
            }
        }));
    }

    /**
     * Get the default retina service instance connecting to the retina host:port configured in
     * PIXELS_HOME/pixels.properties. This default instance whill be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     * @return
     */
    public static RetinaService Instance() { return defaultInstance; }

    /**
     * This method should only be used to connect to a retina server that is not configured through
     * PIXELS_HOME/pixels.properties. <b>No need</b> to manually shut down the returned retina service.
     * @param host the host name of the retina server
     * @param port the port of the retina server
     * @return the created retina service instance
     */
    public static RetinaService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        RetinaService retinaService = otherInstances.get(address);
        if (retinaService != null)
        {
            return retinaService;
        }
        retinaService = new RetinaService(host, port);
        otherInstances.put(address, retinaService);
        return retinaService;
    }
    
    private final ManagedChannel channel;
    private final RetinaWorkerServiceGrpc.RetinaWorkerServiceBlockingStub stub;
    private boolean isShutdown;

    private RetinaService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = RetinaWorkerServiceGrpc.newBlockingStub(this.channel);
        this.isShutdown = false;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutdown)
        {
            // Wait for at most 5 seconds, this should be enough to shut down an RPC client.
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutdown = true;
        }
    }

    public boolean deleteRecord(long fileId, int rgId, int rgRowId, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.DeleteRecordRequest request = RetinaProto.DeleteRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setFileId(fileId)
                .setRgId(rgId)
                .setRgRowId(rgRowId)
                .setTimestamp(timestamp)
                .build();
        RetinaProto.DeleteRecordResponse response = this.stub.deleteRecord(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("failed to delete record: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        return true;
    }

    public boolean deleteRecords(long[] fileIds, int[] rgIds, int[] rgRowIds, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();

        if (fileIds == null || rgIds == null || rgRowIds == null ||
            fileIds.length != rgIds.length || rgIds.length != rgRowIds.length)
        {
            throw new RetinaException("Invalid input arrays: Length mismatch");
        }

        RetinaProto.DeleteRecordsRequest.Builder requestBuiler = RetinaProto.DeleteRecordsRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setTimestamp(timestamp);

        for (int i = 0; i < fileIds.length; i++)
        {
            RetinaProto.RowLocation row = RetinaProto.RowLocation.newBuilder()
                    .setFileId(fileIds[i])
                    .setRgId(rgIds[i])
                    .setRgRowId(rgRowIds[i])
                    .build();
            requestBuiler.addRows(row);
        }

        RetinaProto.DeleteRecordsResponse response = this.stub.deleteRecords(requestBuiler.build());
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("failed to delete records: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        return true;
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
            throw new RetinaException("failed to add visibility: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        return true;
    }

    public long[][] queryVisibility(String filePath, int[] rgIds, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.QueryVisibilityRequest request = RetinaProto.QueryVisibilityRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setFilePath(filePath)
                .addAllRgIds(Arrays.stream(rgIds).boxed().collect(Collectors.toList()))
                .setTimestamp(timestamp)
                .build();
        RetinaProto.QueryVisibilityResponse response = this.stub.queryVisibility(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("failed to query visibility: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        long[][] visibilityBitmaps = new long[rgIds.length][];
        for (int i = 0; i < response.getBitmapsCount(); i++)
        {
            RetinaProto.VisibilityBitmap bitmap = response.getBitmaps(i);
            visibilityBitmaps[i] = bitmap.getBitmapList().stream().mapToLong(Long::longValue).toArray();
        }
        return visibilityBitmaps;
    }

    public boolean garbageCollect(String filePath, int[] rgIds, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.GarbageCollectRequest request = RetinaProto.GarbageCollectRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setFilePath(filePath)
                .addAllRgIds(Arrays.stream(rgIds).boxed().collect(Collectors.toList()))
                .setTimestamp(timestamp)
                .build();
        RetinaProto.GarbageCollectResponse response = this.stub.garbageCollect(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("failed to garbage collect: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        return true;
    }

    public RetinaProto.GetSuperVersionResponse getSuperVersion(String schemaName, String tableName) throws RetinaException 
{
        String token = UUID.randomUUID().toString();
        RetinaProto.GetSuperVersionRequest request = RetinaProto.GetSuperVersionRequest.newBuilder()
            .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
            .setSchema(schemaName)
            .setTable(tableName)
            .build();
        RetinaProto.GetSuperVersionResponse response = this.stub.getSuperVersion(request);
                if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("Schema: " + schemaName + "\tTable: " + tableName + ", failed to get superversion: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        return response;
    }

    public boolean insertRecord(String schemaName, String tableName, List<ByteString> colValues, long timestamp) throws RetinaException {
        String token = UUID.randomUUID().toString();
        RetinaProto.InsertRecordRequest request = RetinaProto.InsertRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchema(schemaName)
                .setTable(tableName)
                .addAllColValues(colValues)
                .setTimestamp(timestamp)
                .build();

        RetinaProto.InsertRecordResponse response = this.stub.insertRecord(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new RetinaException("failed to insert record: " + response.getHeader().getErrorCode()
                    + " " + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new RetinaException("response token does not match.");
        }
        return true;
    }

    public boolean insertRecord(String schemaName, String tableName, byte[][] colValues, long timestamp) throws RetinaException {
        List<ByteString> colValueList = new ArrayList<>(colValues.length);
        for (byte[] col : colValues) {
            colValueList.add(ByteString.copyFrom(col));
        }
        return insertRecord(schemaName, tableName, colValueList, timestamp);
    }
}
