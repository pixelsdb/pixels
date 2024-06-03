/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.core.RetinaProto;
import io.pixelsdb.pixels.core.RetinaServiceGrpc;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RetinaService
{
    private final ManagedChannel channel;
    private final RetinaServiceGrpc.RetinaServiceBlockingStub stub;

    public RetinaService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = RetinaServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public VectorizedRowBatch queryRecords(String schemaName, String tableName, int rgid, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.RequestHeader header = RetinaProto.RequestHeader.newBuilder().setToken(token).build();

        RetinaProto.QueryRecordsRequest request = RetinaProto.QueryRecordsRequest.newBuilder()
                .setHeader(header)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setRgid(rgid)
                .setTimestamp(timestamp)
                .build();
        try
        {
            RetinaProto.QueryRecordsResponse response = this.stub.queryRecords(request);
            if (response.getHeader().getErrorCode() != ErrorCode.SUCCESS)
            {
                throw new RetinaException("failed to queryRecords, error code=" + response.getHeader().getErrorCode());
            }
            long pos = response.getPos();
            // TODO: read records from shared memory.
//            MemoryMappedFile mmf = new MemoryMappedFile();

            RetinaProto.QueryAck ack = RetinaProto.QueryAck.newBuilder()
                    .setHeader(header)
                    .setPos(pos)
                    .build();
            RetinaProto.ResponseHeader response_ = this.stub.finishRecords(ack);
            if (response_.getErrorCode() != ErrorCode.SUCCESS)
            {
                throw new RetinaException("failed to ack queryRecords, error code=" + response.getHeader().getErrorCode());
            }

            return new VectorizedRowBatch(0);
        } catch (Exception e)
        {
            throw new RetinaException("failed to queryRecords", e);
        }
    }

    public Bitmap getVisibility(String schemaName, String tableName, int rgid, long timestamp) throws RetinaException
    {
        String token = UUID.randomUUID().toString();
        RetinaProto.RequestHeader header = RetinaProto.RequestHeader.newBuilder().setToken(token).build();

        RetinaProto.QueryVisibilityRequest request = RetinaProto.QueryVisibilityRequest.newBuilder()
                .setHeader(header)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setRgid(rgid)
                .setTimestamp(timestamp)
                .build();
        try
        {
            RetinaProto.QueryVisibilityResponse response = this.stub.queryVisibility(request);
            if (response.getHeader().getErrorCode() != ErrorCode.SUCCESS)
            {
                throw new RetinaException("failed to queryRecords, error code=" + response.getHeader().getErrorCode());
            }
            long pos = response.getPos();
            // TODO: read bitmap from shared memory.
//            MemoryMappedFile mmf = new MemoryMappedFile();
            return new Bitmap(0, false);
        } catch (Exception e)
        {
            throw new RetinaException("failed to queryRecords", e);
        }
    }
}
