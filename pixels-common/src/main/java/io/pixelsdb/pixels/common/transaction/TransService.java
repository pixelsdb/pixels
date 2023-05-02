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
package io.pixelsdb.pixels.common.transaction;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;

import java.util.concurrent.TimeUnit;

/**
 * @create 2022-02-20
 * @update 2023-05-02 merge transaction context management into trans service.
 * @author hank
 */
public class TransService
{
    private final ManagedChannel channel;
    private final TransServiceGrpc.TransServiceBlockingStub stub;

    public TransService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = TransServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * @param readOnly true if the transaction is determined to be read only, false otherwise.
     * @return the initialized context of the transaction, containing the allocated trans id and timestamp.
     * @throws TransException
     */
    public TransContext beginTrans(boolean readOnly) throws TransException
    {
        TransProto.BeginTransRequest request = TransProto.BeginTransRequest.newBuilder().setReadOnly(readOnly).build();
        try
        {
            TransProto.BeginTransResponse response = this.stub.beginTrans(request);
            if (response.getErrorCode() != ErrorCode.SUCCESS)
            {
                throw new TransException("failed to get query transaction info, error code=" + response.getErrorCode());
            }
            return new TransContext(response.getTransId(), response.getTimestamp(), readOnly);
        }
        catch (Exception e)
        {
            throw new TransException("failed to begin transaction", e);
        }
    }

    public int pushLowWatermark(long queryTimestamp) throws TransException
    {
        TransProto.PushLowWatermarkRequest request = TransProto.PushLowWatermarkRequest.newBuilder()
                .setQueryTransTimestamp(queryTimestamp).build();
        try
        {
            TransProto.PushLowWatermarkResponse response = this.stub.pushLowWatermark(request);
            return response.getErrorCode();
        }
        catch (Exception e)
        {
            throw new TransException("failed to push low watermark", e);
        }
    }

    public int pushHighWatermark(long writeTransTimestamp) throws TransException
    {
        TransProto.PushHighWatermarkRequest request = TransProto.PushHighWatermarkRequest.newBuilder()
                .setWriteTransTimestamp(writeTransTimestamp).build();
        try
        {
            TransProto.PushHighWatermarkResponse response = this.stub.pushHighWatermark(request);
            return response.getErrorCode();
        }
        catch (Exception e)
        {
            throw new TransException("failed to push high watermark", e);
        }
    }
}
