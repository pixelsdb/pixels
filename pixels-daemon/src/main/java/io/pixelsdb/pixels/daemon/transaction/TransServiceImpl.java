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
package io.pixelsdb.pixels.daemon.transaction;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created at: 20/02/2022
 * Author: hank
 */
public class TransServiceImpl extends TransServiceGrpc.TransServiceImplBase
{
    private static Logger log = LogManager.getLogger(TransServiceImpl.class);

    public static AtomicLong QueryId = new AtomicLong(0);
    /**
     * Issue #174:
     * In this issue, we have not fully implemented the logic related to the watermarks.
     * So we use two atomic longs to simulate the watermarks.
     */
    public static AtomicLong LowWatermark = new AtomicLong(0);
    public static AtomicLong HighWatermark = new AtomicLong(0);

    public TransServiceImpl () { }

    @Override
    public void getQueryTimestamp(TransProto.GetQueryTimestampRequest request, StreamObserver<TransProto.GetQueryTimestampResponse> responseObserver)
    {
        TransProto.GetQueryTimestampResponse response = TransProto.GetQueryTimestampResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS)
                .setQueryId(QueryId.getAndIncrement()) // incremental query id
                .setQueryTimestamp(HighWatermark.get()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void pushLowWatermark(TransProto.PushLowWatermarkRequest request, StreamObserver<TransProto.PushLowWatermarkResponse> responseObserver)
    {
        long value = LowWatermark.get();
        int error = ErrorCode.SUCCESS;
        long queryTimestamp = request.getQueryTimestamp();
        if (queryTimestamp >= value)
        {
            while(LowWatermark.compareAndSet(value, queryTimestamp))
            {
                value = LowWatermark.get();
                if (queryTimestamp < value)
                {
                    error = ErrorCode.TRANS_LOW_WATERMARK_NOT_PUSHED;
                    break;
                }
            }
        } else
        {
            error = ErrorCode.TRANS_LOW_WATERMARK_NOT_PUSHED;
        }
        TransProto.PushLowWatermarkResponse response = TransProto.PushLowWatermarkResponse.newBuilder()
                .setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void pushHighWatermark(TransProto.PushHighWatermarkRequest request, StreamObserver<TransProto.PushHighWatermarkResponse> responseObserver)
    {
        long value = HighWatermark.get();
        int error = ErrorCode.SUCCESS;
        long writeTransTimestamp = request.getWriteTransTimestamp();
        if (writeTransTimestamp >= value)
        {
            while(HighWatermark.compareAndSet(value, writeTransTimestamp))
            {
                value = HighWatermark.get();
                if (writeTransTimestamp < value)
                {
                    error = ErrorCode.TRANS_HIGH_WATERMARK_NOT_PUSHED;
                    break;
                }
            }
        } else
        {
            error = ErrorCode.TRANS_HIGH_WATERMARK_NOT_PUSHED;
        }
        TransProto.PushHighWatermarkResponse response = TransProto.PushHighWatermarkResponse.newBuilder()
                .setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
