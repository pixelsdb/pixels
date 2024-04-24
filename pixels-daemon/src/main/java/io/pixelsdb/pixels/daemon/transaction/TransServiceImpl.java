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
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @create 2022-02-20
 * @update 2022-05-02 update protocol to support transaction context operations
 * @author hank
 */
public class TransServiceImpl extends TransServiceGrpc.TransServiceImplBase
{
    private static final Logger log = LogManager.getLogger(TransServiceImpl.class);

    public static final AtomicLong TransId = new AtomicLong(0);
    /**
     * Issue #174:
     * In this issue, we have not fully implemented the logic related to the watermarks.
     * So we use two atomic longs to simulate the watermarks.
     */
    public static final AtomicLong LowWatermark = new AtomicLong(0);
    public static final AtomicLong HighWatermark = new AtomicLong(0);

    public TransServiceImpl () { }

    @Override
    public void beginTrans(TransProto.BeginTransRequest request,
                           StreamObserver<TransProto.BeginTransResponse> responseObserver)
    {
        long transId = TransId.getAndIncrement(); // incremental transaction id
        long timestamp = HighWatermark.get();
        TransProto.BeginTransResponse response = TransProto.BeginTransResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS)
                .setTransId(transId)
                .setTimestamp(timestamp).build();
        TransContext context = new TransContext(transId, timestamp, request.getReadOnly());
        TransContextManager.Instance().addTransContext(context);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void commitTrans(TransProto.CommitTransRequest request,
                            StreamObserver<TransProto.CommitTransResponse> responseObserver)
    {
        int error = ErrorCode.SUCCESS;
        if (TransContextManager.Instance().isTransExist(request.getTransId()))
        {
            // must get transaction context before setTransCommit()
            boolean readOnly = TransContextManager.Instance().getTransContext(request.getTransId()).isReadOnly();
            boolean success = TransContextManager.Instance().setTransCommit(request.getTransId());
            if (!success)
            {
                error = ErrorCode.TRANS_COMMIT_FAILED;
            }
            long timestamp = request.getTimestamp();
            if (readOnly)
            {
                long value = LowWatermark.get();
                if (timestamp >= value)
                {
                    while (!LowWatermark.compareAndSet(value, timestamp))
                    {
                        value = LowWatermark.get();
                        if (timestamp < value)
                        {
                            // it is not an error if there is no need to push the low watermark
                            break;
                        }
                    }
                }
            } else
            {
                long value = HighWatermark.get();
                if (timestamp >= value)
                {
                    while (!HighWatermark.compareAndSet(value, timestamp))
                    {
                        value = HighWatermark.get();
                        if (timestamp < value)
                        {
                            // it is not an error if there is no need to push the high watermark
                            break;
                        }
                    }
                }
            }
        }
        else
        {
            log.error(String.format("transaction id %d does not exist in the context manager", request.getTransId()));
            error = ErrorCode.TRANS_ID_NOT_EXIST;
        }

        TransProto.CommitTransResponse response =
                TransProto.CommitTransResponse.newBuilder().setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void rollbackTrans(TransProto.RollbackTransRequest request,
                              StreamObserver<TransProto.RollbackTransResponse> responseObserver)
    {
        int error = ErrorCode.SUCCESS;
        if (TransContextManager.Instance().isTransExist(request.getTransId()))
        {
            if (!TransContextManager.Instance().setTransRollback(request.getTransId()))
            {
                error = ErrorCode.TRANS_ROLLBACK_FAILED;
            }
        }
        else
        {
            log.error(String.format("transaction id %d does not exist in the context manager", request.getTransId()));
            error = ErrorCode.TRANS_ID_NOT_EXIST;
        }

        TransProto.RollbackTransResponse response =
                TransProto.RollbackTransResponse.newBuilder().setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getTransContext(TransProto.GetTransContextRequest request,
                                StreamObserver<TransProto.GetTransContextResponse> responseObserver)
    {
        TransContext context = null;
        if (request.hasTransId())
        {
            context = TransContextManager.Instance().getTransContext(request.getTransId());
        }
        else if (request.hasExternalTraceId())
        {
            context = TransContextManager.Instance().getTransContext(request.getExternalTraceId());

        }
        TransProto.GetTransContextResponse.Builder builder = TransProto.GetTransContextResponse.newBuilder();
        if (context != null)
        {
            builder.setErrorCode(ErrorCode.SUCCESS).setTransContext(context.toProtobuf());
        }
        else
        {
            builder.setErrorCode(ErrorCode.TRANS_CONTEXT_NOT_FOUND);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void setTransProperty(TransProto.SetTransPropertyRequest request,
                                 StreamObserver<TransProto.SetTransPropertyResponse> responseObserver)
    {
        TransContext context = null;
        if (request.hasTransId())
        {
            context = TransContextManager.Instance().getTransContext(request.getTransId());
        }
        else if (request.hasExternalTraceId())
        {
            context = TransContextManager.Instance().getTransContext(request.getExternalTraceId());

        }
        String key = request.getKey();
        String value = request.getValue();
        TransProto.SetTransPropertyResponse.Builder builder = TransProto.SetTransPropertyResponse.newBuilder();
        if (context != null)
        {
            String prevValue = (String) context.getProperties().setProperty(key, value);
            if (prevValue != null)
            {
                builder.setPrevValue(prevValue);
            }
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        else
        {
            builder.setErrorCode(ErrorCode.TRANS_CONTEXT_NOT_FOUND);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateQueryCosts(TransProto.UpdateQueryCostsRequest request,
                                 StreamObserver<TransProto.UpdateQueryCostsResponse> responseObserver)
    {
        TransContext context = null;
        if (request.hasTransId())
        {
            context = TransContextManager.Instance().getTransContext(request.getTransId());
        }
        else if (request.hasExternalTraceId())
        {
            context = TransContextManager.Instance().getTransContext(request.getExternalTraceId());

        }
        double newScanBytes = request.getNewScanBytes();
        double addCostCents = request.getAddCostCents();
        TransProto.UpdateQueryCostsResponse.Builder builder = TransProto.UpdateQueryCostsResponse.newBuilder();
        if (context != null)
        {
            context.getProperties().setProperty(Constants.TRANS_CONTEXT_SCAN_BYTES_KEY, String.valueOf(newScanBytes));
            double curCostCents = Double.parseDouble(
                    context.getProperties().getProperty(Constants.TRANS_CONTEXT_COST_CENTS_KEY, "0"));
            context.getProperties().setProperty(Constants.TRANS_CONTEXT_COST_CENTS_KEY,
                    String.valueOf(curCostCents + addCostCents));
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        else
        {
            builder.setErrorCode(ErrorCode.TRANS_CONTEXT_NOT_FOUND);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTransConcurrency(TransProto.GetTransConcurrencyRequest request,
                                    StreamObserver<TransProto.GetTransConcurrencyResponse> responseObserver) {
        int concurrency = TransContextManager.Instance().getQueryConcurrency(request.getReadOnly());
        TransProto.GetTransConcurrencyResponse response = TransProto.GetTransConcurrencyResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS).setConcurrency(concurrency).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void bindExternalTraceId(TransProto.BindExternalTraceIdRequest request,
                                    StreamObserver<TransProto.BindExternalTraceIdResponse> responseObserver) {
        boolean success = TransContextManager.Instance().bindExternalTraceId(
                request.getTransId(), request.getExternalTraceId());
        TransProto.BindExternalTraceIdResponse response = TransProto.BindExternalTraceIdResponse.newBuilder()
                .setErrorCode(success ? ErrorCode.SUCCESS : ErrorCode.TRANS_ID_NOT_EXIST).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
