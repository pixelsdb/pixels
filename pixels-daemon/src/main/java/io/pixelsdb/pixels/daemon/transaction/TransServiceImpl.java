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

import io.etcd.jetcd.KeyValue;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.lock.PersistentAutoIncrement;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.pixelsdb.pixels.common.utils.Constants.TRANS_LEASE_PERIOD_MS;

/**
 * @author hank, gengdy
 * @create 2022-02-20
 * @update 2022-05-02 update protocol to support transaction context operations
 * @update 2025-06-07 support begin transactions in batch
 * @update 2025-09-30 support commit transactions in batch (gengdy)
 * @update 2025-10-04 remove transaction timestamp in commit and commit-batch
 */
public class TransServiceImpl extends TransServiceGrpc.TransServiceImplBase
{
    private static final Logger logger = LogManager.getLogger(TransServiceImpl.class);

    /**
     * transId is monotonically increasing.
     * <br/>
     * Issue #1099:
     * For non-readonly transactions, transId is also used as the transaction timestamp.
     * For readonly transactions, the current high watermark is used as the transaction timestamp.
     */
    private static final PersistentAutoIncrement transId;
    /**
     * Issue #174:
     * In this issue, we have not fully implemented the logic related to the watermarks.
     * So we use two atomic longs to simulate the watermarks.
     */
    private static final AtomicLong lowWatermark;
    private static final AtomicLong highWatermark;

    private static final ScheduledExecutorService watermarksCheckpoint;

    static
    {
        try
        {
            transId = new PersistentAutoIncrement(Constants.AI_TRANS_ID_KEY, false);
            KeyValue lowWatermarkKv = EtcdUtil.Instance().getKeyValue(Constants.TRANS_LOW_WATERMARK_KEY);
            if (lowWatermarkKv == null)
            {
                EtcdUtil.Instance().putKeyValue(Constants.TRANS_LOW_WATERMARK_KEY, "0");
                lowWatermark = new AtomicLong(0);
            }
            else
            {
                lowWatermark = new AtomicLong(Long.parseLong(lowWatermarkKv.getValue().toString(StandardCharsets.UTF_8)));
            }
            KeyValue highWatermarkKv = EtcdUtil.Instance().getKeyValue(Constants.TRANS_HIGH_WATERMARK_KEY);
            if (highWatermarkKv == null)
            {
                EtcdUtil.Instance().putKeyValue(Constants.TRANS_HIGH_WATERMARK_KEY, "0");
                highWatermark = new AtomicLong(0);
            }
            else
            {
                highWatermark = new AtomicLong(Long.parseLong(highWatermarkKv.getValue().toString(StandardCharsets.UTF_8)));
            }
            watermarksCheckpoint = Executors.newSingleThreadScheduledExecutor();
            int period = Constants.TRANS_WATERMARKS_CHECKPOINT_PERIOD_SEC;
            watermarksCheckpoint.scheduleAtFixedRate(() -> {
                EtcdUtil.Instance().putKeyValue(Constants.TRANS_LOW_WATERMARK_KEY, Long.toString(lowWatermark.get()));
                EtcdUtil.Instance().putKeyValue(Constants.TRANS_HIGH_WATERMARK_KEY, Long.toString(highWatermark.get()));
            }, period, period, TimeUnit.SECONDS);
        } catch (EtcdException e)
        {
            logger.error("failed to create persistent auto-increment ids for transaction service", e);
            throw new RuntimeException("failed to create persistent auto-increment ids for transaction service", e);
        }
    }

    public TransServiceImpl() { }

    @Override
    public void beginTrans(TransProto.BeginTransRequest request,
                           StreamObserver<TransProto.BeginTransResponse> responseObserver)
    {
        TransProto.BeginTransResponse response;
        try
        {
            long transId = TransServiceImpl.transId.getAndIncrement();
            long timestamp = request.getReadOnly() ? highWatermark.get() : transId;
            long leaseStartMs = request.getReadOnly() ? 0L : System.currentTimeMillis();
            long leasePeriodMs = request.getReadOnly() ? 0L : TRANS_LEASE_PERIOD_MS;
            response = TransProto.BeginTransResponse.newBuilder()
                    .setErrorCode(ErrorCode.SUCCESS).setTransId(transId).setTimestamp(timestamp)
                    .setLeaseStartMs(leaseStartMs).setLeasePeriodMs(leasePeriodMs).build();
            TransContext context = new TransContext(transId, timestamp, leaseStartMs, leasePeriodMs, request.getReadOnly());
            TransContextManager.Instance().addTransContext(context);
        } catch (EtcdException e)
        {
            response = TransProto.BeginTransResponse.newBuilder()
                    .setErrorCode(ErrorCode.TRANS_GENERATE_ID_OR_TS_FAILED)
                    .setTransId(0).setTimestamp(0).build();
            logger.error("failed to generate transaction id or timestamp", e);
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void beginTransBatch(TransProto.BeginTransBatchRequest request,
                                StreamObserver<TransProto.BeginTransBatchResponse> responseObserver)
    {
        TransProto.BeginTransBatchResponse.Builder response = TransProto.BeginTransBatchResponse.newBuilder();
        try
        {
            final int numTrans = request.getExpectNumTrans();
            long transId = TransServiceImpl.transId.getAndIncrement(numTrans);
            TransContext[] contexts = new TransContext[numTrans];
            if (request.getReadOnly())
            {
                long timestamp = highWatermark.get();
                for (int i = 0; i < numTrans; i++, transId++)
                {
                    response.addTransIds(transId).addTimestamps(timestamp).addLeaseStartMses(0L).addLeasePeriodMses(0L);
                    TransContext context = new TransContext(transId, timestamp, 0L, 0L, true);
                    contexts[i] = context;
                }
            }
            else
            {
                long timestamp = transId;
                long leaseStartMs = System.currentTimeMillis();
                for (int i = 0; i < numTrans; i++, transId++, timestamp++)
                {
                    response.addTransIds(transId).addTimestamps(timestamp)
                            .addLeaseStartMses(leaseStartMs).addLeasePeriodMses(TRANS_LEASE_PERIOD_MS);
                    TransContext context = new TransContext(transId, timestamp, leaseStartMs, TRANS_LEASE_PERIOD_MS, false);
                    contexts[i] = context;
                }
            }
            TransContextManager.Instance().addTransContextBatch(contexts);
            response.setExactNumTrans(request.getExpectNumTrans());
            response.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (EtcdException e)
        {
            response.setErrorCode(ErrorCode.TRANS_GENERATE_ID_OR_TS_FAILED);
            logger.error("failed to generate transaction id or timestamp", e);
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void commitTrans(TransProto.CommitTransRequest request,
                            StreamObserver<TransProto.CommitTransResponse> responseObserver)
    {
        int error = ErrorCode.SUCCESS;
        TransContext context = TransContextManager.Instance().getTransContext(request.getTransId());
        if (context != null)
        {
            /*
             * Issue #755:
             * push the watermarks before setTransCommit()
             * ensure pushWatermarks calls getMinRunningTransTimestamp() to get the correct value
             */
            pushWatermarks(context.isReadOnly());
            boolean success = TransContextManager.Instance().setTransCommit(request.getTransId());
            if (!success)
            {
                error = ErrorCode.TRANS_COMMIT_FAILED;
            }
        }
        else
        {
            logger.error("transaction id {} does not exist in the context manager", request.getTransId());
            error = ErrorCode.TRANS_ID_NOT_EXIST;
        }

        TransProto.CommitTransResponse response =
                TransProto.CommitTransResponse.newBuilder().setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void commitTransBatch(TransProto.CommitTransBatchRequest request,
                                 StreamObserver<TransProto.CommitTransBatchResponse> responseObserver)
    {
        TransProto.CommitTransBatchResponse.Builder responseBuilder =
                TransProto.CommitTransBatchResponse.newBuilder();

        if (request.getTransIdsCount() == 0)
        {
            logger.error("the count of transaction ids is zero, no transactions to commit");
            responseBuilder.setErrorCode(ErrorCode.TRANS_INVALID_ARGUMENT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        final int numTrans = request.getTransIdsCount();
        long[] transIds = new long[numTrans];
        int errorCode = ErrorCode.SUCCESS;
        for (int i = 0; i < numTrans; ++i)
        {
            long transId = request.getTransIds(i);
            transIds[i] = transId;
            TransContext context = TransContextManager.Instance().getTransContext(transId);
            if (context != null)
            {
                /*
                 * Issue #755:
                 * push the watermarks before setTransCommit()
                 * ensure pushWatermarks calls getMinRunningTransTimestamp() to get the correct value
                 */
                pushWatermarks(context.isReadOnly());
            }
            else
            {
                logger.error("transaction id {} does not exist in the context manager", transId);
                errorCode = ErrorCode.TRANS_BATCH_PARTIAL_ID_NOT_EXIST;
            }
        }
        Boolean[] success = new Boolean[numTrans];
        boolean allSuccess = TransContextManager.Instance().setTransCommitBatch(transIds, success);
        responseBuilder.addAllResults(Arrays.asList(success));
        if (allSuccess)
        {
            responseBuilder.setErrorCode(ErrorCode.SUCCESS);
        }
        else
        {
            if (errorCode != ErrorCode.TRANS_BATCH_PARTIAL_ID_NOT_EXIST)
            {
                // other errors occurred
                responseBuilder.setErrorCode(ErrorCode.TRANS_BATCH_PARTIAL_COMMIT_FAILED);
            }
            else
            {
                responseBuilder.setErrorCode(errorCode);
            }
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void rollbackTrans(TransProto.RollbackTransRequest request,
                              StreamObserver<TransProto.RollbackTransResponse> responseObserver)
    {
        int error = ErrorCode.SUCCESS;
        if (TransContextManager.Instance().isTransExist(request.getTransId()))
        {
            // must get transaction context before setTransCommit()
            boolean readOnly = TransContextManager.Instance().getTransContext(request.getTransId()).isReadOnly();
            boolean success = TransContextManager.Instance().setTransRollback(request.getTransId());
            if (!success)
            {
                error = ErrorCode.TRANS_ROLLBACK_FAILED;
            }
            pushWatermarks(readOnly);
        }
        else
        {
            logger.error("transaction id {} does not exist in the context manager", request.getTransId());
            error = ErrorCode.TRANS_ID_NOT_EXIST;
        }

        TransProto.RollbackTransResponse response =
                TransProto.RollbackTransResponse.newBuilder().setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void extendTransLease(TransProto.ExtendTransLeaseRequest request, StreamObserver<TransProto.ExtendTransLeaseResponse> responseObserver)
    {
        super.extendTransLease(request, responseObserver);
    }

    @Override
    public void extendTransLeaseBatch(TransProto.ExtendTransLeaseBatchRequest request, StreamObserver<TransProto.ExtendTransLeaseBatchResponse> responseObserver)
    {
        super.extendTransLeaseBatch(request, responseObserver);
    }

    private void pushWatermarks(boolean readOnly)
    {
        long timestamp = TransContextManager.Instance().getMinRunningTransTimestamp(readOnly);
        if (readOnly)
        {
            long value = lowWatermark.get();
            if (timestamp > value)
            {
                while (!lowWatermark.compareAndSet(value, timestamp))
                {
                    value = lowWatermark.get();
                    if (timestamp <= value)
                    {
                        // it is not an error if there is no need to push the low watermark
                        break;
                    }
                }
            }
        }
        else
        {
            long value = highWatermark.get();
            if (timestamp > value)
            {
                while (!highWatermark.compareAndSet(value, timestamp))
                {
                    value = highWatermark.get();
                    if (timestamp <= value)
                    {
                        // it is not an error if there is no need to push the high watermark
                        break;
                    }
                }
            }
        }
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
        double newScanBytes = request.getScanBytes();
        TransProto.UpdateQueryCostsResponse.Builder builder = TransProto.UpdateQueryCostsResponse.newBuilder();
        if (context != null)
        {
            context.getProperties().setProperty(Constants.TRANS_CONTEXT_SCAN_BYTES_KEY, String.valueOf(newScanBytes));
            if (request.hasVmCostCents())
            {
                context.getProperties().setProperty(Constants.TRANS_CONTEXT_VM_COST_CENTS_KEY,
                        String.valueOf(request.getVmCostCents()));
            }
            else if (request.hasCfCostCents())
            {
                context.getProperties().setProperty(Constants.TRANS_CONTEXT_CF_COST_CENTS_KEY,
                        String.valueOf(request.getCfCostCents()));
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
    public void getTransConcurrency(TransProto.GetTransConcurrencyRequest request,
                                    StreamObserver<TransProto.GetTransConcurrencyResponse> responseObserver)
    {
        int concurrency = TransContextManager.Instance().getQueryConcurrency(request.getReadOnly());
        TransProto.GetTransConcurrencyResponse response = TransProto.GetTransConcurrencyResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS).setConcurrency(concurrency).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void bindExternalTraceId(TransProto.BindExternalTraceIdRequest request,
                                    StreamObserver<TransProto.BindExternalTraceIdResponse> responseObserver)
    {
        boolean success = TransContextManager.Instance().bindExternalTraceId(
                request.getTransId(), request.getExternalTraceId());
        TransProto.BindExternalTraceIdResponse response = TransProto.BindExternalTraceIdResponse.newBuilder()
                .setErrorCode(success ? ErrorCode.SUCCESS : ErrorCode.TRANS_ID_NOT_EXIST).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void dumpTrans(TransProto.DumpTransRequest request,
                          StreamObserver<TransProto.DumpTransResponse> responseObserver)
    {
        boolean success = TransContextManager.Instance().dumpTransContext(request.getTimestamp());
        TransProto.DumpTransResponse response = TransProto.DumpTransResponse.newBuilder()
                .setErrorCode(success ? ErrorCode.SUCCESS : ErrorCode.TRANS_ID_NOT_EXIST).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getSafeGcTimestamp(com.google.protobuf.Empty request,
                                   StreamObserver<TransProto.GetSafeGcTimestampResponse> responseObserver)
    {
        long safeTs = Math.max(0, lowWatermark.get() - 1);
        TransProto.GetSafeGcTimestampResponse response = TransProto.GetSafeGcTimestampResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS)
                .setTimestamp(safeTs)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
