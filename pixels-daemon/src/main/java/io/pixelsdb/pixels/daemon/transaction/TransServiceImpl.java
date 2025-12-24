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
import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.lock.PersistentAutoIncrement;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
 * @update 2025-10-04 use trans id as trans ts for non-readonly transactions,
 * and remove transaction timestamp in commit and commit-batch
 * @update 2025-11-09 support transaction lease for non-readonly transactions
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
    private static final ScheduledExecutorService leaseCheckAndCleanup;

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
            int watermarkInterval = Constants.TRANS_WATERMARKS_CHECKPOINT_INTERVAL_SEC;
            watermarksCheckpoint.scheduleAtFixedRate(() -> {
                EtcdUtil.Instance().putKeyValue(Constants.TRANS_LOW_WATERMARK_KEY, Long.toString(lowWatermark.get()));
                EtcdUtil.Instance().putKeyValue(Constants.TRANS_HIGH_WATERMARK_KEY, Long.toString(highWatermark.get()));
            }, watermarkInterval, watermarkInterval, TimeUnit.SECONDS);

            int leaseCheckInterval = Constants.TRANS_LEASE_CHECK_INTERVAL_SEC;
            leaseCheckAndCleanup = Executors.newSingleThreadScheduledExecutor();
            leaseCheckAndCleanup.scheduleAtFixedRate(() -> {
                List<Long> expiredTransIds = TransContextManager.Instance().getExpiredTransIds();
                for (long expiredTransId : expiredTransIds)
                {
                    boolean success = TransContextManager.Instance().setTransRollback(expiredTransId);
                    logger.debug("transaction {} has been rolled back successfully ({}) due to lease expire",
                            expiredTransId, success);
                    /* Issue #1245:
                     * Push watermarks after transaction termination to ensure watermarks are pushed correctly when the
                     * transactions terminate in a different order than begin.
                     */
                    pushWatermarks(false);
                }
            }, leaseCheckInterval, leaseCheckInterval, TimeUnit.SECONDS);

        } catch (EtcdException e)
        {
            logger.error("failed to create persistent auto-increment ids for transaction service", e);
            throw new RuntimeException("failed to create persistent auto-increment ids for transaction service", e);
        }
    }

    public TransServiceImpl()
    {
    }

    @Override
    public void beginTrans(TransProto.BeginTransRequest request,
                           StreamObserver<TransProto.BeginTransResponse> responseObserver)
    {
        TransProto.BeginTransResponse response;
        try
        {
            long transId = TransServiceImpl.transId.getAndIncrement();
            /* Issue #1234:
             * HWM means all transactions with a timestamp below it are commited, hence query should get a
             * timestamp = HWM -1 instead of HWM.
             */
            long timestamp = request.getReadOnly() ? highWatermark.get() - 1 : transId;
            TransContext context;
            if (request.getReadOnly())
            {
                response = TransProto.BeginTransResponse.newBuilder()
                        .setErrorCode(ErrorCode.SUCCESS).setTransId(transId).setTimestamp(timestamp).build();
                context = new TransContext(transId, timestamp, 0L, 0L, request.getReadOnly());
            }
            else
            {
                // Issue #1163: lease is only required for non-readonly transactions.
                long leaseStartMs = System.currentTimeMillis();
                long leasePeriodMs = TRANS_LEASE_PERIOD_MS;
                response = TransProto.BeginTransResponse.newBuilder()
                        .setErrorCode(ErrorCode.SUCCESS).setTransId(transId).setTimestamp(timestamp)
                        .setLeaseStartMs(leaseStartMs).setLeasePeriodMs(leasePeriodMs).build();
                context = new TransContext(transId, timestamp, leaseStartMs, leasePeriodMs, request.getReadOnly());
            }
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
                /* Issue #1234:
                 * HWM means all transactions with a timestamp below it are commited, hence query should get a
                 * timestamp = HWM -1 instead of HWM.
                 */
                long timestamp = highWatermark.get() - 1;
                for (int i = 0; i < numTrans; i++, transId++)
                {
                    response.addTransIds(transId).addTimestamps(timestamp);
                    TransContext context = new TransContext(transId, timestamp, 0L, 0L, true);
                    contexts[i] = context;
                }
            }
            else
            {
                long timestamp = transId;
                // Issue #1163: lease is only required for non-readonly transactions.
                long leaseStartMs = System.currentTimeMillis();
                long leasePeriodMs = TRANS_LEASE_PERIOD_MS;
                for (int i = 0; i < numTrans; i++, transId++, timestamp++)
                {
                    response.addTransIds(transId).addTimestamps(timestamp)
                            .addLeaseStartMses(leaseStartMs).addLeasePeriodMses(leasePeriodMs);
                    TransContext context = new TransContext(transId, timestamp, leaseStartMs, leasePeriodMs, false);
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
        // must get transaction context before terminating transaction
        TransContext context = TransContextManager.Instance().getTransContext(request.getTransId());
        if (context != null)
        {
            // Issue #1163: check lease early to set error code for lease expire.
            if (!context.isReadOnly() && context.getLease().hasExpired(System.currentTimeMillis(), Lease.Role.Assigner))
            {
                boolean success = TransContextManager.Instance().setTransRollback(request.getTransId());
                if (!success)
                {
                    error = ErrorCode.TRANS_ROLLBACK_FAILED;
                }
                else
                {
                    error = ErrorCode.TRANS_LEASE_EXPIRED;
                }
            }
            else
            {
                boolean success = TransContextManager.Instance().setTransCommit(request.getTransId());
                if (!success)
                {
                    error = ErrorCode.TRANS_COMMIT_FAILED;
                }
            }
            /* Issue #1245:
             * Push watermarks after transaction termination to ensure watermarks are pushed correctly when the
             * transactions terminate in a different order than begin.
             */
            pushWatermarks(context.isReadOnly());
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
        TransProto.CommitTransBatchResponse.Builder responseBuilder = TransProto.CommitTransBatchResponse.newBuilder();
        if (request.getTransIdsCount() == 0)
        {
            logger.error("the count of transaction ids is zero, no transactions to commit");
            responseBuilder.setErrorCode(ErrorCode.TRANS_INVALID_ARGUMENT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }
        final int numTrans = request.getTransIdsCount();
        List<Boolean> success = new ArrayList<>(numTrans);
        // Issue #1163: lease is checked inside setTransCommitBatch, we do not set dedicated error code for expired leases.
        boolean allSuccess = TransContextManager.Instance().setTransCommitBatch(request.getTransIdsList(), success);
        if (allSuccess)
        {
            responseBuilder.setErrorCode(ErrorCode.SUCCESS);
        }
        else
        {
            responseBuilder.setErrorCode(ErrorCode.TRANS_BATCH_PARTIAL_COMMIT_FAILED);
        }
        /* Issue #1245:
         * Push watermarks after transaction termination to ensure watermarks are pushed correctly when the
         * transactions terminate in a different order than begin.
         */
        pushWatermarks(request.getReadOnly());
        responseBuilder.addAllResults(success);
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
            // must get transaction context before setTransRollback()
            TransContext context = TransContextManager.Instance().getTransContext(request.getTransId());
            if (context != null)
            {
                if (!context.isReadOnly() && context.getLease().hasExpired(System.currentTimeMillis(), Lease.Role.Assigner))
                {
                    error = ErrorCode.TRANS_LEASE_EXPIRED;
                }
                // rollback the transaction no mater it has expired or not
                boolean success = TransContextManager.Instance().setTransRollback(request.getTransId());
                if (!success)
                {
                    error = ErrorCode.TRANS_ROLLBACK_FAILED;
                }
                /* Issue #1245:
                 * Push watermarks after transaction termination to ensure watermarks are pushed correctly when the
                 * transactions terminate in a different order than begin.
                 */
                pushWatermarks(context.isReadOnly());
            }
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
    public void extendTransLease(TransProto.ExtendTransLeaseRequest request,
                                 StreamObserver<TransProto.ExtendTransLeaseResponse> responseObserver)
    {
        int error = ErrorCode.SUCCESS;
        long newLeaseStartMs = -1;
        if (TransContextManager.Instance().isTransExist(request.getTransId()))
        {
            newLeaseStartMs = TransContextManager.Instance().extendTransLease(request.getTransId());
            if (newLeaseStartMs < 0)
            {
                if (newLeaseStartMs == -1L)
                {
                    error = ErrorCode.TRANS_LEASE_EXPIRED;
                }
                else if (newLeaseStartMs == -2L)
                {
                    error = ErrorCode.TRANS_ID_NOT_EXIST;
                }
                else
                {
                    error = ErrorCode.TRANS_EXTEND_LEASE_FAILED;
                }
            }
        }
        else
        {
            logger.error("transaction id {} does not exist in the context manager", request.getTransId());
            error = ErrorCode.TRANS_ID_NOT_EXIST;
        }

        TransProto.ExtendTransLeaseResponse response = TransProto.ExtendTransLeaseResponse.newBuilder()
                .setErrorCode(error).setNewLeaseStartMs(newLeaseStartMs).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void extendTransLeaseBatch(TransProto.ExtendTransLeaseBatchRequest request,
                                      StreamObserver<TransProto.ExtendTransLeaseBatchResponse> responseObserver)
    {
        TransProto.ExtendTransLeaseBatchResponse.Builder responseBuilder =
                TransProto.ExtendTransLeaseBatchResponse.newBuilder();

        if (request.getTransIdsCount() == 0)
        {
            logger.error("the count of transaction ids is zero, no transactions to commit");
            responseBuilder.setErrorCode(ErrorCode.TRANS_INVALID_ARGUMENT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        final int numTrans = request.getTransIdsCount();
        int errorCode = ErrorCode.SUCCESS;
        List<Boolean> success = new ArrayList<>(numTrans);
        long newLeaseStartMs = TransContextManager.Instance().extendTransLeaseBatch(request.getTransIdsList(), success);
        if (numTrans != success.size())
        {
            errorCode = ErrorCode.TRANS_BATCH_EXTEND_LEASE_FAILED;
        }
        else
        {
            responseBuilder.addAllSuccess(success);
            responseBuilder.setNewLeaseStartMs(newLeaseStartMs);
        }
        responseBuilder.setErrorCode(errorCode);
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * To push the watermarks correctly, this method must be called after the termination of a transaction.
     * @param readOnly whether the transaction is readonly
     */
    private static void pushWatermarks(boolean readOnly)
    {
        long timestamp = TransContextManager.Instance().getMinRunningTransTimestamp(readOnly);
        if (timestamp < 0)
        {
            /* Issue #1245:
             * In case of no running transactions, push low/high watermark right behind the max timestamp of
             * terminated readonly/non-readonly transactions.
             * Thus, the existing terminated transactions are visible.
             */
            timestamp = TransContextManager.Instance().getMaxTerminatedTransTimestamp(readOnly) + 1;
            /* Double check if there are running transactions, because there is an interval between previous
             * getMinRunningTransTimestamp() and getMaxTerminatedTransTimestamp(), new transactions may begin and
             * terminate during this interval, making it incorrect to push the watermark to the max timestamp of
             * terminated transactions.
             */
            long minRunningTransTimestamp = TransContextManager.Instance().getMinRunningTransTimestamp(readOnly);
            if (minRunningTransTimestamp > 0)
            {
                if (timestamp > 0 && minRunningTransTimestamp < timestamp)
                {
                    logger.error("new running transactions obtained backward timestamps, this is illegal");
                    return;
                }
                // push watermark to the new min running transaction timestamp
                timestamp = minRunningTransTimestamp;
            }
            if (timestamp < 0)
            {
                logger.error("trying to push watermarks before any transaction is terminated, this is illegal");
                return;
            }
        }
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

    @Override
    public void markTransOffloaded(TransProto.MarkTransOffloadedRequest request,
                                   StreamObserver<TransProto.MarkTransOffloadedResponse> responseObserver)
    {
        int error = ErrorCode.SUCCESS;
        boolean success = TransContextManager.Instance().markTransOffloaded(request.getTransId());
        if (!success)
        {
            logger.error("transaction id {} does not exist or failed to mark as offloaded", request.getTransId());
            error = ErrorCode.TRANS_ID_NOT_EXIST;
        }

        // After marking, attempt to push low watermark.
        pushWatermarks(true);
        
        TransProto.MarkTransOffloadedResponse response = TransProto.MarkTransOffloadedResponse.newBuilder()
                .setErrorCode(error).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
