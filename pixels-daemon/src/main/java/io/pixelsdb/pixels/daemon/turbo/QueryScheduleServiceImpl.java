/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.daemon.turbo;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.turbo.ExecutorType;
import io.pixelsdb.pixels.turbo.QueryScheduleServiceGrpc;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2023-05-31
 */
public class QueryScheduleServiceImpl extends QueryScheduleServiceGrpc.QueryScheduleServiceImplBase
{
    private static final Logger log = LogManager.getLogger(QueryScheduleServiceImpl.class);

    @Override
    public void scheduleQuery(TurboProto.ScheduleQueryRequest request,
                              StreamObserver<TurboProto.ScheduleQueryResponse> responseObserver)
    {
        long transId = request.getTransId();
        boolean forceMpp = request.getForceMpp();
        ExecutorType executorType;
        if (forceMpp)
        {
            while (!QueryScheduleQueues.Instance().EnqueueMpp(transId))
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e)
                {
                    log.error("interrupted while waiting for retrying enqueue mpp");
                }
            }
            executorType = ExecutorType.MPP;
        }
        else
        {
            while ((executorType = QueryScheduleQueues.Instance().Enqueue(transId)) == ExecutorType.PENDING)
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e)
                {
                    log.error("interrupted while waiting for enqueue adaptively.");
                }
            }
        }
        TurboProto.ScheduleQueryResponse response = TurboProto.ScheduleQueryResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS).setExecutorType(executorType.name()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void finishQuery(TurboProto.FinishQueryRequest request,
                            StreamObserver<TurboProto.FinishQueryResponse> responseObserver)
    {
        long transId = request.getTransId();
        ExecutorType executorType = ExecutorType.valueOf(request.getExecutorType());
        boolean success = QueryScheduleQueues.Instance().Dequeue(transId, executorType);
        TurboProto.FinishQueryResponse.Builder builder = TurboProto.FinishQueryResponse.newBuilder();
        if (success)
        {
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        else
        {
            builder.setErrorCode(ErrorCode.QUERY_SCHEDULE_DEQUEUE_FAILED);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getQuerySlots(TurboProto.GetQuerySlotsRequest request,
                              StreamObserver<TurboProto.GetQuerySlotsResponse> responseObserver)
    {
        int mppSlots = QueryScheduleQueues.Instance().getMppSlots();
        int cfSlots = QueryScheduleQueues.Instance().getCfSlots();
        TurboProto.GetQuerySlotsResponse response = TurboProto.GetQuerySlotsResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS).setMppSlots(mppSlots).setCfSlots(cfSlots).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getQueryConcurrency(TurboProto.GetQueryConcurrencyRequest request,
                                    StreamObserver<TurboProto.GetQueryConcurrencyResponse> responseObserver)
    {
        int mppConcurrency = QueryScheduleQueues.Instance().getMppConcurrency();
        int cfConcurrency = QueryScheduleQueues.Instance().getCfConcurrency();
        TurboProto.GetQueryConcurrencyResponse response = TurboProto.GetQueryConcurrencyResponse.newBuilder()
                .setErrorCode(ErrorCode.SUCCESS).setMppConcurrency(mppConcurrency).setCfConcurrency(cfConcurrency).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
