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
package io.pixelsdb.pixels.common.turbo;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.QueryScheduleException;
import io.pixelsdb.pixels.turbo.QueryScheduleServiceGrpc;
import io.pixelsdb.pixels.turbo.TurboProto;

import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2023-05-31
 */
public class QueryScheduleService
{
    private final ManagedChannel channel;
    private final QueryScheduleServiceGrpc.QueryScheduleServiceBlockingStub stub;

    public static class QuerySlots
    {
        public final int MppSlots;
        public final int CfSlots;

        public QuerySlots(int mppSlots, int cfSlots)
        {
            MppSlots = mppSlots;
            CfSlots = cfSlots;
        }
    }

    public QueryScheduleService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = QueryScheduleServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public ExecutorType scheduleQuery(long transId, boolean forceMpp) throws QueryScheduleException
    {
        TurboProto.ScheduleQueryRequest request = TurboProto.ScheduleQueryRequest.newBuilder()
                .setTransId(transId).setForceMpp(forceMpp).build();
        TurboProto.ScheduleQueryResponse response = this.stub.scheduleQuery(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new QueryScheduleException("failed to schedule query, error code=" + response.getErrorCode());
        }
        return ExecutorType.valueOf(response.getExecutorType());
    }

    public boolean finishQuery(long transId, ExecutorType executorType)
    {
        TurboProto.FinishQueryRequest request = TurboProto.FinishQueryRequest.newBuilder()
                .setTransId(transId).setExecutorType(executorType.name()).build();
        TurboProto.FinishQueryResponse response = this.stub.finishQuery(request);
        return response.getErrorCode() == ErrorCode.SUCCESS;
    }

    public QuerySlots getQuerySlots() throws QueryScheduleException
    {
        TurboProto.GetQuerySlotsRequest request = TurboProto.GetQuerySlotsRequest.newBuilder().build();
        TurboProto.GetQuerySlotsResponse response = this.stub.getQuerySlots(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new QueryScheduleException("failed to get query slots, error code=" + response.getErrorCode());
        }
        return new QuerySlots(response.getMppSlots(), response.getCfSlots());
    }
}
