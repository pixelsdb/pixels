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
import io.pixelsdb.pixels.turbo.QueryScheduleServiceGrpc;

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
        public int MppSlots = 0;
        public int CfSlots = 0;
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

    public ExecutorType scheduleQuery(long transId, boolean forceMpp)
    {
        return null;
    }

    public boolean finishQuery(long transId, ExecutorType executorType)
    {
        return false;
    }

    public QuerySlots getQuerySlots()
    {
        return null;
    }
}
