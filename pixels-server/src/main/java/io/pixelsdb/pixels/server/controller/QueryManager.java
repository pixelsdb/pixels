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
package io.pixelsdb.pixels.server.controller;

import io.pixelsdb.pixels.common.exception.QueryScheduleException;
import io.pixelsdb.pixels.common.exception.QueryServerException;
import io.pixelsdb.pixels.common.server.ExecutionHint;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetResultResponse;
import io.pixelsdb.pixels.common.turbo.ExecutorType;
import io.pixelsdb.pixels.common.turbo.QueryScheduleService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-06-01
 */
public class QueryManager
{
    private static final QueryManager instance;

    static
    {
        instance = new QueryManager();
    }

    protected static QueryManager Instance()
    {
        return instance;
    }

    public static class RunningQuery
    {
        private final long transId;
        private final String query;
        private final ExecutorType executorType;
        private final int limit;
        private final String callbackToken;

        public RunningQuery(long transId, String query, ExecutorType executorType, int limit, String callbackToken)
        {
            this.transId = transId;
            this.query = query;
            this.executorType = executorType;
            this.limit = limit;
            this.callbackToken = callbackToken;
        }

        public long getTransId()
        {
            return transId;
        }

        public String getQuery()
        {
            return query;
        }

        public ExecutorType getExecutorType()
        {
            return executorType;
        }

        public int getLimit()
        {
            return limit;
        }

        public String getCallbackToken()
        {
            return callbackToken;
        }
    }

    private final ArrayBlockingQueue<SubmitQueryRequest> pendingQueue = new ArrayBlockingQueue<>(1024);
    private final ConcurrentHashMap<String, RunningQuery> runningQueries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, GetResultResponse> queryResults = new ConcurrentHashMap<>();
    private final ExecutorService submitService = Executors.newSingleThreadExecutor();
    private final ExecutorService executeService = Executors.newCachedThreadPool();
    private final QueryScheduleService queryScheduleService;
    private boolean running;

    private QueryManager() throws QueryServerException
    {
        String host = ConfigFactory.Instance().getProperty("query.schedule.server.host");
        int port = Integer.parseInt(ConfigFactory.Instance().getProperty("query.schedule.server.port"));
        try
        {
            this.queryScheduleService = new QueryScheduleService(host, port);
        } catch (QueryScheduleException e)
        {
            throw new QueryServerException("failed to initialize query schedule service", e);
        }
        this.running = true;
        this.submitService.submit(() -> {
            while (running)
            {
                try
                {
                    SubmitQueryRequest request = pendingQueue.poll(1000, TimeUnit.MILLISECONDS);
                    if (request != null)
                    {
                        // only
                        checkArgument(request.getExecutionHint() == ExecutionHint.COST_EFFECTIVE,
                                "pending queue should only contain cost-effective queries");
                        QueryScheduleService.QuerySlots querySlots = queryScheduleService.getQuerySlots();
                        if (querySlots.MppSlots > 0)
                        {
                            submit(request);
                        }
                        else
                        {
                            // no available slots, put the request back to the pending queue
                            pendingQueue.put(request);
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                } catch (InterruptedException | QueryScheduleException e)
                {
                    throw new QueryServerException("failed to submit query", e);
                }
            }
        });
        this.submitService.shutdown();
    }

    /**
     * Add the request into the pending queue. The request is going to be submitted later.
     * @param request the query submit request
     * @throws QueryServerException
     */
    public void pending(SubmitQueryRequest request) throws QueryServerException
    {
        try
        {
            this.pendingQueue.put(request);
        } catch (InterruptedException e)
        {
            throw new QueryServerException("failed to add query to the pending queue", e);
        }
    }

    /**
     * Immediately submit the request and add the submitted query into running queue.
     * @param request
     */
    public void submit(SubmitQueryRequest request)
    {
        if (request.getExecutionHint() == ExecutionHint.COST_EFFECTIVE)
        {
            // submit it to the mpp connection
        }
        else if (request.getExecutionHint() == ExecutionHint.IMMEDIATE)
        {
            // submit it to the pixels-turbo connection
        }
        else
        {
            throw new QueryServerException("unknown query execution hint " + request.getExecutionHint());
        }
    }

    public void shutdown()
    {
        this.running = false;
        this.submitService.shutdownNow();
    }
}
