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

import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.QueryScheduleException;
import io.pixelsdb.pixels.common.exception.QueryServerException;
import io.pixelsdb.pixels.common.server.ExecutionHint;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetResultResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.common.turbo.QueryScheduleService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-06-01
 */
public class QueryManager
{
    private static final Logger log = LogManager.getLogger(QueryManager.class);
    private static final QueryManager instance;

    static
    {
        instance = new QueryManager();
    }

    protected static QueryManager Instance()
    {
        return instance;
    }

    private static class ReceivedQuery
    {
        private final String traceToken;
        private final SubmitQueryRequest request;

        public ReceivedQuery(String traceToken, SubmitQueryRequest request)
        {
            this.traceToken = traceToken;
            this.request = request;
        }

        public String getTraceToken()
        {
            return traceToken;
        }

        public SubmitQueryRequest getRequest()
        {
            return request;
        }
    }

    private final ArrayBlockingQueue<ReceivedQuery> pendingQueue = new ArrayBlockingQueue<>(1024);
    private final ConcurrentHashMap<String, ReceivedQuery> runningQueries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, GetResultResponse> queryResults = new ConcurrentHashMap<>();
    private final ExecutorService submitService = Executors.newSingleThreadExecutor();
    private final ExecutorService executeService = Executors.newCachedThreadPool();
    private final QueryScheduleService queryScheduleService;
    private final String jdbcUrl;
    private final Properties costEffectiveConnProp;
    private final Properties immediateConnProp;
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

        this.jdbcUrl = ConfigFactory.Instance().getProperty("presto.pixels.jdbc.url");
        boolean orderEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.order.layout.enabled"));
        boolean compactEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.compact.layout.enabled"));
        this.costEffectiveConnProp = new Properties();
        this.costEffectiveConnProp.setProperty("user", ConfigFactory.Instance().getProperty("presto.user"));
        this.costEffectiveConnProp.setProperty("SSL", ConfigFactory.Instance().getProperty("presto.ssl"));
        String sessionPropertiesBase = "pixels.ordered_path_enabled:" + orderEnabled + ";" +
                "pixels.compact_path_enabled:" + compactEnabled + ";";
        this.costEffectiveConnProp.setProperty("sessionProperties", sessionPropertiesBase + "pixels.cloud_function_enabled:false");

        this.immediateConnProp = new Properties();
        this.immediateConnProp.setProperty("user", ConfigFactory.Instance().getProperty("presto.user"));
        this.immediateConnProp.setProperty("SSL", ConfigFactory.Instance().getProperty("presto.ssl"));
        this.immediateConnProp.setProperty("sessionProperties", sessionPropertiesBase + "pixels.cloud_function_enabled:true");

        this.running = true;
        this.submitService.submit(() -> {
            while (running)
            {
                try
                {
                    ReceivedQuery query = pendingQueue.poll(1000, TimeUnit.MILLISECONDS);
                    if (query != null)
                    {
                        // pending queue should only contain cost-effective queries.
                        checkArgument(query.getRequest().getExecutionHint() == ExecutionHint.COST_EFFECTIVE,
                                "pending queue should only contain cost-effective queries");
                        QueryScheduleService.QuerySlots querySlots = queryScheduleService.getQuerySlots();
                        if (querySlots.MppSlots > 0)
                        {
                            submit(query);
                        }
                        else
                        {
                            // no available slots, put the request back to the pending queue
                            pendingQueue.put(query);
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                } catch (InterruptedException | QueryScheduleException e)
                {
                    log.error("failed to submit query", e);
                    throw new QueryServerException("failed to submit query", e);
                }
            }
        });
        this.submitService.shutdown();
    }

    /**
     * Add the request into the pending queue. The request is going to be submitted later.
     * @param request the query submit request
     * @return the trace token
     * @throws QueryServerException
     */
    public SubmitQueryResponse submitQuery(SubmitQueryRequest request) throws QueryServerException
    {
        if (request.getExecutionHint() == ExecutionHint.COST_EFFECTIVE)
        {
            try
            {
                String traceToken = UUID.randomUUID().toString();
                this.pendingQueue.put(new ReceivedQuery(traceToken, request));
                return new SubmitQueryResponse(ErrorCode.SUCCESS, "", traceToken);
            } catch (InterruptedException e)
            {
                return new SubmitQueryResponse(ErrorCode.QUERY_SERVER_PENDING_INTERRUPTED,
                        "failed to add query to the pending queue", null);
            }
        }
        else if (request.getExecutionHint() == ExecutionHint.IMMEDIATE)
        {
            try
            {
                String traceToken = UUID.randomUUID().toString();
                this.submit(new ReceivedQuery(traceToken, request));
                return new SubmitQueryResponse(ErrorCode.SUCCESS, "", traceToken);
            } catch (Throwable e)
            {
                return new SubmitQueryResponse(ErrorCode.QUERY_SERVER_EXECUTE_FAILED, e.getMessage(), null);
            }
        }
        else
        {
            return new SubmitQueryResponse(ErrorCode.QUERY_SERVER_EXECUTE_FAILED,
                    "unknown query execution hint " + request.getExecutionHint(), null);
        }
    }

    /**
     * Immediately submit the request and add the submitted query into running queue.
     * @param query the query to submit
     */
    private void submit(ReceivedQuery query)
    {
        Properties properties;
        SubmitQueryRequest request = query.getRequest();
        if (request.getExecutionHint() == ExecutionHint.COST_EFFECTIVE)
        {
            // submit it to the mpp connection
            properties = this.costEffectiveConnProp;
        }
        else if (request.getExecutionHint() == ExecutionHint.IMMEDIATE)
        {
            // submit it to the pixels-turbo connection
            properties = this.immediateConnProp;
        }
        else
        {
            throw new QueryServerException("unknown query execution hint " + request.getExecutionHint());
        }

        String traceToken = query.getTraceToken();
        this.executeService.submit(() -> {
            properties.setProperty("traceToken", traceToken);
            try (Connection connection = DriverManager.getConnection(this.jdbcUrl, properties))
            {
                Statement statement = connection.createStatement();
                this.runningQueries.put(traceToken, query);
                long start = System.currentTimeMillis();
                ResultSet resultSet = statement.executeQuery(request.getQuery());
                long latencyMs = System.currentTimeMillis() - start;
                int columnCount = resultSet.getMetaData().getColumnCount();
                int[] columnPrintSizes = new int[columnCount];
                String[] columnNames = new String[columnCount];
                for (int i = 1; i <= columnCount; ++i)
                {
                    columnPrintSizes[i-1] = resultSet.getMetaData().getColumnDisplaySize(i);
                    columnNames[i-1] = resultSet.getMetaData().getColumnLabel(i);
                }
                String[][] rows = new String[request.getLimitRows()][];
                for (int i = 0; i < request.getLimitRows() && resultSet.next(); ++i)
                {
                    String[] row = new String[columnCount];
                    for (int j = 1; j <= columnCount; ++j)
                    {
                        row[j-1] = resultSet.getString(j);
                    }
                    rows[i] = row;
                }

                // TODO: support get cost from trans service.
                GetResultResponse result = new GetResultResponse(ErrorCode.SUCCESS, "",
                        columnPrintSizes, columnNames, rows, latencyMs, 0);
                this.runningQueries.remove(traceToken);
                this.queryResults.put(traceToken, result);
            } catch (SQLException e)
            {
                GetResultResponse result = new GetResultResponse(ErrorCode.QUERY_SERVER_EXECUTE_FAILED, e.getMessage(),
                        null, null, null, 0, 0);
                this.runningQueries.remove(traceToken);
                this.queryResults.put(traceToken, result);
                log.error("failed to execute query with trac token " + traceToken, e);
                throw new QueryServerException("failed to execute query with trac token " + traceToken, e);
            }
        });
    }

    public void shutdown()
    {
        this.running = false;
        this.submitService.shutdownNow();
        this.executeService.shutdownNow();
    }

    public int getNumPendingQueries()
    {
        return this.pendingQueue.size();
    }

    public int getNumRunningQueries()
    {
        return this.runningQueries.size();
    }

    public boolean isQueryRunning(String traceId)
    {
        return this.runningQueries.containsKey(traceId);
    }

    public boolean isQueryFinished(String traceId)
    {
        return this.queryResults.containsKey(traceId);
    }

    public GetResultResponse popQueryResult(String traceId)
    {
        return this.queryResults.remove(traceId);
    }
}
