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
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.server.ExecutionHint;
import io.pixelsdb.pixels.common.server.PriceModel;
import io.pixelsdb.pixels.common.server.QueryStatus;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryResultResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.turbo.QueryScheduleService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.Constants.RELAXED_EXECUTION_MAX_POSTPONE_SEC;
import static io.pixelsdb.pixels.common.utils.Constants.RELAXED_EXECUTION_RETRY_INTERVAL_SEC;
import static io.pixelsdb.pixels.server.constant.ControllerParameters.GET_QUERY_COSTS_DELAY_MS;
import static io.pixelsdb.pixels.server.constant.ControllerParameters.QUERY_RESULT_CLEAR_DELAY_MS;

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

        private final long receivedTimeMs;

        public ReceivedQuery(String traceToken, SubmitQueryRequest request, long receivedTimeMs)
        {
            this.traceToken = traceToken;
            this.request = request;
            this.receivedTimeMs = receivedTimeMs;
        }

        public String getTraceToken()
        {
            return traceToken;
        }

        public SubmitQueryRequest getRequest()
        {
            return request;
        }

        public long getReceivedTimeMs()
        {
            return receivedTimeMs;
        }
    }

    private final LinkedBlockingQueue<ReceivedQuery> pendingQueueRe = new LinkedBlockingQueue<>();
    private final ConcurrentLinkedQueue<ReceivedQuery> pendingQueueRe2nd = new ConcurrentLinkedQueue<>();
    private final LinkedBlockingQueue<ReceivedQuery> pendingQueueBe = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, ReceivedQuery> runningQueries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, GetQueryResultResponse> queryResults = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> finishedQueries = new ConcurrentHashMap<>();
    private final ExecutorService relaxedSubmitService = Executors.newSingleThreadExecutor();
    private final ExecutorService relaxedRetryService = Executors.newSingleThreadExecutor();
    private final ExecutorService bestEffortSubmitService = Executors.newSingleThreadExecutor();
    private final ExecutorService queryResultClearService = Executors.newSingleThreadExecutor();
    private final ExecutorService executeService = Executors.newCachedThreadPool();
    private final QueryScheduleService queryScheduleService;
    private final TransService transService;
    private final String jdbcUrl;
    private final Properties costEffectiveConnProp;
    private final Properties immediateConnProp;
    private boolean running;

    private QueryManager() throws QueryServerException
    {
        String scheduleServerHost = ConfigFactory.Instance().getProperty("query.schedule.server.host");
        int scheduleServerPort = Integer.parseInt(ConfigFactory.Instance().getProperty("query.schedule.server.port"));
        String transServerHost = ConfigFactory.Instance().getProperty("trans.server.host");
        int transServerPort = Integer.parseInt(ConfigFactory.Instance().getProperty("trans.server.port"));
        try
        {
            /*
             * Issue #490:
             * Auto scaling the MPP cluster is done by the query engine backend (e.g., Trino).
             * Here, we only need to get the query slots from the query schedule service and do not need to report
             * metrics for cluster auto-scaling, so we set scalingEnabled to false.
             */
            this.queryScheduleService = new QueryScheduleService(scheduleServerHost, scheduleServerPort, false);
            this.transService = new TransService(transServerHost, transServerPort);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                queryScheduleService.shutdown();
                try
                {
                    transService.shutdown();
                } catch (InterruptedException e)
                {
                    log.error("failed to shutdown query schedule service or transaction service", e);
                }
            }));
        } catch (QueryScheduleException e)
        {
            throw new QueryServerException("failed to initialize query schedule service", e);
        }

        this.jdbcUrl = ConfigFactory.Instance().getProperty("presto.jdbc.url");
        boolean orderEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.ordered.layout.enabled"));
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
        this.relaxedSubmitService.submit(() -> {
            while (running)
            {
                try
                {
                    ReceivedQuery query = pendingQueueRe.poll(60, TimeUnit.SECONDS);
                    if (query != null)
                    {
                        // this queue should only contain relaxed queries that are to be executed in the mpp cluster
                        checkArgument(query.getRequest().getExecutionHint() == ExecutionHint.RELAXED,
                                "pending queue should only contain cost-effective queries");
                        QueryScheduleService.QuerySlots querySlots = queryScheduleService.getQuerySlots();
                        if (querySlots.mppSlots > 0)
                        {
                            submit(query);
                        }
                        else
                        {
                            // no available slots, put the request to the secondary pending queue
                            pendingQueueRe2nd.add(query);
                        }
                    }
                } catch (InterruptedException | QueryScheduleException e)
                {
                    log.error("failed to submit relaxed query", e);
                    throw new QueryServerException("failed to submit relaxed query", e);
                }
            }
        });
        this.relaxedSubmitService.shutdown();

        this.relaxedRetryService.submit(() -> {
            while (running)
            {
                try
                {
                    TimeUnit.SECONDS.sleep(RELAXED_EXECUTION_RETRY_INTERVAL_SEC);
                    for (Iterator<ReceivedQuery> it = pendingQueueRe2nd.iterator(); it.hasNext(); )
                    {
                        ReceivedQuery query = it.next();
                        if ((System.currentTimeMillis() - query.receivedTimeMs) / 1000 >
                                RELAXED_EXECUTION_MAX_POSTPONE_SEC - RELAXED_EXECUTION_RETRY_INTERVAL_SEC)
                        {
                            // the query will exceed the max postpone time in the next retry
                            submit(query);
                        }
                        else
                        {
                            // give the query a chance to retry
                            pendingQueueRe.put(query);
                        }
                        it.remove();
                    }
                } catch (InterruptedException e)
                {
                    log.error("failed to retry submit relaxed query", e);
                    throw new QueryServerException("failed to retry submit relaxed query", e);
                }
            }
        });
        this.relaxedRetryService.shutdown();

        this.bestEffortSubmitService.submit(() -> {
            while (running)
            {
                try
                {
                    ReceivedQuery query = pendingQueueBe.poll(60, TimeUnit.SECONDS);
                    if (query != null)
                    {
                        // this queue should only contain best-effort queries that are to be executed in the mpp cluster
                        checkArgument(query.getRequest().getExecutionHint() == ExecutionHint.BEST_OF_EFFORT,
                                "pending queue should only contain cost-effective queries");
                        QueryScheduleService.QueryConcurrency queryConcurrency = queryScheduleService.getQueryConcurrency();
                        if (queryConcurrency.mppConcurrency == 0)
                        {
                            // submit the query if there is no other query running in the mpp cluster
                            submit(query);
                        }
                        else
                        {
                            // put the query back to the best-effort pending queue if other queries are running in the mpp cluster
                            pendingQueueBe.put(query);
                            TimeUnit.SECONDS.sleep(1);
                        }
                    }
                } catch (InterruptedException | QueryScheduleException e)
                {
                    log.error("failed to submit best-effort query", e);
                    throw new QueryServerException("failed to submit best-effort query", e);
                }
            }
        });
        this.bestEffortSubmitService.shutdown();

        this.queryResultClearService.submit(() -> {
            while (running)
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(QUERY_RESULT_CLEAR_DELAY_MS);
                    for (Map.Entry<String, GetQueryResultResponse> entry : this.queryResults.entrySet())
                    {
                        long current = System.currentTimeMillis();
                        String traceToken = entry.getKey();
                        GetQueryResultResponse response = entry.getValue();
                        if (current - response.getFinishTimestampMs() >= QUERY_RESULT_CLEAR_DELAY_MS)
                        {
                            // put it into finished query before removing from query results, to avoid unknown query status
                            this.finishedQueries.put(traceToken, traceToken);
                            this.queryResults.remove(traceToken);
                        }
                    }
                } catch (InterruptedException e)
                {
                    log.error("failed to clear query result", e);
                    throw new QueryServerException("failed to clear query result", e);
                }
            }
        });
        this.queryResultClearService.shutdown();
    }

    /**
     * Add the request into the pending queue. The request is going to be submitted later.
     * @param request the query submit request
     * @return the trace token
     * @throws QueryServerException
     */
    public SubmitQueryResponse submitQuery(SubmitQueryRequest request)
    {
        if (request.getExecutionHint() == ExecutionHint.RELAXED || request.getExecutionHint() == ExecutionHint.BEST_OF_EFFORT)
        {
            try
            {
                String traceToken = UUID.randomUUID().toString();
                if (request.getExecutionHint() == ExecutionHint.RELAXED)
                {
                    this.pendingQueueRe.put(new ReceivedQuery(traceToken, request, System.currentTimeMillis()));
                }
                else
                {
                    this.pendingQueueBe.put(new ReceivedQuery(traceToken, request, System.currentTimeMillis()));
                }
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
                this.submit(new ReceivedQuery(traceToken, request, System.currentTimeMillis()));
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
        if (request.getExecutionHint() == ExecutionHint.RELAXED || request.getExecutionHint() == ExecutionHint.BEST_OF_EFFORT)
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
                long pendingTimeMs = System.currentTimeMillis() - query.getReceivedTimeMs();
                long start = System.currentTimeMillis();
                ResultSet resultSet = statement.executeQuery(request.getQuery());
                long finishTimestampMs = System.currentTimeMillis();
                long executeTimeMs = finishTimestampMs - start;

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

                resultSet.close();
                statement.close();

                GetQueryResultResponse result = new GetQueryResultResponse(ErrorCode.SUCCESS, "",
                        request.getExecutionHint(), columnPrintSizes, columnNames, rows, pendingTimeMs,
                        executeTimeMs, finishTimestampMs);
                // put result before removing from running queries, to avoid unknown query status
                this.queryResults.put(traceToken, result);
                this.runningQueries.remove(traceToken);
            } catch (SQLException e)
            {
                GetQueryResultResponse result = new GetQueryResultResponse(
                        ErrorCode.QUERY_SERVER_EXECUTE_FAILED, e.getMessage());
                // put result before removing from running queries, to avoid unknown query status
                this.queryResults.put(traceToken, result);
                this.runningQueries.remove(traceToken);
                log.error("failed to execute query with trace token " + traceToken, e);
                throw new QueryServerException("failed to execute query with trace token " + traceToken, e);
            }
        });
    }

    public void shutdown()
    {
        this.running = false;
        this.relaxedSubmitService.shutdownNow();
        this.relaxedRetryService.shutdownNow();
        this.bestEffortSubmitService.shutdownNow();
        this.queryResultClearService.shutdownNow();
        this.executeService.shutdownNow();
    }

    public int getNumPendingQueries()
    {
        return this.pendingQueueRe.size();
    }

    public int getNumRunningQueries()
    {
        return this.runningQueries.size();
    }

    public QueryStatus getQueryStatus(String traceToken)
    {
        if (this.queryResults.containsKey(traceToken) || this.finishedQueries.containsKey(traceToken))
        {
            return QueryStatus.FINISHED;
        }
        if (this.runningQueries.containsKey(traceToken))
        {
            return QueryStatus.RUNNING;
        }
        return QueryStatus.PENDING;
    }

    /**
     * Get the query result of a query with the trace token.
     * @param traceToken the trace token of the query
     * @return null if the query result is not found
     */
    public synchronized GetQueryResultResponse popQueryResult(String traceToken)
    {
        GetQueryResultResponse response = this.queryResults.get(traceToken);

        if (response != null)
        {
            // Issue #649: get the costs from the transaction service
            while (!response.hasValidCents())
            {
                try
                {
                    TransContext transContext = this.transService.getTransContext(traceToken);
                    double vmCostCents;
                    // wait until get vmCostCents
                    if ((vmCostCents = Double.parseDouble(transContext.getProperties().getProperty(
                            Constants.TRANS_CONTEXT_VM_COST_CENTS_KEY, "-1"))) < 0)
                    {
                        // to our experience, 500ms is enough for transaction service to collect the cost metrics
                        TimeUnit.MILLISECONDS.sleep(GET_QUERY_COSTS_DELAY_MS);
                        continue;
                    }
                    response.setCostCents(vmCostCents);
                    double cfCostCents = Double.parseDouble(transContext.getProperties().getProperty(
                            Constants.TRANS_CONTEXT_CF_COST_CENTS_KEY));
                    if (cfCostCents < 0)
                    {
                        throw new TransException("the trans context returned by transaction service has an invalid cf cost");
                    }
                    response.addCostCents(cfCostCents);
                    double scanBytes = Double.parseDouble(transContext.getProperties().getProperty(
                            Constants.TRANS_CONTEXT_SCAN_BYTES_KEY));
                    if (scanBytes < 0)
                    {
                        throw new TransException("the trans context returned by transaction service has an invalid scan bytes");
                    }
                    double billedCents = PriceModel.billedCents(scanBytes, response.getExecutionHint());
                    response.setBilledCents(billedCents);
                } catch (TransException e)
                {
                    log.error("failed to get trans context with trace token " + traceToken, e);
                    throw new QueryServerException("failed to get trans context with trace token " + traceToken, e);
                } catch (InterruptedException e)
                {
                    log.error("failed to sleep before getting vm costs", e);
                    throw new QueryServerException("failed to sleep before getting vm costs", e);
                }
            }
        }
        return response;
    }
}