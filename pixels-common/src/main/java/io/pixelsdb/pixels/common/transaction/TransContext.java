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
package io.pixelsdb.pixels.common.transaction;

import io.pixelsdb.pixels.common.metrics.CloudWatchCountMetrics;
import io.pixelsdb.pixels.common.metrics.NamedCount;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * <P>
 *     The context of the transactions in Pixels.
 * </P>
 *
 * Created at: 20/02/2022
 * Author: hank
 */
public class TransContext
{
    private static TransContext instance;

    public static TransContext Instance()
    {
        if (instance == null)
        {
            instance = new TransContext();
        }
        return instance;
    }

    private final Map<Long, QueryTransInfo> queryTransContext;
    private final CloudWatchCountMetrics cloudWatchCountMetrics;
    private final String metricsName;
    private final ScheduledExecutorService metricsReporter;
    private int prevConcurrency = 0;

    private TransContext()
    {
        this.queryTransContext = new ConcurrentHashMap<>();
        this.cloudWatchCountMetrics = new CloudWatchCountMetrics();
        this.metricsName = ConfigFactory.Instance().getProperty("query.concurrency.metrics.name");
        this.metricsReporter = Executors.newScheduledThreadPool(1);
        int period = Integer.parseInt(ConfigFactory.Instance().getProperty("query.concurrency.report.period.sec"));
        this.metricsReporter.scheduleAtFixedRate(() -> {
            NamedCount count = new NamedCount(metricsName, queryTransContext.size());
            cloudWatchCountMetrics.putCount(count);
        }, 0, period, TimeUnit.SECONDS);
    }

    public synchronized void beginQuery(QueryTransInfo info)
    {
        requireNonNull(info, "query transaction info is null");
        checkArgument(info.getQueryStatus() == QueryTransInfo.Status.PENDING);
        this.queryTransContext.put(info.getQueryId(), info);
        int concurrency = this.queryTransContext.size();
        if (concurrency != prevConcurrency)
        {
            prevConcurrency = concurrency;
            NamedCount count = new NamedCount(this.metricsName, concurrency);
            this.cloudWatchCountMetrics.putCount(count);
        }
    }

    public synchronized void commitQuery(long queryId)
    {
        QueryTransInfo info = this.queryTransContext.remove(queryId);
        if (info != null)
        {
            info.setQueryStatus(QueryTransInfo.Status.COMMIT);
        }
    }

    public synchronized void rollbackQuery(long queryId)
    {
        QueryTransInfo info = this.queryTransContext.remove(queryId);
        if (info != null)
        {
            info.setQueryStatus(QueryTransInfo.Status.ROLLBACK);
        }
    }

    public synchronized QueryTransInfo getQueryTransInfo(long queryId)
    {
        return this.queryTransContext.get(queryId);
    }

    public synchronized boolean isTerminated(long queryId)
    {
        QueryTransInfo info =  this.queryTransContext.get(queryId);
        return info == null || info.getQueryStatus() == QueryTransInfo.Status.COMMIT ||
                info.getQueryStatus() == QueryTransInfo.Status.ROLLBACK;
    }

    /**
     * @return the number of concurrent queries at this moment.
     */
    public synchronized int getTransConcurrency()
    {
        return this.queryTransContext.size();
    }
}
