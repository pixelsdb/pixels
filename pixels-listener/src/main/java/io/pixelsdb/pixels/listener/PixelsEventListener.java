/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.listener;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.common.utils.HttpUtil;
import io.pixelsdb.pixels.listener.exception.ListenerExecption;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import io.airlift.log.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static io.pixelsdb.pixels.listener.exception.ListenerErrorCode.PIXELS_EVENT_LISTENER_ERROR;
import static io.pixelsdb.pixels.listener.exception.ListenerErrorCode.PIXELS_EVENT_LISTENER_METRIC_ERROR;

/**
 * Created at: 18-12-8
 * Author: hank
 */
public class PixelsEventListener implements EventListener
{
    private static Logger logger = Logger.get(PixelsEventListener.class);

    private final String logDir;
    private final boolean enabled;
    private final String userPrefix;
    private final String schema;
    private final String queryType;
    private static BufferedWriter LogWriter = null;
    private static final double GCThreshold;

    static
    {
        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
        /**
         * Issue #87:
         * Get the gc threshold here.
         */
        String thresholdStr = ConfigFactory.Instance().getProperty("pixels.gc.threshold");
        GCThreshold = Double.parseDouble(thresholdStr);
        logger.info("Using pixels.gc.threshold (" + GCThreshold + ")...");
    }

    public PixelsEventListener (String logDir, boolean enabled,
                                String userPrefix,
                                String schema,
                                String queryType)
    {
        this.logDir = logDir.endsWith("/") ? logDir : logDir + "/";
        this.enabled = enabled;
        this.userPrefix = userPrefix;
        this.schema = schema;
        this.queryType = queryType;
        try
        {
            if (this.enabled == true && LogWriter == null)
            {
                LogWriter = new BufferedWriter(new FileWriter(
                                this.logDir + "pixels_query_" +
                                        DateUtil.getCurTime() + ".log", true));
                LogWriter.write("\"query id\",\"user\",\"elapsed (ms)\",\"execution (ms)\",\"read throughput (MB)\",gc time (ms)");
                LogWriter.newLine();
                LogWriter.flush();
            }
        } catch (IOException e)
        {
            throw new PrestoException(PIXELS_EVENT_LISTENER_ERROR,
                    new ListenerExecption("can not create log writer."));
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (this.enabled == false)
        {
            return;
        }

        double free = Runtime.getRuntime().freeMemory();
        double total = Runtime.getRuntime().totalMemory();

        /**
         * Issue #87:
         * Do explicit gc here, instead of in PixelsReaderImpl.close().
         */
        long gcms = -1;
        if (free / total < GCThreshold)
        {
            /**
             * By calling gc(), we try to do gc on time when the query is finished.
             * It would be very expensive to do gc when executing small queries.
             */
            long start = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            gcms = (System.currentTimeMillis() - start);
            logger.info("GC time after query: " + gcms + " ms.");
        }

        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        String user = queryCompletedEvent.getContext().getUser();
        String schema = queryCompletedEvent.getContext().getSchema().get();
        String uri = queryCompletedEvent.getMetadata().getUri().toString();
        if (schema.equalsIgnoreCase(this.schema))
        {
            if (this.userPrefix.equals("none") ||
                    (!this.userPrefix.equals("none") && user.startsWith(this.userPrefix)))
            {
                try
                {
                    String content = HttpUtil.GetContentByGet(uri.toString());
                    JSONObject object = JSONObject.parseObject(content);
                    String query = object.getString("query");
                    if (query.toLowerCase().contains(this.queryType.toLowerCase()))
                    {
                        JSONObject statsObject = object.getJSONObject("queryStats");
                        double elapsed = this.parseElapsedToMillis(statsObject.getString("elapsedTime"));
                        double queued = this.parseElapsedToMillis(statsObject.getString("queuedTime"));
                        double analysis = this.parseElapsedToMillis(statsObject.getString("analysisTime"));
                        double planning = this.parseElapsedToMillis(statsObject.getString("totalPlanningTime"));
                        double finishing = this.parseElapsedToMillis(statsObject.getString("finishingTime"));
                        double inputDataSize = this.parseDataSizeToMB(statsObject.getString("rawInputDataSize"));
                        if (elapsed < 0 || queued < 0 || analysis < 0 ||
                                planning < 0 || finishing < 0 || inputDataSize < 0)
                        {
                            throw new ListenerExecption("elapsedTime:" + statsObject.getString("elapsedTime") +
                                    ",queuedTime:" + statsObject.getString("queuedTime") +
                                    ",analysisTime:" + statsObject.getString("analysisTime") +
                                    ",totalPlanningTime:" + statsObject.getString("totalPlanningTime") +
                                    ",finishingTime:" + statsObject.getString("finishingTime") +
                                    ",rawInputDataSize:" + statsObject.getString("rawInputDataSize")
                            );
                        }
                        double execution = elapsed - queued - analysis - planning - finishing;
                        double throughput = inputDataSize / execution * 1000;
                        LogWriter.write(queryId + "," + user + "," + elapsed + "," + execution + "," + throughput + "," + (gcms>=0 ? gcms : "na"));
                        LogWriter.newLine();
                        LogWriter.flush();
                    }
                } catch (IOException e)
                {
                    logger.error("can not write log in pixels event listener.");
                    logger.info("query id: " + queryId + ", user: " + user + ", uri: " + uri.toString());
                } catch (ListenerExecption e)
                {
                    logger.error("can not parse metrics in presto json.");
                    logger.info("query id: " + queryId + ", user: " + user + ", uri: " + uri.toString());
                    throw new PrestoException(PIXELS_EVENT_LISTENER_METRIC_ERROR, e);
                }
            }
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }

    /**
     * parse elapsed milli seconds from string.
     * return -1 if the string can not be parsed.
     * @param str
     * @return
     */
    private double parseElapsedToMillis (String str)
    {
        if (str == null)
        {
            return 0;
        }

        if (str.endsWith("ns"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("ns"))) / 1000 / 1000;
        }
        else if (str.endsWith("us"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("us"))) / 1000;
        }
        else if (str.endsWith("ms"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("ms")));
        }
        else if (str.endsWith("s"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("s"))) * 1000;
        }
        else if (str.endsWith("m"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("m"))) * 60 * 1000;
        }
        else if (str.endsWith("h"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("h"))) * 60 * 60 * 1000;
        }
        else
        {
            return -1;
        }
    }

    /**
     * parse the megabytes from string.
     * return -1 if the string can not be parsed.
     * @param str
     * @return
     */
    private double parseDataSizeToMB (String str)
    {
        if (str == null)
        {
            return 0;
        }

        if (str.endsWith("KB"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("KB"))) / 1024;
        }
        else if (str.endsWith("MB"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("MB")));
        }
        else if (str.endsWith("GB"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("GB"))) * 1024;
        }
        else if (str.endsWith("TB"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("TB"))) * 1024 * 1024;
        }
        else if (str.endsWith("B"))
        {
            return Double.parseDouble(str.substring(0, str.indexOf("B"))) / 1024 / 1024;
        }
        else
        {
            return -1;
        }
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        LogWriter.close();
    }
}
