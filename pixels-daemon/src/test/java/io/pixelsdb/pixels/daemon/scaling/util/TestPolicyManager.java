/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.daemon.scaling.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.scaling.MetricsQueue;
import org.junit.Test;

import java.beans.Transient;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;
import java.util.concurrent.TimeUnit;

public class TestPolicyManager
{
    public static class Query
    {
        private  long ts;
        private  String sql;
        public long getTs() { return ts; }
        public String getSql() { return sql; }
        public Query() {}
        public Query(long ts, String sql) { this.ts = ts; this.sql = sql; }
    }

    @Test
    public void AIASTest() {
        String jdbcUrl = ConfigFactory.Instance().getProperty("presto.jdbc.url") + "/tpch";
        boolean orderEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.ordered.layout.enabled"));
        boolean compactEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.compact.layout.enabled"));
        Properties costEffectiveConnProp = new Properties();
        costEffectiveConnProp.setProperty("user", ConfigFactory.Instance().getProperty("presto.user"));
        costEffectiveConnProp.setProperty("SSL", ConfigFactory.Instance().getProperty("presto.ssl"));
        String sessionPropertiesBase = "pixels.ordered_path_enabled:" + orderEnabled + ";" +
                "pixels.compact_path_enabled:" + compactEnabled + ";";
        costEffectiveConnProp.setProperty("sessionProperties", sessionPropertiesBase + "pixels.cloud_function_enabled:false");

        ExecutorService executeService = Executors.newCachedThreadPool();
        PolicyManager policyManager = new PolicyManager();
        policyManager.doAutoScaling();
        ObjectMapper objectMapper = new ObjectMapper();
        String dir = "/home/ubuntu/dev/pixels/pixels-daemon/src/test/java/io/pixelsdb/pixels/daemon/scaling/policy/helper/";
        File file = new File(dir + "queries.json");
        TypeReference<List<Query>> typeReference = new TypeReference<List<Query>>() {};
        List<Query> Queries;
        try {
            Queries = objectMapper.readValue(file, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long st = System.currentTimeMillis();
        long st2 = st;
        int cnt = 0;
        for (Query query : Queries) {
            if (System.currentTimeMillis()-st >= TimeUnit.MINUTES.toMillis(5))
            {
                policyManager.doAutoScaling();
                st = System.currentTimeMillis();
            }
            Properties properties = new Properties(costEffectiveConnProp);
            String traceToken = UUID.randomUUID().toString();
            if (query.getTs() + st2 - System.currentTimeMillis() >= TimeUnit.MINUTES.toMillis(1))
            {
                try {
                    Thread.sleep(query.getTs() + st2 -System.currentTimeMillis()-10*1000);
                    st2 = System.currentTimeMillis();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            long delay = query.getTs() + st - System.currentTimeMillis();
            String sql = query.getSql();
//            System.out.print(sql);
            executeService.submit(() -> {
                try {
                    Thread.sleep(delay > 0 ?delay : 0);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                properties.setProperty("traceToken", traceToken);
                try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql);
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    int[] columnPrintSizes = new int[columnCount];
                    String[] columnNames = new String[columnCount];
                    for (int i = 1; i <= columnCount; ++i)
                    {
                        columnPrintSizes[i-1] = resultSet.getMetaData().getColumnDisplaySize(i);
                        columnNames[i-1] = resultSet.getMetaData().getColumnLabel(i);
                    }
                    String[][] rows = new String[10][];
                    for (int i = 0; i < 10 && resultSet.next(); ++i)
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
                } catch (SQLException e) {
                    System.out.printf("failed to execute query with trace token " + traceToken, e);
                }
            });
        }
        executeService.shutdown();
        boolean alldown = false;
        while(!alldown)
        {
            if (System.currentTimeMillis()-st >= TimeUnit.MINUTES.toMillis(5))
            {
                policyManager.doAutoScaling();
                st = System.currentTimeMillis();
            }
            try {
                alldown = executeService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testDoAutoScalingMethod()
    {
        PolicyManager policyManager = new PolicyManager();
        BlockingDeque<Integer> metricsQueue = MetricsQueue.queue;

        System.out.print("start\n");
        try{
            while( true ){
                policyManager.doAutoScaling();
                Thread.sleep(5*60*1000);
            }
        }catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
//        System.out.print("finish!");
    }

    @Test
    public void testConsumer()
    {
        new Thread(new PolicyManager()).start();
        BlockingDeque<Integer> metricsQueue = MetricsQueue.queue;
        metricsQueue.add(5);
        metricsQueue.add(1);
        try
        {
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
