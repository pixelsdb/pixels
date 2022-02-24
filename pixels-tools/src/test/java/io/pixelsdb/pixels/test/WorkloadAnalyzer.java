/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.test;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * pixels
 *
 * @author guodong
 */
public class WorkloadAnalyzer
{
    private String workloadPath = "/Users/Jelly/Desktop/pixels/cache/Mar17/1187_dedup_query.txt";
    private String workloadColsLog = "/Users/Jelly/Desktop/pixels/cache/workload_cols.csv";
    private String workloadCacheLog = "/Users/Jelly/Desktop/pixels/cache/workload_cache.csv";
    private String workloadCachedQuery = "/Users/Jelly/Desktop/pixels/cache/Mar17/1187_cached_qid.txt";
    private String cachedWorkloadPath = "/Users/Jelly/Desktop/pixels/cache/Mar17/1187_cached_query.txt";

    @Test
    public void processWorkload()
    {
        try
        {
            BufferedReader workloadReader = new BufferedReader(new FileReader(workloadPath));
            BufferedReader workloadCachedQReader = new BufferedReader(new FileReader(workloadCachedQuery));
            BufferedWriter cachedWorkloadWriter = new BufferedWriter(new FileWriter(cachedWorkloadPath));
            String line;
            String qId = "";
            Map<String, String> queries = new HashMap<>();
            while ((line = workloadReader.readLine()) != null)
            {
                if (line.isEmpty())
                {
                    continue;
                }
                if (false == line.startsWith("SELECT"))
                {
                    qId = line;
                }
                else
                {
                    String query = line;
                    queries.put(qId, query);
                }
            }
            while ((line = workloadCachedQReader.readLine()) != null)
            {
                String id = line.split("_")[1];
                cachedWorkloadWriter.write(id);
                cachedWorkloadWriter.newLine();
                cachedWorkloadWriter.write(queries.get(id));
                cachedWorkloadWriter.newLine();
                cachedWorkloadWriter.newLine();
            }
            workloadReader.close();
            workloadCachedQReader.close();
            cachedWorkloadWriter.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Analyze workload file.
     * Report column visit frequency and cache status
     */
    @Test
    public void analyze0()
    {
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(workloadPath));
            Map<String, Integer> colCounts = new HashMap<>();
            Map<String, Integer> colAppearCounts = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (false == line.startsWith("SELECT"))
                {
                    continue;
                }
                String[] lineSplits = line.split("\\s");
                String selection = lineSplits[1];
                String orderBy = lineSplits[6];
                String[] selectionCols = selection.split(",");
                String[] orderCols = orderBy.split(",");
                String[] cols = new String[selectionCols.length + orderCols.length];
                System.arraycopy(selectionCols, 0, cols, 0, selectionCols.length);
                System.arraycopy(orderCols, 0, cols, selectionCols.length, orderCols.length);
                for (String col : cols)
                {
                    col = col.toLowerCase();
                    if (false == colCounts.containsKey(col))
                    {
                        colCounts.put(col, 1);
                    }
                    else
                    {
                        int prevCount = colCounts.get(col);
                        colCounts.put(col, prevCount + 1);
                    }
                }
                Set<String> colsSet = new HashSet<>(Arrays.asList(cols));
                for (String col : colsSet)
                {
                    col = col.toLowerCase();
                    if (false == colAppearCounts.containsKey(col))
                    {
                        colAppearCounts.put(col, 1);
                    }
                    else
                    {
                        int prevCount = colAppearCounts.get(col);
                        colAppearCounts.put(col, prevCount + 1);
                    }
                }
            }
            reader.close();

            MetadataService metadataService = new MetadataService("dbiir01", 18888);
            Layout layout = metadataService.getLayout("pixels", "test_1187", 3);
            Compact compact = layout.getCompactObject();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheColumnlets = compact.getColumnletOrder().subList(0, cacheBorder);
            Set<String> cacheColOrderIdSet = new HashSet<>();
            for (String c : cacheColumnlets)
            {
                cacheColOrderIdSet.add(c.split(":")[1]);
            }
            Order order = layout.getOrderObject();
            List<String> columnOrder = order.getColumnOrder();
            List<String> cachedColumns = new ArrayList<>();
            for (String cid : cacheColOrderIdSet)
            {
                int id = Integer.parseInt(cid);
                cachedColumns.add(columnOrder.get(id));
            }

            // all columns
            List<Column> columns = metadataService.getColumns("pixels", "test_1187");
            Map<String, Double> columnSizes = new HashMap<>();
            for (Column column : columns)
            {
                columnSizes.put(column.getName(), column.getSize());
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(workloadColsLog));
            writer.write("colName,colCount,colAppear,colSize");
            writer.newLine();
            writer.flush();
            for (String col : colCounts.keySet())
            {
                writer.write(col + "," + colCounts.get(col) + "," + colAppearCounts.get(col) + "," + columnSizes.get(col));
                writer.newLine();
            }
            writer.close();

            // cached columns
            writer = new BufferedWriter(new FileWriter(workloadCacheLog));
            writer.write("colName,colCount,colAppear,colSize");
            writer.newLine();
            writer.flush();
            for (String col : cachedColumns)
            {
                writer.write(col + "," + (colCounts.get(col) == null ? 0 : colCounts.get(col)) + ","
                        + (colAppearCounts.get(col) == null ? 0 : colAppearCounts.get(col)) + "," + columnSizes.get(col));
                writer.newLine();
            }
            writer.close();
            metadataService.shutdown();
        }
        catch (IOException | MetadataException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void analyze1()
    {
        String workloadPath = "/Users/Jelly/Desktop/pixels/cache/Mar12/1187_dedup_query.txt";
        String cachedColsPath = "/Users/Jelly/Desktop/pixels/cache/Mar12/updated_cached_cols";
        String colStatsPath = "/Users/Jelly/Desktop/pixels/cache/Mar12/workload_cols.csv";
        String queryInCacheStatPath = "/Users/Jelly/Desktop/pixels/cache/Mar12/query_cols_in_cache.csv";

        Set<String> cachedCols = new HashSet<>();
        Map<String, Double> colSizeMap = new HashMap<>();

        try
        {
            BufferedReader cachedColsReader = new BufferedReader(new FileReader(cachedColsPath));
            BufferedReader colsStatsReader = new BufferedReader(new FileReader(colStatsPath));
            BufferedReader workloadReader = new BufferedReader(new FileReader(workloadPath));
            BufferedWriter queryInCacheStatWriter = new BufferedWriter(new FileWriter(queryInCacheStatPath));

            String line;
            String qId;

            while ((line = cachedColsReader.readLine()) != null)
            {
                String[] cols = line.toLowerCase().split(",");
                cachedCols.addAll(Arrays.asList(cols));
            }

            colsStatsReader.readLine();  // skip the header
            while ((line = colsStatsReader.readLine()) != null)
            {
                String colName = line.split(",")[0].toLowerCase();
                Double colSize = Double.parseDouble(line.split(",")[3]);
                colSizeMap.put(colName, colSize);
            }

            while ((line = workloadReader.readLine()) != null)
            {
                if (line.isEmpty())
                {
                    continue;
                }
                if (false == line.contains("SELECT"))
                {
                    qId = line;
                    queryInCacheStatWriter.write(qId + ",");
                }
                else
                {
                    line = line.toLowerCase();
                    String[] lineSplits = line.split("\\s");
                    String selection = lineSplits[1];
                    String orderBy = lineSplits[6];
                    String[] selectionCols = selection.split(",");
                    String[] orderCols = orderBy.split(",");
                    String[] cols = new String[selectionCols.length + orderCols.length];
                    System.arraycopy(selectionCols, 0, cols, 0, selectionCols.length);
                    System.arraycopy(orderCols, 0, cols, selectionCols.length, orderCols.length);
                    int inCacheNum = 0;
                    int totalNum = cols.length;
                    double inCacheSize = 0;
                    double totalSize = 0;
                    for (String col : cols)
                    {
                        double colSize = colSizeMap.get(col);
                        if (cachedCols.contains(col))
                        {
                            inCacheNum++;
                            inCacheSize += colSize;
                        }
                        totalSize += colSize;
                    }
                    queryInCacheStatWriter.write(inCacheNum + "," + totalNum + "," + inCacheSize + "," + totalSize);
                    queryInCacheStatWriter.newLine();
                }
            }
            colsStatsReader.close();
            workloadReader.close();
            cachedColsReader.close();
            queryInCacheStatWriter.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
