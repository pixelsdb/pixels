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
package io.pixelsdb.pixels.cli.executor;

import com.facebook.presto.jdbc.PrestoDriver;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.cli.Main;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.trino.jdbc.TrinoDriver;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-04-16
 */
public class StatExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        String schemaName = ns.getString("schema");
        String tableName = ns.getString("table");
        int concurrency = Integer.parseInt(ns.getString("concurrency"));
        boolean orderedEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.ordered.layout.enabled"));
        boolean compactEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.compact.layout.enabled"));

        MetadataService metadataService = MetadataService.Instance();
        List<Layout> layouts = metadataService.getLayouts(schemaName, tableName);
        List<String> files = new LinkedList<>();
        for (Layout layout : layouts)
        {
            if (layout.isReadable())
            {
                if (orderedEnabled)
                {
                    String[] orderedPaths = layout.getOrderedPathUris();
                    Main.validateOrderedOrCompactPaths(orderedPaths);
                    Storage storage = StorageFactory.Instance().getStorage(orderedPaths[0]);
                    files.addAll(storage.listPaths(orderedPaths));
                }
                if (compactEnabled)
                {
                    String[] compactPaths = layout.getCompactPathUris();
                    Main.validateOrderedOrCompactPaths(compactPaths);
                    Storage storage = StorageFactory.Instance().getStorage(compactPaths[0]);
                    files.addAll(storage.listPaths(compactPaths));
                }
            }
        }

        // get the statistics.
        long startTime = System.currentTimeMillis();

        List<Column> columns = metadataService.getColumns(schemaName, tableName, true);
        Map<String, Column> columnMap = new HashMap<>(columns.size());
        Map<String, StatsRecorder> columnStatsMap = new ConcurrentHashMap<>(columns.size());

        for (Column column : columns)
        {
            column.setChunkSize(0);
            column.setSize(0);
            columnMap.put(column.getName(), column);
        }

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        AtomicInteger totalRowGroupCount = new AtomicInteger(0);
        AtomicLong totalRowCount = new AtomicLong(0L);
        System.out.println("Read File Count: " + files.size() + "\tConcurrency: "+ concurrency);
        CompletableFuture.allOf(files.stream().map(filePath ->
                CompletableFuture.runAsync(() ->
                {
                    try
                    {
                        processFile(filePath, columnMap, columnStatsMap, totalRowGroupCount, totalRowCount);
                    }
                    catch (Exception e)
                    {
                        System.err.println("Error processing file: " + filePath);
                        e.printStackTrace();
                    }
                }, executor)
        ).toArray(CompletableFuture[]::new)).join();
        executor.shutdown();
        {
            long readFileEndTime = System.currentTimeMillis();
            System.out.println("Read File Elapsed time: " + (readFileEndTime - startTime) / 1000.0 + "s.");
        }

        ConfigFactory instance = ConfigFactory.Instance();
        Properties properties = new Properties();
        properties.setProperty("user", instance.getProperty("presto.user"));
        // properties.setProperty("password", instance.getProperty("presto.password"));
        properties.setProperty("SSL", instance.getProperty("presto.ssl"));
        StringBuilder builder = new StringBuilder()
                .append("pixels.ordered_path_enabled:").append(orderedEnabled).append(";")
                .append("pixels.compact_path_enabled:").append(compactEnabled);
        properties.setProperty("sessionProperties", builder.toString());
        String jdbc = instance.getProperty("presto.jdbc.url");
        try
        {
            DriverManager.registerDriver(new TrinoDriver());
            DriverManager.registerDriver(new PrestoDriver());
        } catch (SQLException e)
        {
            e.printStackTrace();
        }

        for (Column column : columns)
        {
            column.setChunkSize(column.getSize() / totalRowGroupCount.get());
            column.setRecordStats(columnStatsMap.get(column.getName())
                    .serialize().build().toByteString().asReadOnlyByteBuffer());
            column.getRecordStats().mark();
            metadataService.updateColumn(column);
            column.getRecordStats().reset();
        }

        metadataService.updateRowCount(schemaName, tableName, totalRowCount.get());

        /* Set cardinality and null_fraction after the chunk size and column size,
         * because chunk size and column size must exist in the metadata when calculating
         * the cardinality and null_fraction using SQL queries.
         *
         * Issue #485:
         * No need to drop the cached columns here, because metadata cache is only used in
         * the query engine process during transactional query execution.
         */
        try (Connection connection = DriverManager.getConnection(jdbc, properties))
        {
            for (Column column : columns)
            {
                // ANSI SQL uses double quotes for identifiers.
                String sql = String.format(
                        "SELECT COUNT(DISTINCT(\"%s\")) AS cardinality, " +
                                "SUM(CASE WHEN \"%s\" IS NULL THEN 1 ELSE 0 END) AS null_count " +
                                "FROM \"%s\".\"%s\"",
                        column.getName(),
                        column.getName(),
                        schemaName,
                        tableName
                );
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next())
                {
                    long cardinality = resultSet.getLong("cardinality");
                    double nullFraction = resultSet.getLong("null_count") / (double) totalRowCount.get();
                    System.out.println(column.getName() + " cardinality: " + cardinality +
                            ", null fraction: " + nullFraction);
                    column.setCardinality(cardinality);
                    column.setNullFraction(nullFraction);
                }
                resultSet.close();
                statement.close();
                metadataService.updateColumn(column);
            }
        } catch (SQLException e)
        {
            System.err.println(command + "failed");
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (endTime - startTime) / 1000.0 + "s.");
    }

    private void processFile(String filePath,
                             Map<String, Column> columnMap,
                             Map<String, StatsRecorder> columnStatsMap,
                             AtomicInteger totalRowGroupCount,
                             AtomicLong totalRowCount) throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(filePath);
        try (PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath(filePath).setStorage(storage).setEnableCache(false)
                .setCacheOrder(ImmutableList.of()).setPixelsCacheReader(null)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            PixelsProto.Footer fileFooter = pixelsReader.getFooter();
            int numRowGroup = pixelsReader.getRowGroupNum();
            totalRowGroupCount.addAndGet(numRowGroup);
            totalRowCount.addAndGet(pixelsReader.getNumberOfRows());
            List<PixelsProto.Type> types = fileFooter.getTypesList();
            for (int i = 0; i < numRowGroup; ++i)
            {
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(i);
                List<PixelsProto.ColumnChunkIndex> chunkIndices =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntriesList();
                for (int j = 0; j < types.size(); ++j)
                {
                    Column column = columnMap.get(types.get(j).getName());
                    synchronized (column)
                    {
                        long chunkLength = chunkIndices.get(j).getChunkLength();
                        column.setSize(chunkLength + column.getSize());
                    }
                }
            }
            List<TypeDescription> fields = pixelsReader.getFileSchema().getChildren();
            checkArgument(fields.size() == types.size(),
                    "types.size and fields.size are not consistent");
            for (int i = 0; i < fields.size(); ++i)
            {
                TypeDescription field = fields.get(i);
                PixelsProto.Type type = types.get(i);

                PixelsProto.ColumnStatistic currentStat = fileFooter.getColumnStats(i);
                columnStatsMap.compute(type.getName(), (k, existingRecorder) ->
                {
                    if (existingRecorder == null)
                    {
                        return StatsRecorder.create(field, currentStat);
                    } else
                    {
                        existingRecorder.merge(StatsRecorder.create(field, currentStat));
                        return existingRecorder;
                    }
                });
            }
        }
    }
}
