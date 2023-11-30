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

import java.sql.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.cli.Main.validateOrderOrCompactPath;

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
        boolean orderedEnabled = Boolean.parseBoolean(ns.getString("ordered_enabled"));
        boolean compactEnabled = Boolean.parseBoolean(ns.getString("compact_enabled"));

        String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
        int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metadataHost, metadataPort);
        List<Layout> layouts = metadataService.getLayouts(schemaName, tableName);
        List<String> files = new LinkedList<>();
        for (Layout layout : layouts)
        {
            if (layout.isReadable())
            {
                if (orderedEnabled)
                {
                    String[] orderedPaths = layout.getOrderedPathUris();
                    validateOrderOrCompactPath(orderedPaths);
                    Storage storage = StorageFactory.Instance().getStorage(orderedPaths[0]);
                    files.addAll(storage.listPaths(orderedPaths));
                }
                if (compactEnabled)
                {
                    String[] compactPaths = layout.getCompactPathUris();
                    validateOrderOrCompactPath(compactPaths);
                    Storage storage = StorageFactory.Instance().getStorage(compactPaths[0]);
                    files.addAll(storage.listPaths(compactPaths));
                }
            }
        }

        // get the statistics.
        long startTime = System.currentTimeMillis();

        List<Column> columns = metadataService.getColumns(schemaName, tableName, true);
        Map<String, Column> columnMap = new HashMap<>(columns.size());
        Map<String, StatsRecorder> columnStatsMap = new HashMap<>(columns.size());

        for (Column column : columns)
        {
            column.setChunkSize(0);
            column.setSize(0);
            columnMap.put(column.getName(), column);
        }

        int rowGroupCount = 0;
        long rowCount = 0;
        for (String filePath : files)
        {
            Storage storage = StorageFactory.Instance().getStorage(filePath);
            PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                    .setPath(filePath).setStorage(storage).setEnableCache(false)
                    .setCacheOrder(ImmutableList.of()).setPixelsCacheReader(null)
                    .setPixelsFooterCache(new PixelsFooterCache()).build();
            PixelsProto.Footer fileFooter = pixelsReader.getFooter();
            int numRowGroup = pixelsReader.getRowGroupNum();
            rowGroupCount += numRowGroup;
            rowCount += pixelsReader.getNumberOfRows();
            List<PixelsProto.Type> types = fileFooter.getTypesList();
            for (int i = 0; i < numRowGroup; ++i)
            {
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(i);
                List<PixelsProto.ColumnChunkIndex> chunkIndices =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntriesList();
                for (int j = 0; j < types.size(); ++j)
                {
                    Column column = columnMap.get(types.get(j).getName());
                    long chunkLength = chunkIndices.get(j).getChunkLength();
                    column.setSize(chunkLength + column.getSize());
                }
            }
            List<TypeDescription> fields = pixelsReader.getFileSchema().getChildren();
            checkArgument(fields.size() == types.size(),
                    "types.size and fields.size are not consistent");
            for (int i = 0; i < fields.size(); ++i)
            {
                TypeDescription field = fields.get(i);
                PixelsProto.Type type = types.get(i);
                StatsRecorder statsRecorder = columnStatsMap.get(type.getName());
                if (statsRecorder == null)
                {
                    columnStatsMap.put(type.getName(),
                            StatsRecorder.create(field, fileFooter.getColumnStats(i)));
                }
                else
                {
                    statsRecorder.merge(StatsRecorder.create(field, fileFooter.getColumnStats(i)));
                }
            }
            pixelsReader.close();
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
            column.setChunkSize(column.getSize() / rowGroupCount);
            column.setRecordStats(columnStatsMap.get(column.getName())
                    .serialize().build().toByteString().asReadOnlyByteBuffer());
            column.getRecordStats().mark();
            metadataService.updateColumn(column);
            column.getRecordStats().reset();
        }

        metadataService.updateRowCount(schemaName, tableName, rowCount);

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
                String sql = "SELECT COUNT(DISTINCT(" + column.getName() + ")) AS cardinality, " +
                        "SUM(CASE WHEN " + column.getName() + " IS NULL THEN 1 ELSE 0 END) AS null_count " +
                        "FROM " + schemaName + "." + tableName;
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next())
                {
                    long cardinality = resultSet.getLong("cardinality");
                    double nullFraction = resultSet.getLong("null_count") / (double) rowCount;
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
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (endTime - startTime) / 1000 + "s.");
        metadataService.shutdown();
    }
}
