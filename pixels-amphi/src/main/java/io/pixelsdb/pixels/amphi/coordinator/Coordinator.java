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
package io.pixelsdb.pixels.amphi.coordinator;

import io.pixelsdb.pixels.amphi.analyzer.PlanAnalysis;
import io.pixelsdb.pixels.common.exception.AmphiException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.parser.PixelsParser;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Coordinator decides where to execute the query request (in-cloud or on-premise).
 */
public class Coordinator
{
    private final static Logger logger = LogManager.getLogger(Coordinator.class);

    private final MetadataService metadataService;

    public Coordinator(MetadataService metadataService)
    {
        this.metadataService = metadataService;
    }

    /**
     * Endpoint to make the coordinator decision.
     * @param query
     * @param schema
     * @param peerName
     * @return boolean indicator on query execution in-cloud (true) or on-premise (false)
     * @throws SqlParseException
     * @throws AmphiException
     * @throws IOException
     * @throws InterruptedException
     * @throws MetadataException
     */
    public boolean decideInCloud(String query, String schema, String peerName)
            throws SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        PixelsParser parser = new PixelsParser(metadataService, schema, parserConfig, properties);
        SqlNode parsedNode = parser.parseQuery(query);
        SqlNode validatedNode = parser.validate(parsedNode);
        RelNode rel = parser.toRelNode(validatedNode);
        logger.trace("Parsed SQL Query: \n" + parsedNode);

        // Run one-time traversal to collect analysis
        PlanAnalysis analysis = new PlanAnalysis(metadataService, query, rel, schema);
        analysis.analyze();
        Map<String, List<String>> projectColumns = analysis.getProjectColumns();
        logger.trace("Required columns for the query: " + projectColumns);

        // If the worker has already cached all columns to execute the query, then perform on the worker
        if (cachedAllColumns(projectColumns, schema, peerName))
        {
            logger.trace("Peer " + peerName + " does not contain all required columns for query " + query);
            return false;
        }

        logger.trace("Peer " + peerName + " contains all required columns for query " + query);
        return true;
    }

    /**
     * Retrieve peer path from metadata service and decide the availability of required columns.
     * @param columns
     * @param schema
     * @param peerName
     * @return boolean indicator on local availability of columns
     * @throws MetadataException
     */
    private boolean cachedAllColumns(Map<String, List<String>> columns, String schema, String peerName)
            throws MetadataException
    {
        Peer peer = metadataService.getPeer(peerName);
        List<PeerPath> peerPaths = metadataService.getPeerPaths(peer.getId(), false);
        Map<Long, List<Column>> pathIdToColumns = peerPaths.stream()
                .collect(Collectors.toMap(PeerPath::getPathId, PeerPath::getColumns));

        for (Map.Entry<String, List<String>> entry : columns.entrySet())
        {
            String table = entry.getKey();
            List<String> columnList = entry.getValue();

            if (columnList.isEmpty())
            {
                continue;
            }

            Layout layout = metadataService.getLatestLayout(schema, table);
            Path orderedPath = metadataService.getPaths(layout.getId(), true).get(0);

            // Check if columnList subset of cachedColumns
            if (!pathIdToColumns.containsKey(orderedPath.getId()))
            {
                return false;
            }
            List<String> cachedColumns = pathIdToColumns.get(orderedPath.getId())
                    .stream().map(column -> column.getName()).collect(Collectors.toList());
            if (!cachedColumns.containsAll(columnList))
            {
                return false;
            }
        }

        return true;
    }
}
