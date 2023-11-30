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
package io.pixelsdb.pixels.amphi.downloader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Built as the main method to download partial data to the local storage.
 */
public class DownloadProcess {
    public static void main(String[] args)
            throws IOException, IllegalArgumentException, MetadataException
    {
        // Load configuration
        String configFilePath = args[0];
        String configContent = readConfigFile(configFilePath);

        String metadataServiceHost;
        int metadataServicePort;
        String schemaName;
        JSONArray schemaData;
        String inputStorage;
        String outputStorage;
        String outputDirectory;
        Path outputDirPath;

        if (configContent != null) {
            JSONObject configJson = JSON.parseObject(configContent);

            metadataServiceHost = configJson.getString("MetadataServiceHost");
            if (metadataServiceHost == null) {
                throw new IllegalArgumentException("MetadataServiceHost is missing in the configuration file.");
            }

            if (!configJson.containsKey("MetadataServicePort")) {
                throw new IllegalArgumentException("MetadataServicePort is missing in the configuration file.");
            }
            metadataServicePort = configJson.getIntValue("MetadataServicePort");

            schemaName = configJson.getString("SchemaName");
            if (schemaName == null) {
                throw new IllegalArgumentException("SchemaName is missing in the configuration file.");
            }

            schemaData = configJson.getJSONArray("SchemaData");
            if (schemaData == null) {
                throw new IllegalArgumentException("SchemaData is missing in the configuration file.");
            }

            inputStorage = configJson.getString("InputStorage");
            if (inputStorage == null) {
                throw new IllegalArgumentException("InputStorage is missing in the configuration file.");
            }

            outputStorage = configJson.getString("OutputStorage");
            if (outputStorage == null) {
                throw new IllegalArgumentException("OutputStorage is missing in the configuration file.");
            }

            outputDirectory = configJson.getString("OutputDirectory");
            if (outputDirectory == null) {
                throw new IllegalArgumentException("OutputDirectory is missing in the configuration file.");
            }
            outputDirPath = Paths.get(outputDirectory);
            if (!Files.exists(outputDirPath)) {
                throw new IllegalArgumentException("The output directory does not exist.");
            }
        } else {
            throw new IllegalArgumentException("Configuration file content is empty or wrongly formatted.");
        }

        // Retrieve metadata from host
        MetadataService metadataService = new MetadataService(metadataServiceHost, metadataServicePort);
        Storage inputStorageInstance = StorageFactory.Instance().getStorage(inputStorage);
        Storage outputStorageInstance = StorageFactory.Instance().getStorage(outputStorage);

        PartialSchemaLoader partialSchemaLoader = new PartialSchemaLoader();
        partialSchemaLoader.registerAllSchemas();

        // Download the data by table level
        for (int i = 0; i < schemaData.size(); i++)
        {
            JSONObject tableData = schemaData.getJSONObject(i);
            String tableName = tableData.getString("table");
            if (tableName == null) {
                throw new IllegalArgumentException("Table is missing in the SchemaData.");
            }

            // Extract partial column and schema
            List<String> columnNames = tableData.getJSONArray("columns").toJavaList(String.class);
            List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
            List<Column> partial = columns.stream()
                    .filter(column -> columnNames.contains(column.getName()))
                    .collect(Collectors.toList());
            Schema partialSchema = partialSchemaLoader.getPartialSchema(tableName, columnNames);

            // Get input path
            Layout layout = metadataService.getLatestLayout(schemaName, tableName);
            String path = layout.getOrderedPathUris()[0];
            if (path == null) {
                throw new MetadataException("Failed to get a valid path for: " + schemaName + ", " + tableName);
            }
            List<Status> statuses = inputStorageInstance.listStatus(path);
            String[] sourcePaths = statuses.stream().map(status -> status.getPath()).toArray(String[]::new);

            // Manage output directory
            Path tableDir = outputDirPath.resolve(tableName);
            try {
                if (!Files.exists(tableDir)) {
                    Files.createDirectory(tableDir);
                } else {
                    System.out.println("Directory already exists: " + tableDir);
                }
            } catch (IOException e) {
                new IOException("Cannot create directory: " + tableDir);
            }

            // Start download process
            PeerDownloader downloader = PeerDownloader.newBuilder()
                    .setSchema(partialSchema)
                    .setColumns(partial)
                    .setSourcePaths(Arrays.asList(sourcePaths))
                    .setInputStorage(inputStorageInstance)
                    .setOutputStorage(outputStorageInstance)
                    .setOutDirPath(tableDir.toString())
                    .build();
            downloader.writeParquetFile();

            System.out.println("Downloaded partial columns in table " + tableName);
        }
    }

    private static String readConfigFile(String filePath)
            throws IOException
    {
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            throw new IOException("Configuration file for download process does not exist: " + e.getMessage());
        }
    }
}

