/*
 * Copyright 2025 PixelsDB.
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

package io.pixelsdb.pixels.cli.load;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.daemon.NodeProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author hank
 * @author: tao
 * @create in 2018-10-30 15:18
 **/
public abstract class AbstractPixelsConsumer extends Consumer
{
    public static final AtomicInteger GlobalTargetPathId = new AtomicInteger(0);
    protected final BlockingQueue<String> inputFiles;
    protected final Parameters parameters;
    protected final ConcurrentLinkedQueue<LoadedInfo> loadedInfos;
    protected final List<File> tmpFiles = new ArrayList<>();
    protected final List<Path> targetPaths;
    protected MetadataService metadataService;
    // Shared configurations
    protected TypeDescription schema;
    protected int maxRowNum;
    protected String regex;
    protected EncodingLevel encodingLevel;
    protected boolean nullsPadding;
    protected SinglePointIndex index;
    protected IndexService defaultIndexService;
    protected int[] pkMapping;
    protected int[] orderMapping;
    protected TypeDescription pkTypeDescription;
    // Shared Pixels file configs
    protected int pixelStride;
    protected int rowGroupSize;
    protected long blockSize;
    protected short replication;

    protected AbstractPixelsConsumer(BlockingQueue<String> inputFiles, Parameters parameters,
                                     ConcurrentLinkedQueue<LoadedInfo> loadedInfos)
    {
        this.inputFiles = inputFiles;
        this.parameters = parameters;
        this.loadedInfos = loadedInfos;
        this.metadataService = parameters.getMetadataService();
        this.targetPaths = parameters.getLoadingPaths();
        initializeCommonConfig();
    }

    private void initializeCommonConfig()
    {
        // Initialization logic moved from original run()
        this.schema = TypeDescription.fromString(parameters.getSchema());
        this.maxRowNum = parameters.getMaxRowNum();
        this.regex = parameters.getRegex().equals("\\s") ? " " : parameters.getRegex();
        this.encodingLevel = parameters.getEncodingLevel();
        this.nullsPadding = parameters.isNullsPadding();
        this.orderMapping = parameters.getOrderMapping();
        this.pkMapping = parameters.getPkMapping();
        this.index = parameters.getIndex();
        this.pkTypeDescription = parameters.getPkTypeDescription();
        this.defaultIndexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.rpc);

        ConfigFactory configFactory = ConfigFactory.Instance();
        this.pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        this.rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
        this.blockSize = Long.parseLong(configFactory.getProperty("block.size"));
        this.replication = Short.parseShort(configFactory.getProperty("block.replication"));
    }

    @Override
    public void run()
    {
        System.out.println("Start " + this.getClass().getSimpleName() + ", " + Thread.currentThread().getName() +
                ", time: " + DateUtil.formatTime(new Date()));
        int count = 0;
        boolean isRunning = true;

        try
        {
            while (isRunning)
            {
                String originalFilePath = inputFiles.poll(2, TimeUnit.SECONDS);
                if (originalFilePath != null)
                {
                    count++;
                    processSourceFile(originalFilePath);
                } else
                {
                    // No source file can be consumed within 2 seconds, loading is considered finished.
                    isRunning = false;
                }
            }

            // Flush any remaining data from writers
            flushRemainingData();

        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(this.getClass().getSimpleName() + " interrupted", e);
        } catch (RuntimeException e)
        {
            throw e;
        } catch (Exception e)
        {
            throw new RuntimeException(this.getClass().getSimpleName() + " failed", e);
        } finally
        {
            cleanupTemporaryFiles();
            System.out.println(Thread.currentThread().getName() + ":" + count);
            System.out.println("Exit " + this.getClass().getSimpleName() + ", thread: " + Thread.currentThread().getName() +
                    ", time: " + DateUtil.formatTime(new Date()));
        }
    }

    protected abstract void processSourceFile(String originalFilePath) throws IOException, MetadataException;

    protected abstract void flushRemainingData() throws IOException, MetadataException;

    private void cleanupTemporaryFiles()
    {
        for (File tmpFile : tmpFiles)
        {
            if (tmpFile.getType() == File.Type.TEMPORARY_INGEST)
            {
                try
                {
                    if (!metadataService.deleteFiles(Collections.singletonList((tmpFile.getId()))))
                    {
                        throw new MetadataException("failed to delete temporary load file " + tmpFile.getId());
                    }
                } catch (MetadataException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Close the pixels writer and add the file to loaded file queue.
     * Files in the loaded files queue will be updated in metadata.
     *
     * @param pixelsWriter the pixels writer
     * @param loadedFile   the file name has been loaded
     * @param filePath     the path of the directory where the file was written
     * @throws IOException
     */
    protected void closeWriterAndAddFile(PixelsWriter pixelsWriter, File loadedFile, Path filePath, NodeProto.NodeInfo nodeInfo) throws IOException
    {
        pixelsWriter.close();
        loadedFile.setType(File.Type.REGULAR);
        loadedFile.setNumRowGroup(pixelsWriter.getNumRowGroup());
        LoadedInfo loadedInfo = new LoadedInfo();
        loadedInfo.loadedFile = loadedFile;
        loadedInfo.loadedPath = filePath;
        loadedInfo.loadedRetinaNode = nodeInfo;
        this.loadedInfos.add(loadedInfo);
    }

    /**
     * Create a temporary file through the metadata service
     *
     * @param fileName the file name without directory path
     * @param filePath the path of the directory where the file was written
     */
    protected File openTmpFile(String fileName, Path filePath) throws MetadataException
    {
        File file = new File();
        file.setName(fileName);
        file.setType(File.Type.TEMPORARY_INGEST);
        file.setNumRowGroup(1);
        file.setPathId(filePath.getId());
        String tmpFilePath = filePath.getUri() + "/" + fileName;
        if (!this.metadataService.addFiles(Collections.singletonList(file)))
        {
            throw new MetadataException("failed to add temporary load file " + tmpFilePath);
        }
        file.setId(metadataService.getFileId(tmpFilePath));
        return file;
    }

    protected PixelsWriter getPixelsWriter(Storage targetStorage, String targetFilePath)
    {
        return PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setHasHiddenColumn(true)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(targetStorage)
                .setPath(targetFilePath)
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncodingLevel(encodingLevel)
                .setNullsPadding(nullsPadding)
                .setCompressionBlockSize(1)
                .build();
    }

    protected void writeRowToBatch(VectorizedRowBatch rowBatch, String line, long timestamp)
    {
        // limit=-1 keeps trailing empty fields; String.split(regex) drops them by default.
        String[] colsInLine = Pattern.compile(Pattern.quote(regex)).split(line, -1);
        writeRowToBatch(rowBatch, colsInLine, timestamp);
    }

    protected void writeRowToBatch(VectorizedRowBatch rowBatch, String[] colsInLine, long timestamp)
    {
        ColumnVector[] columnVectors = rowBatch.cols;

        for (int i = 0; i < columnVectors.length - 1; i++)
        {
            int valueIdx = orderMapping[i];
            if (valueIdx < 0 || valueIdx >= colsInLine.length)
            {
                throw new IllegalArgumentException("Missing value at input index " + valueIdx + " for column " + i);
            }

            String value = colsInLine[valueIdx];
            try
            {
                // LOAD dialect: \N/null -> NULL; empty text/binary -> ""; empty other -> NULL.
                if (value == null || value.equalsIgnoreCase("\\N"))
                {
                    columnVectors[i].addNull();
                }
                else if (!value.isEmpty())
                {
                    columnVectors[i].add(value);
                }
                else
                {
                    switch (schema.getChildren().get(i).getCategory())
                    {
                        case CHAR:
                        case VARCHAR:
                        case STRING:
                        case BINARY:
                        case VARBINARY:
                            columnVectors[i].add("");
                            break;
                        default:
                            columnVectors[i].addNull();
                            break;
                    }
                }
            } catch (Exception e)
            {
                throw new IllegalArgumentException("Failed to parse column " + i + " from value " + value, e);
            }
        }
        columnVectors[columnVectors.length - 1].add(timestamp);
        rowBatch.size++;
    }
}
