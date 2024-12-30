/*
 * Copyright 2018-2019 PixelsDB.
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

import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: tao
 * @author hank
 * @create in 2018-10-30 15:18
 **/
public class PixelsConsumer extends Consumer
{
    public static final AtomicInteger GlobalTargetPathId = new AtomicInteger(0);
    private final BlockingQueue<String> queue;
    private final Parameters parameters;
    private final ConcurrentLinkedQueue<File> loadedFiles;

    public PixelsConsumer(BlockingQueue<String> queue, Parameters parameters, ConcurrentLinkedQueue<File> loadedFiles)
    {
        this.queue = queue;
        this.parameters = parameters;
        this.loadedFiles = loadedFiles;
    }

    @Override
    public void run()
    {
        System.out.println("Start PixelsConsumer, " + currentThread().getName() +
                ", time: " + DateUtil.formatTime(new Date()));
        int count = 0;

        boolean isRunning = true;
        try
        {
            final List<Path> targetPaths = parameters.getLoadingPaths();
            String schemaStr = parameters.getSchema();
            int[] orderMapping = parameters.getOrderMapping();
            int maxRowNum = parameters.getMaxRowNum();
            String regex = parameters.getRegex();
            EncodingLevel encodingLevel = parameters.getEncodingLevel();
            boolean nullsPadding = parameters.isNullsPadding();
            if (regex.equals("\\s"))
            {
                regex = " ";
            }

            ConfigFactory configFactory = ConfigFactory.Instance();
            int pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
            int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
            long blockSize = Long.parseLong(configFactory.getProperty("block.size"));
            short replication = Short.parseShort(configFactory.getProperty("block.replication"));

            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
            ColumnVector[] columnVectors = rowBatch.cols;

            BufferedReader reader;
            String line;

            boolean initPixelsFile = true;
            String targetFileName = null;
            String targetFilePath;
            PixelsWriter pixelsWriter = null;
            Path currTargetPath = null;
            int rowCounter = 0;

            while (isRunning)
            {
                String originalFilePath = queue.poll(2, TimeUnit.SECONDS);
                if (originalFilePath != null)
                {
                    count++;
                    Storage originStorage = StorageFactory.Instance().getStorage(originalFilePath);
                    reader = new BufferedReader(new InputStreamReader(originStorage.open(originalFilePath)));

                    System.out.println("loading data from: " + originalFilePath);

                    // loaded rows use the same timestamp
                    long timestamp = parameters.getTimestamp();
                    while ((line = reader.readLine()) != null)
                    {
                        if (initPixelsFile)
                        {
                            if(line.isEmpty())
                            {
                                System.err.println("thread: " + currentThread().getName() + " got empty line.");
                                continue;
                            }
                            // we create a new pixels file if we can read a next line from the source file

                            // choose the target output directory using round-robin
                            int targetPathId = GlobalTargetPathId.getAndIncrement() % targetPaths.size();
                            currTargetPath = targetPaths.get(targetPathId);
                            String targetDirPath = currTargetPath.getUri();
                            Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

                            if (!targetDirPath.endsWith("/"))
                            {
                                targetDirPath += "/";
                            }
                            targetFileName = DateUtil.getCurTime() + ".pxl";
                            targetFilePath = targetDirPath + targetFileName;

                            pixelsWriter = PixelsWriterImpl.newBuilder()
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
                        initPixelsFile = false;

                        rowBatch.size++;
                        rowCounter++;

                        String[] colsInLine = line.split(regex);
                        for (int i = 0; i < columnVectors.length - 1; i++)
                        {
                            try
                            {
                                int valueIdx = orderMapping[i];
                                if (valueIdx >= colsInLine.length ||
                                        colsInLine[valueIdx].isEmpty() ||
                                        colsInLine[valueIdx].equalsIgnoreCase("\\N"))
                                {
                                    columnVectors[i].addNull();
                                } else
                                {
                                    columnVectors[i].add(colsInLine[valueIdx]);
                                }
                            }
                            catch (Exception e)
                            {
                                System.out.println("line: " + line);
                                e.printStackTrace();
                            }
                        }
                        // add hidden timestamp column value
                        columnVectors[columnVectors.length - 1].add(timestamp);

                        if (rowBatch.size >= rowBatch.getMaxSize())
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                            rowBatch.reset();
                        }

                        if (rowCounter >= maxRowNum)
                        {
                            // finish writing the file
                            if (rowBatch.size != 0)
                            {
                                pixelsWriter.addRowBatch(rowBatch);
                                rowBatch.reset();
                            }
                            closeWriterAndAddFile(pixelsWriter, targetFileName, currTargetPath);
                            rowCounter = 0;
                            initPixelsFile = true;
                        }
                    }
                    reader.close();
                } else
                {
                    // no source file can be consumed within 2 seconds,
                    // loading is considered to be finished.
                    isRunning = false;
                }
            }

            if (rowCounter > 0)
            {
                // last file to write
                if (rowBatch.size != 0)
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
                closeWriterAndAddFile(pixelsWriter, targetFileName, currTargetPath);
            }
        } catch (InterruptedException e)
        {
            System.out.println("PixelsConsumer: " + e.getMessage());
            currentThread().interrupt();
        } catch (Throwable e)
        {
            e.printStackTrace();
        } finally
        {
            System.out.println(currentThread().getName() + ":" + count);
            System.out.println("Exit PixelsConsumer, thread: " + currentThread().getName() +
                    ", time: " + DateUtil.formatTime(new Date()));
        }
    }

    /**
     * Close the pixels writer and add the file to loaded file queue.
     * Files in the loaded files queue will be saved in metadata.
     * @param pixelsWriter the pixels writer
     * @param fileName the file name without directory path
     * @param filePath the path of the directory where the file was written
     * @throws IOException
     */
    private void closeWriterAndAddFile(PixelsWriter pixelsWriter, String fileName, Path filePath) throws IOException
    {
        pixelsWriter.close();
        File loadedFile = new File();
        loadedFile.setName(fileName);
        loadedFile.setNumRowGroup(pixelsWriter.getNumRowGroup());
        loadedFile.setPathId(filePath.getId());
        this.loadedFiles.offer(loadedFile);
    }
}
