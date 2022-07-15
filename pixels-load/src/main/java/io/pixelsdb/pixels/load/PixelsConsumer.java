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
package io.pixelsdb.pixels.load;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author: tao
 * @author hank
 * @date: Create in 2018-10-30 15:18
 **/
public class PixelsConsumer extends Consumer
{

    private BlockingQueue<String> queue;
    private Properties prop;
    private Config config;

    public Properties getProp()
    {
        return prop;
    }

    public PixelsConsumer(BlockingQueue<String> queue, Properties prop, Config config)
    {
        this.queue = queue;
        this.prop = prop;
        this.config = config;
    }

    @Override
    public void run()
    {
        System.out.println("Start PixelsConsumer, " + currentThread().getName() + ", time: " + DateUtil.formatTime(new Date()));
        int count = 0;

        boolean isRunning = true;
        try
        {
            String targetDirPath = config.getPixelsPath();
            String schemaStr = config.getSchema();
            int[] orderMapping = config.getOrderMapping();
            int maxRowNum = config.getMaxRowNum();
            String regex = config.getRegex();
            if (regex.equals("\\s"))
            {
                regex = " ";
            }

            Properties prop = getProp();
            int pixelStride = Integer.parseInt(prop.getProperty("pixel.stride"));
            int rowGroupSize = Integer.parseInt(prop.getProperty("row.group.size"));
            long blockSize = Long.parseLong(prop.getProperty("block.size"));
            short replication = Short.parseShort(prop.getProperty("block.replication"));

            TypeDescription schema = TypeDescription.fromString(schemaStr);
            // System.out.println(schemaStr);
            // System.out.println(loadingDataPath);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            ColumnVector[] columnVectors = rowBatch.cols;

            BufferedReader reader;
            String line;

            boolean initPixelsFile = true;
            String targetFilePath;
            PixelsWriter pixelsWriter = null;
            int rowCounter = 0;

            Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

            while (isRunning)
            {
                String originalFilePath = queue.poll(2, TimeUnit.SECONDS);
                if (originalFilePath != null)
                {
                    count++;
                    Storage originStorage = StorageFactory.Instance().getStorage(originalFilePath);
                    reader = new BufferedReader(new InputStreamReader(originStorage.open(originalFilePath)));

                    while ((line = reader.readLine()) != null)
                    {
                        if (initPixelsFile == true)
                        {
                            if(line.length() == 0)
                            {
                                System.out.println(currentThread().getName() + "\tcontent: (" + line + ")");
                                continue;
                            }
                            // we create a new pixels file if we can read a next line from the source file.

                            targetFilePath = targetDirPath + DateUtil.getCurTime() + ".pxl";
                            pixelsWriter = PixelsWriterImpl.newBuilder()
                                    .setSchema(schema)
                                    .setPixelStride(pixelStride)
                                    .setRowGroupSize(rowGroupSize)
                                    .setStorage(targetStorage)
                                    .setPath(targetFilePath)
                                    .setBlockSize(blockSize)
                                    .setReplication(replication)
                                    .setBlockPadding(true)
                                    .setEncoding(true)
                                    .setCompressionBlockSize(1)
                                    .build();
                        }
                        initPixelsFile = false;

                        rowBatch.size++;
                        rowCounter++;

                        String[] colsInLine = line.split(regex);
                        for (int i = 0; i < columnVectors.length; i++)
                        {
                            try
                            {
                                int valueIdx = orderMapping[i];
                                if (colsInLine[valueIdx].isEmpty() ||
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

                        if (rowBatch.size >= rowBatch.getMaxSize())
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                            rowBatch.reset();
                            if (rowCounter >= maxRowNum)
                            {
                                pixelsWriter.close();
                                rowCounter = 0;
                                initPixelsFile = true;
                            }
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
                // left last file to write
                if (rowBatch.size != 0)
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
                pixelsWriter.close();
            }
        } catch (InterruptedException e)
        {
            System.out.println("PixelsConsumer: " + e.getMessage());
            currentThread().interrupt();
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            System.out.println(currentThread().getName() + ":" + count);
            System.out.println("Exit PixelsConsumer, " + currentThread().getName() + ", time: " + DateUtil.formatTime(new Date()));
        }
    }
}
