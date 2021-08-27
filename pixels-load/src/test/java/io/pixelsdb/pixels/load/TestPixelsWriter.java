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
package io.pixelsdb.pixels.load;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.StringUtil;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.load.multi.Config;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created at: 18-11-19
 * Author: hank
 */
public class TestPixelsWriter
{
    @Test
    public void testWrite () throws IOException, MetadataException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        Config config = new Config("pixels", "test_105", 5000, "\t", "pixels", null);
        config.load(configFactory);
        String loadingDataPath = config.getPixelsPath();
        String schemaStr = config.getSchema();
        int[] orderMapping = config.getOrderMapping();
        int maxRowNum = config.getMaxRowNum();
        String regex = config.getRegex();

        int pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size")) * 1024 * 1024;
        long blockSize = Long.parseLong(configFactory.getProperty("block.size")) * 1024l * 1024l;
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));

        Storage storage = StorageFactory.Instance().getStorage("hdfs");
        TypeDescription schema = TypeDescription.fromString(schemaStr);
        VectorizedRowBatch rowBatch = schema.createRowBatch();
        ColumnVector[] columnVectors = rowBatch.cols;

        storage.delete(loadingDataPath + "test_5000_lines.pxl", false);

        BufferedReader reader = new BufferedReader(new InputStreamReader(storage.open(
                "hdfs://dbiir10:9000/pixels/pixels/test_105/source_small/000148_0_small")));
        String line;

        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(storage)
                .setFilePath(loadingDataPath + "test_5000_lines.pxl")
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncoding(true)
                .setCompressionBlockSize(1)
                .build();

        int rowCounter = 0;

        while ((line = reader.readLine()) != null)
        {
            line = StringUtil.replaceAll(line, "false", "0");
            line = StringUtil.replaceAll(line, "False", "0");
            line = StringUtil.replaceAll(line, "true", "1");
            line = StringUtil.replaceAll(line, "True", "1");
            int rowId = rowBatch.size++;
            rowCounter++;
            if (regex.equals("\\s"))
            {
                regex = " ";
            }
            String[] colsInLine = line.split(regex);
            for (int i = 0; i < columnVectors.length; i++)
            {
                int valueIdx = orderMapping[i];
                if (colsInLine[valueIdx].equalsIgnoreCase("\\N"))
                {
                    columnVectors[i].isNull[rowId] = true;
                } else
                {
                    columnVectors[i].add(colsInLine[valueIdx]);
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
                }
            }
        }

        reader.close();

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
    }

    @Test
    public void testRead()
            throws IOException, MetadataException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        Config config = new Config("pixels", "test_105", 5000, "\t", "pixels", null);
        config.load(configFactory);
        String loadingDataPath = config.getPixelsPath();

        Storage storage = StorageFactory.Instance().getStorage("hdfs");
        VectorizedRowBatch rowBatch;

        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(loadingDataPath + "test_5000_lines.pxl")
                .build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        String[] cols = {"querydayname"};
        option.includeCols(cols);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        rowBatch = recordReader.readBatch(5000);
        System.out.println(rowBatch.size);
    }
}
