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
package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.List;

public class TestPixelsReaderWithDeletion
{
    private final static RetinaService retinaService = RetinaService.Instance();
    // private final static String currentPath = "/home/gengdy/data/tpch/1g/nation/v-0-ordered/20250314082208_12.pxl";
    // private final static long currentFileId = 55;
    private final static String currentPath = "/home/gengdy/data/tpch/1g/nation/v-0-compact/20250314082324_24_compact.pxl";
    private final static long currentFileId = 67;
    private final static String currentPathUri = "file://" + currentPath;

    public static void printRecord(long timestamp)
    {
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("file");
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(currentPath)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();

            TypeDescription schema = reader.getFileSchema();
            List<String> fieldNames = schema.getFieldNames();
            System.out.println("fieldNames: " + fieldNames);
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++)
            {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            option.transTimestamp(timestamp);
            PixelsRecordReader recordReader = reader.read(option);
            System.out.println("recordReader.getCompletedRows():" + recordReader.getCompletedRows());
            System.out.println("reader.getRowGroupInfo(0).getNumberOfRows():" + reader.getRowGroupInfo(0).getNumberOfRows());
            int batchSize = 10000;

            VectorizedRowBatch rowBatch;
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                System.out.println("rowBatch: " + rowBatch);
                if (rowBatch.endOfFile)
                {
                    break;
                }
            }

            reader.close();
        } catch (IOException e)
        {
            System.out.println("Err path: " + currentPath.toString());
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        printRecord(-1L);

        // delete some records
        try
        {
            retinaService.deleteRecord(currentFileId, 0, 5, 5);
            retinaService.deleteRecord(currentFileId, 0, 10, 10);
            retinaService.deleteRecord(currentFileId, 0, 15, 15);
            retinaService.deleteRecord(currentFileId, 0, 20, 20);
        } catch (RetinaException e)
        {
            throw new RuntimeException("Failed to delete records", e);
        }

        printRecord(5);
        printRecord(10);
        printRecord(15);
        printRecord(20);
    }
}
