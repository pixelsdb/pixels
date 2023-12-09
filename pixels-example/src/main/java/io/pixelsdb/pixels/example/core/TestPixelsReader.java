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
package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.List;

/**
 * @author hank
 */
public class TestPixelsReader
{
    public static void main(String[] args)
    {
        // Note you may need to restart intellij to let it pick up the updated environment variable value
        // example path: s3://bucket-name/test-file.pxl
        String currentPath = System.getenv("PIXELS_WRITE_READ_TO_S3_TEST_FILE");
        System.out.println(currentPath);
        try {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(currentPath)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();

            TypeDescription schema = reader.getFileSchema();
            List<String> fieldNames = schema.getFieldNames();
            System.out.println("fieldNames: " + fieldNames);
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            PixelsRecordReader recordReader = reader.read(option);
            System.out.println("recordReader.getCompletedRows():" + recordReader.getCompletedRows());
            System.out.println("reader.getRowGroupInfo(0).getNumberOfRows():" + reader.getRowGroupInfo(0).getNumberOfRows());
            int batchSize = 10000;
            VectorizedRowBatch rowBatch;
            int len = 0;
            int numRows = 0;
            int numBatches = 0;
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                System.out.println("rowBatch: " + rowBatch);
                numBatches++;
                String result = rowBatch.toString();
                len += result.length();
                System.out.println("loop:" + numBatches + ", rowBatchSize:" + rowBatch.size);
                if (rowBatch.endOfFile) {
                    numRows += rowBatch.size;
                    break;
                }
                numRows += rowBatch.size;
            }
            System.out.println("numBatches:" + numBatches + ", numRows:" + numRows);
            reader.close();
        } catch (IOException e) {
            System.out.println("Err path: " + currentPath.toString());
            e.printStackTrace();
        }
    }
}
