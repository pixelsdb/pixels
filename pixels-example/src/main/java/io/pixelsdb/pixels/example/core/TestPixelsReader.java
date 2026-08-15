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
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

/**
 * Minimal example showing how to read a Pixels file from the local file system.
 * <p>
 * Run {@code TestPixelsWriter} first to produce the file, then this example opens a
 * {@link PixelsReader}, selects the columns to project via {@link PixelsReaderOption},
 * and iterates {@link VectorizedRowBatch}es until end of file.
 * <p>
 * For an end-to-end write-then-read example with per-value verification across all
 * column types, see {@code TestPixelsReadWrite}.
 *
 * @author hank
 * @create 2018-11-19
 */
public class TestPixelsReader
{
    private static final int BATCH_SIZE = 10000;

    public static void main(String[] args) throws Exception
    {
        String pixelsFile = "/tmp/pixels-writer-example.pxl";
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);

        try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(pixelsFile)
                .setPixelsFooterCache(new PixelsFooterCache())
                .build())
        {
            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            // project all columns of the file schema
            option.includeCols(reader.getFileSchema().getFieldNames().toArray(new String[0]));

            PixelsRecordReader recordReader = reader.read(option);
            long totalRows = 0;
            while (true)
            {
                VectorizedRowBatch rowBatch = recordReader.readBatch(BATCH_SIZE);
                totalRows += rowBatch.size;
                if (rowBatch.endOfFile)
                {
                    break;
                }
            }
            recordReader.close();
            System.out.println("Read " + totalRows + " rows from " + pixelsFile);
        }
    }
}
