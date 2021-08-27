/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 * @author hank
 */
public class MockPixelsReader
        implements Runnable
{
    private final Storage storage;
    private final String filePath;
    private final String[] schema;

    public MockPixelsReader(Storage storage, String filePath, String[] schema)
    {
        this.storage = storage;
        this.filePath = filePath;
        this.schema = schema;
    }

    @Override
    public void run()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(schema);

        try
        {
            PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(filePath)
                    .build();
            PixelsRecordReader recordReader = pixelsReader.read(option);
            VectorizedRowBatch rowBatch;
            int batchSize = 10000;
            long num = 0;
            long start = System.currentTimeMillis();
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                if (rowBatch.endOfFile)
                {
                    num += rowBatch.size;
                    break;
                }
                num += rowBatch.size;
            }
            long end = System.currentTimeMillis();
            System.out.println("[" + filePath + "] "
                    + start + " " + end + " " + num + ", cpu cost: " + (end - start));
            pixelsReader.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
