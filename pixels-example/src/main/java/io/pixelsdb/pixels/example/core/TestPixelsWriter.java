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
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

/**
 * Minimal example showing how to write a Pixels file to the local file system.
 * <p>
 * The flow is: build a {@link PixelsWriter} against a {@link TypeDescription} schema,
 * fill {@link VectorizedRowBatch}es column by column, add them to the writer, and close.
 * <p>
 * For an end-to-end write-then-read example with per-value verification across all
 * column types, see {@code TestPixelsReadWrite}.
 *
 * @author hank
 * @create 2018-11-19
 */
public class TestPixelsWriter
{
    private static final String SCHEMA_STRING = "struct<id:int,name:string>";
    private static final int ROW_NUM = 1000;

    public static void main(String[] args) throws Exception
    {
        String pixelsFile = "/tmp/pixels-writer-example.pxl";
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);

        TypeDescription schema = TypeDescription.fromString(SCHEMA_STRING);
        PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(10000)
                .setRowGroupSize(64 * 1024 * 1024)
                .setStorage(storage)
                .setPath(pixelsFile)
                .setBlockSize(256 * 1024 * 1024)
                .setReplication((short) 1)
                .setBlockPadding(true)
                .setEncodingLevel(EncodingLevel.EL2)
                .setCompressionBlockSize(1)
                .build();

        VectorizedRowBatch rowBatch = schema.createRowBatch();
        IntColumnVector id = (IntColumnVector) rowBatch.cols[0];
        BinaryColumnVector name = (BinaryColumnVector) rowBatch.cols[1];

        for (int i = 0; i < ROW_NUM; i++)
        {
            int row = rowBatch.size++;
            id.vector[row] = i;
            id.isNull[row] = false;
            name.setVal(row, ("row-" + i).getBytes());
            name.isNull[row] = false;
            if (rowBatch.size == rowBatch.getMaxSize())
            {
                writer.addRowBatch(rowBatch);
                rowBatch.reset();
            }
        }
        if (rowBatch.size != 0)
        {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
        writer.close();
        System.out.println("Written " + ROW_NUM + " rows to " + pixelsFile);
    }
}
