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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.*;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * @author: tao
 * @date: Create in 2018-11-07 16:05
 **/
public class TestPixelsCore
{

    String hdfsDir = "/home/tao/data/hadoop-2.7.3/etc/hadoop/"; // dbiir10

    @Test
    public void testReadPixelsFile() throws IOException
    {
        String pixelsFile = "hdfs://dbiir10:9000/pixels/pixels/test_105/20181117213047_3239.pxl";
        Storage storage = StorageFactory.Instance().getStorage("hdfs");

        PixelsReader pixelsReader = null;
        try {
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(pixelsFile)
                    .build();
            System.out.println(pixelsReader.getRowGroupNum());
            System.out.println(pixelsReader.getRowGroupInfo(0).toString());
            System.out.println(pixelsReader.getRowGroupInfo(1).toString());
            if (pixelsReader.getFooter().getRowGroupStatsList().size() != 1) {
                System.out.println("Path: " + pixelsFile + ", RGNum: " + pixelsReader.getRowGroupNum());
            }

            pixelsReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWritePixelsFile() throws IOException
    {
        String pixelsFile = "hdfs://dbiir10:9000//pixels/pixels/test_105/v_0_order/.pxl";
        Storage storage = StorageFactory.Instance().getStorage("hdfs");

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        String schemaStr = "struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>";

        try {
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) rowBatch.cols[0];              // int
            DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[1];          // float
            DoubleColumnVector c = (DoubleColumnVector) rowBatch.cols[2];          // double
            TimestampColumnVector d = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
            LongColumnVector e = (LongColumnVector) rowBatch.cols[4];              // boolean
            BinaryColumnVector z = (BinaryColumnVector) rowBatch.cols[5];            // string

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64 * 1024 * 1024)
                            .setStorage(storage)
                            .setFilePath(pixelsFile)
                            .setBlockSize(256 * 1024 * 1024)
                            .setReplication((short) 3)
                            .setBlockPadding(true)
                            .setEncoding(true)
                            .setCompressionBlockSize(1)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
            for (int i = 0; i < 1; i++) {
                int row = rowBatch.size++;
                a.vector[row] = i;
                a.isNull[row] = false;
                b.vector[row] = Float.floatToIntBits(i * 3.1415f);
                b.isNull[row] = false;
                c.vector[row] = Double.doubleToLongBits(i * 3.14159d);
                c.isNull[row] = false;
                d.set(row, timestamp);
                d.isNull[row] = false;
                e.vector[row] = i > 25000 ? 1 : 0;
                e.isNull[row] = false;
                z.setVal(row, String.valueOf(i).getBytes());
                z.isNull[row] = false;
                if (rowBatch.size == rowBatch.getMaxSize()) {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }

            if (rowBatch.size != 0) {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }

            pixelsWriter.close();
        } catch (IOException | PixelsWriterException e) {
            e.printStackTrace();
        }
    }

}
