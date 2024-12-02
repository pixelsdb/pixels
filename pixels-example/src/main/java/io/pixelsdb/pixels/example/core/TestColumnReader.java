/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.pixelsdb.pixels.core.predicate.PixelsPredicate.TRUE_PREDICATE;

public class TestColumnReader
{
    public static void main(String[] args) throws IOException
    {
        String pixelsFile = "/home/pixels/data/tpch_5g/test/test.pxl";
        Storage storage = StorageFactory.Instance().getStorage("file");
        String schemaStr = "struct<a:boolean,b:date,c:decimal(5,2),d:double,e:float,f:int,g:decimal(30,20),h:string,i:time,j:timestamp,k:vector>";

        try
        {
            // delete pixel file
            Files.deleteIfExists(Paths.get(pixelsFile));

            // write pixel file
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatchWithHiddenColumn();
            ByteColumnVector a = (ByteColumnVector) rowBatch.cols[0]; // boolean
            DateColumnVector b = (DateColumnVector) rowBatch.cols[1]; // date
            DecimalColumnVector c = (DecimalColumnVector) rowBatch.cols[2]; // decimal
            DoubleColumnVector d = (DoubleColumnVector) rowBatch.cols[3]; // double
            FloatColumnVector e = (FloatColumnVector) rowBatch.cols[4]; // float
            LongColumnVector f = (LongColumnVector) rowBatch.cols[5]; // int
            LongDecimalColumnVector g = (LongDecimalColumnVector) rowBatch.cols[6]; // long decimal
            BinaryColumnVector h = (BinaryColumnVector) rowBatch.cols[7]; // string
            TimeColumnVector m = (TimeColumnVector) rowBatch.cols[8]; // time
            TimestampColumnVector j = (TimestampColumnVector) rowBatch.cols[9]; // timestamp
            VectorColumnVector k = (VectorColumnVector) rowBatch.cols[10]; // vector
            LongColumnVector l = (LongColumnVector) rowBatch.cols[11]; // long

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setHasHiddenColumn(true)
                            .setPixelStride(10000)
                            .setRowGroupSize(64 * 1024 * 1024)
                            .setStorage(storage)
                            .setPath(pixelsFile)
                            .setBlockSize(256 * 1024 * 1024)
                            .setReplication((short) 3)
                            .setBlockPadding(true)
                            .setEncodingLevel(EncodingLevel.EL2)
                            .setCompressionBlockSize(1)
                            .setNullsPadding(true)
                            .build();

            for (int i = 0; i < 10; i++)
            {
                int row = rowBatch.size++;
                a.vector[row] = (byte) (i % 2);
                a.isNull[row] = false;
                b.isNull[row] = true;
                c.vector[row] = 10000 + i;
                c.isNull[row] = false;
                d.isNull[row] = true;
                e.vector[row] = 10000 + i;
                e.isNull[row] = false;
                f.isNull[row] = true;
                g.vector[row << 1] = 0;
                g.vector[(row << 1) + 1] = 10000 + i;
                g.isNull[row] = false;
                h.isNull[row] = true;
                m.set(row, 1000 * i);
                m.isNull[row] = false;
                j.isNull[row] = true;
                k.setRef(row, new double[]{i + 0.1, i + 0.2});
                k.isNull[row] = false;
                l.vector[row] = 100 - i;
                l.isNull[row] = false;
            }
            for (int i = 10; i < 20; i++)
            {
                int row = rowBatch.size++;
                a.isNull[row] = true;
                b.dates[row] = 1000 + i;
                b.isNull[row] = false;
                c.isNull[row] = true;
                d.vector[row] = 1000 + i;
                d.isNull[row] = false;
                e.isNull[row] = true;
                f.vector[row] = 1000 + i;
                f.isNull[row] = false;
                g.isNull[row] = true;
                h.setVal(row, String.valueOf(i).getBytes());
                h.isNull[row] = false;
                m.isNull[row] = true;
                j.set(row, 1000 * i);
                j.isNull[row] = false;
//                k.setRef(row, new double[]{i + 0.1, i + 0.2});
//                k.isNull[row] = false;
                k.isNull[row] = true;
                l.vector[row] = 100 - i;
                l.isNull[row] = false;
            }
            for (int i = 20; i < 30; i++)
            {
                int row = rowBatch.size++;
                a.vector[row] = (byte) 1;
                a.isNull[row] = false;
                b.dates[row] = 1000 + i;
                b.isNull[row] = false;
                c.vector[row] = 10000 + i;
                c.isNull[row] = false;
                d.vector[row] = 1000 + i;
                d.isNull[row] = false;
                e.vector[row] = 10000 + i;
                e.isNull[row] = false;
                f.vector[row] = 1000 + i;
                f.isNull[row] = false;
                g.vector[row << 1] = 0;
                g.vector[(row << 1) + 1] = 10000 + i;
                g.isNull[row] = false;
                h.setVal(row, String.valueOf(i).getBytes());
                h.isNull[row] = false;
                m.set(row, 1000 * i);
                m.isNull[row] = false;
                j.set(row, 1000 * i);
                j.isNull[row] = false;
                k.setRef(row, new double[]{i + 0.1, i + 0.2});
                k.isNull[row] = false;
                l.vector[row] = 100 - i;
                l.isNull[row] = false;
            }
            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + pixelsFile);
                rowBatch.reset();
            }
            pixelsWriter.close();

            // read pixel file
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(pixelsFile)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            String[] cols = new String[11];
            cols[0] = reader.getFileSchema().getFieldNames().get(0);
            cols[1] = reader.getFileSchema().getFieldNames().get(1);
            cols[2] = reader.getFileSchema().getFieldNames().get(2);
            cols[3] = reader.getFileSchema().getFieldNames().get(3);
            cols[4] = reader.getFileSchema().getFieldNames().get(4);
            cols[5] = reader.getFileSchema().getFieldNames().get(5);
            cols[6] = reader.getFileSchema().getFieldNames().get(6);
            cols[7] = reader.getFileSchema().getFieldNames().get(7);
            cols[8] = reader.getFileSchema().getFieldNames().get(8);
            cols[9] = reader.getFileSchema().getFieldNames().get(9);
            cols[10] = reader.getFileSchema().getFieldNames().get(10);
            PixelsReaderOption option = new PixelsReaderOption();
            option.transId(0);
            option.transTimestamp(85);
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            option.predicate(TRUE_PREDICATE);
            PixelsRecordReader recordReader = reader.read(option);
            int batchSize = 11;
            VectorizedRowBatch resultBatch;
            int len = 0;
            int numRows = 0;
            int numBatches = 0;
            while (true) {
                resultBatch = recordReader.readBatch(batchSize);
                System.out.println("rowBatch: " + resultBatch);
                numBatches++;
                String result = resultBatch.toString();
                len += result.length();
                System.out.println("loop:" + numBatches + ", rowBatchSize:" + resultBatch.size);
                if (resultBatch.endOfFile) {
                    numRows += resultBatch.size;
                    break;
                }
                numRows += resultBatch.size;
            }
            reader.close();
        } catch (IOException | PixelsWriterException e)
        {
            e.printStackTrace();
        }

        // delete pixel file
        Files.deleteIfExists(Paths.get(pixelsFile));
    }
}
