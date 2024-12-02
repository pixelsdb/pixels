package io.pixelsdb.pixels.core;/*
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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public class TestStreamReaderAndWriter
{
    private IOException readerException = null;
    private final long rowBatchNum = 8L*1024*1024*1024;

    @Test
    public void testStreamReaderAndWriter() throws IOException
    {
        Thread thread1 = new Thread(() -> {
            String path = "stream://localhost:29920";
            PixelsReader reader;

            try
            {
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
                reader = PixelsReaderStreamImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(path)
                        .build();
                TypeDescription fileSchema = reader.getFileSchema();
                List<TypeDescription> types = fileSchema.getChildren();
                if (!types.get(0).toString().equals("integer")
                    || !types.get(1).toString().equals("float")
                    || !types.get(2).toString().equals("double")
                    || !types.get(3).toString().equals("timestamp(3)")
                    || !types.get(4).toString().equals("boolean")
                    || !types.get(5).toString().equals("date")
                    || !types.get(6).toString().equals("time(3)")
                    || !types.get(7).toString().equals("string")
                    || !types.get(8).toString().equals("decimal(6,3)")
                    || !types.get(9).toString().equals("decimal(37,6)"))
                {
                    readerException = new IOException("wrong header");
                }
//                for (TypeDescription type: types)
//                {
//                    System.out.println(type.toString());
//                }


                // read row group
                PixelsReaderOption option = new PixelsReaderOption();
                option.skipCorruptRecords(true);
                option.tolerantSchemaEvolution(true);
                option.includeCols(new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"});
                PixelsRecordReader recordReader = reader.read(option);

                VectorizedRowBatch rowBatch = null;
                try
                {
                    rowBatch = recordReader.readBatch();
                } catch (IOException e)
                {
                    readerException = new IOException("wrong row batch");
                }

                int curRow = 0;
                while (rowBatch != null && rowBatch.size > 0)
                {
                    LongColumnVector va = (LongColumnVector) rowBatch.cols[0];              // int
                    FloatColumnVector vb = (FloatColumnVector) rowBatch.cols[1];            // float
                    DoubleColumnVector vc = (DoubleColumnVector) rowBatch.cols[2];          // double
                    TimestampColumnVector vd = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
                    ByteColumnVector ve = (ByteColumnVector) rowBatch.cols[4];              // boolean
                    DateColumnVector vf = (DateColumnVector) rowBatch.cols[5];              // date
                    TimeColumnVector vg = (TimeColumnVector) rowBatch.cols[6];              // time
                    BinaryColumnVector vh = (BinaryColumnVector) rowBatch.cols[7];          // string
                    DecimalColumnVector vi = (DecimalColumnVector) rowBatch.cols[8];        // decimal
                    LongDecimalColumnVector vj = (LongDecimalColumnVector) rowBatch.cols[9];// long decimal

                    for (int i = 0; i < rowBatch.size; curRow++, i++)
                    {
                        if (curRow % 100 == 0)
                        {
                            if (!va.isNull[i] || !vb.isNull[i] || !vc.isNull[i] || !vd.isNull[i] || !ve.isNull[i]
                                || !vf.isNull[i] || !vg.isNull[i] || !vh.isNull[i] || !vi.isNull[i] || !vj.isNull[i])
                            {
                                System.out.println("is null not right");
                                readerException = new IOException();
//                                break;
                            }
                        } else
                        {
                            if (va.vector[i] != curRow || va.isNull[i])
                            {
                                readerException = new IOException("va not right: " + va.vector[i] + " != " + curRow);
                                System.out.println("va not right: " + va.vector[i] + " != " + curRow);
//                                break;
                            }
                            if (vb.vector[i] != Float.floatToIntBits(curRow * 3.1415f) || vb.isNull[i])
                            {
                                readerException = new IOException("vb not right: " + vb.vector[i] + " != "
                                        + Float.floatToIntBits(curRow * 3.1415f));
                                System.out.println("vb not right: " + vb.vector[i] + " != "
                                        + Float.floatToIntBits(curRow * 3.1415f));
//                                break;
                            }
                            if (vc.vector[i] != Double.doubleToLongBits(curRow * 3.14159d) || vc.isNull[i])
                            {
                                readerException = new IOException("vc not right: " + vc.vector[i] + " != "
                                        + Double.doubleToLongBits(curRow * 3.14159d));
                                System.out.println("vc not right: " + vc.vector[i] + " != "
                                        + Double.doubleToLongBits(curRow * 3.14159d));
//                                break;
                            }
                            if (vd.compareTo(i, new Timestamp(1000000)) != 0 || vd.isNull[i])
                            {
                                readerException = new IOException("vd not right: " + vd.getMicros(i) + " != "
                                        + "1000000");
                                System.out.println("vd not right: " + vd.getMicros(i) + " != "
                                        + "1000000");
                                break;
                            }
                            if (ve.vector[i] != (byte) (curRow % 100 > 25 ? 1 : 0) || ve.isNull[i])
                            {
                                System.out.println("ve " + curRow + " not right: " + ve.vector[i] + " != "
                                        + (byte) (curRow % 100 > 25 ? 1 : 0));
                                readerException = new IOException("ve not right: " + ve.vector[i] + " != "
                                + (byte) (curRow % 100 > 25 ? 1 : 0));
//                                break;
                            }
                            if (vf.compareTo(i, new Date(1000)) != 0 || vf.isNull[i])
                            {
                                readerException = new IOException("vf not right: " + vf.getDate(i) + " != "
                                + 1000);
                                System.out.println("vf not right: " + vf.getDate(i) + " != "
                                        + 1000);
//                                break;
                            }
                            if (vg.compareTo(i, new Time(1000)) != 0 || vg.isNull[i])
                            {
                                readerException = new IOException("vg not right: " + vg.getTime(i) + " != "
                                + 1000);
                                System.out.println("vg not right: " + vg.getTime(i) + " != "
                                        + 1000);
//                                break;
                            }
                            if (!(vh.toString(i).equals(String.valueOf(curRow))) || vh.isNull[i])
                            {
                                readerException = new IOException("vh not right: " + vh.toString(i) + " != "
                                + String.valueOf(curRow));
                                System.out.println("vh not right: " + vh.toString(i) + " != "
                                        + String.valueOf(curRow));
//                                break;
                            }
                            if (vi.vector[i] != curRow || vi.isNull[i])
                            {
                                readerException = new IOException("vi not right: " + vi.vector[i] + " != "
                                + curRow);
                                System.out.println("vi not right: " + vi.vector[i] + " != "
                                        + curRow);
//                                break;
                            }
                            if (vj.vector[i*2] != curRow || vj.vector[i*2+1] != curRow || vj.isNull[i])
                            {
                                readerException = new IOException("vj not right: " + vj.vector[i*2] + " != "
                                + curRow + ", " + vj.vector[i*2+1] + " != " + curRow);
                                System.out.println("vj not right: " + vj.vector[i*2] + " != "
                                        + curRow + ", " + vj.vector[i*2+1] + " != " + curRow);
//                                break;
                            }
//                            if (va.vector[i] != curRow || va.isNull[i]
//                                || vb.vector[i] != Float.floatToIntBits(curRow * 3.1415f) || vb.isNull[i]
//                                || vc.vector[i] != Double.doubleToLongBits(curRow * 3.14159d) || vc.isNull[i]
//                                || vd.compareTo(i, new Timestamp(1000000)) != 0 || vd.isNull[i]
//                                || ve.vector[i] != (byte) (curRow % 100 > 25 ? 1 : 0) || ve.isNull[i]
//                                || vf.compareTo(i, new Date(1000)) != 0 || vf.isNull[i]
//                                || vg.compareTo(i, new Time(1000)) != 0 || vg.isNull[i]
//                                || !(vh.toString(i).equals(String.valueOf(curRow))) || vh.isNull[i]
//                                || vi.vector[i] != curRow || vi.isNull[i]
//                                || vj.vector[i*2] != curRow || vj.vector[i*2+1] != curRow || vj.isNull[i])
//                            {
////                                String a = vh.toString(i);
////                                String b = String.valueOf(curRow);
////                                System.out.println("len of a: " + a.length());
////                                System.out.println("len of b: " + b.length());
////                                System.out.println(a.equals("1"));
////                                System.out.println(b.equals("1"));
////                                System.out.println(a.equals(b));
////                                System.out.println(vh.toString(i).equals(String.valueOf(curRow)));
//                                readerException = new IOException();
//                                break;
//                            }
                        }
                    }


                    rowBatch = recordReader.readBatch();
                }

                reader.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        });

        Thread thread2 = new Thread(() -> {
            // Read from file
            String path = "file:///home/hsy/Downloads/nation.pxl";
            PixelsWriter writer;

            try
            {
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
                PixelsReader reader;
                reader = PixelsReaderImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(path)
                        .setEnableCache(false)
                        .setPixelsFooterCache(new PixelsFooterCache())
                        .build();
                PixelsReaderOption option = new PixelsReaderOption();
                option.skipCorruptRecords(true);
                option.tolerantSchemaEvolution(true);
                option.enableEncodedColumnVector(false);
                String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
                option.includeCols(colNames);
                PixelsRecordReader recordReader = reader.read(option);


                storage = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
                path = "stream://localhost:29920";


                // create row batch and send
                TypeDescription schema = TypeDescription.createStruct();
                schema.addField("a", TypeDescription.createInt());
                schema.addField("b", TypeDescription.createFloat());
                schema.addField("c", TypeDescription.createDouble());
                schema.addField("d", TypeDescription.createTimestamp(3));
                schema.addField("e", TypeDescription.createBoolean());
                schema.addField("f", TypeDescription.createDate());
                schema.addField("g", TypeDescription.createTime(3));
                schema.addField("h", TypeDescription.createString());
                schema.addField("i", TypeDescription.createDecimal(6, 3));
                schema.addField("j", TypeDescription.createDecimal(37, 6));
                writer = PixelsWriterStreamImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(path)
                        .setSchema(schema)
                        .setPixelStride(10000)
                        .setRowGroupSize(1024*512)
                        .setEncodingLevel(EncodingLevel.EL2)
                        .setPartitioned(false)
                        .build();

                VectorizedRowBatch rowBatch = schema.createRowBatch();
                LongColumnVector va = (LongColumnVector) rowBatch.cols[0];              // int
                FloatColumnVector vb = (FloatColumnVector) rowBatch.cols[1];            // float
                DoubleColumnVector vc = (DoubleColumnVector) rowBatch.cols[2];          // double
                TimestampColumnVector vd = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
                ByteColumnVector ve = (ByteColumnVector) rowBatch.cols[4];              // boolean
                DateColumnVector vf = (DateColumnVector) rowBatch.cols[5];              // date
                TimeColumnVector vg = (TimeColumnVector) rowBatch.cols[6];              // time
                BinaryColumnVector vh = (BinaryColumnVector) rowBatch.cols[7];          // string
                DecimalColumnVector vi = (DecimalColumnVector) rowBatch.cols[8];        // decimal
                LongDecimalColumnVector vj = (LongDecimalColumnVector) rowBatch.cols[9];// long decimal

                long curT = 1000000;
                Timestamp timestamp = new Timestamp(curT);

                for (int i = 0; i < TestParams.rowNum; i++)
                {
                    int row = rowBatch.size++;
                    if (i % 100 == 0)
                    {
                        va.isNull[row] = true;
                        va.vector[row] = 0;
                        vb.isNull[row] = true;
                        vb.vector[row] = 0;
                        vc.isNull[row] = true;
                        vc.vector[row] = 0;
                        vd.isNull[row] = true;
                        vd.times[row] = 0;
                        ve.isNull[row] = true;
                        ve.vector[row] = 0;
                        vf.isNull[row] = true;
                        vf.dates[row] = 0;
                        vg.isNull[row] = true;
                        vg.times[row] = 0;
                        vh.isNull[row] = true;
                        vh.vector[row] = new byte[0];
                        vi.isNull[row] = true;
                        vi.vector[row] = 0;
                        vj.isNull[row] = true;
                        vj.vector[row*2] = 0;
                        vj.vector[row*2+1] = 0;
                    }
                    else
                    {
                        va.vector[row] = i;
                        va.isNull[row] = false;
                        vb.vector[row] = Float.floatToIntBits(i * 3.1415f);
                        vb.isNull[row] = false;
                        vc.vector[row] = Double.doubleToLongBits(i * 3.14159d);
                        vc.isNull[row] = false;
                        vd.set(row, timestamp);
                        vd.isNull[row] = false;
                        ve.vector[row] = (byte) (i % 100 > 25 ? 1 : 0);
                        ve.isNull[row] = false;
                        vf.set(row, new Date(1000));
                        vf.isNull[row] = false;
                        vg.set(row, new Time(1000));
                        vg.isNull[row] = false;
                        vh.setVal(row, String.valueOf(i).getBytes());
                        vh.isNull[row] = false;
                        vi.vector[row] = i;
                        vi.isNull[row] = false;
                        vj.vector[row*2] = i;
                        vj.vector[row*2+1] = i;
                        vj.isNull[row] = false;
                    }
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

//                System.out.println(vi.getPrecision());
//                System.out.println(vi.getScale());
//                System.out.println(vj.getPrecision());
//                System.out.println(vj.getScale());
//                System.out.println("writer succeed to build");
                writer.close();
//                System.out.println("writer succeed to close");
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        });
        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (readerException != null)
        {
            System.out.println(readerException.getMessage());
            throw new IOException();
        }
    }
}
