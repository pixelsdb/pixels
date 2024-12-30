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
package io.pixelsdb.pixels.core;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.util.List;

public class TestStreamReaderAndWriter
{
    static ByteColumnVector byteColumnVector = new ByteColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    static IOException readerException = null;
    static long batchNum = 1024*15;
    static int varCharMaxSize = 199;
    static int charMaxSize = 55;
    static boolean nullsPadding = true;
    static TypeDescription schema;
    static LongColumnVector intColumnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    static LongColumnVector longColumnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    static DecimalColumnVector decimalColumnVector = new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE, 15, 2);
    static BinaryColumnVector varCharColumnVector = new BinaryColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    static BinaryColumnVector charColumnVector = new BinaryColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    static DateColumnVector dateColumnVector = new DateColumnVector(VectorizedRowBatch.DEFAULT_SIZE);

    static
    {
        // create schema
        schema = TypeDescription.createStruct();
        schema.addField("a", TypeDescription.createInt());
        schema.addField("b", TypeDescription.createLong());
        schema.addField("c", TypeDescription.createDecimal(15, 2));
        schema.addField("d", TypeDescription.createVarchar(varCharMaxSize));
        schema.addField("e", TypeDescription.createVarchar(varCharMaxSize));
        schema.addField("f", TypeDescription.createChar(charMaxSize));
        schema.addField("g", TypeDescription.createChar(charMaxSize));
        schema.addField("h", TypeDescription.createVarchar(varCharMaxSize));
        schema.addField("i", TypeDescription.createLong());
        schema.addField("j", TypeDescription.createDate());

        // create some columns
        for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
        {
            if (i % 100 == 0)
            {
                intColumnVector.addNull();
                longColumnVector.addNull();
                decimalColumnVector.addNull();
                dateColumnVector.addNull();
            } else
            {
                intColumnVector.add(i%100 > 25 ? 111 : -111);
                longColumnVector.add(i%100 > 25 ? 8L*1024*1024*1024 : -8L*1024*1024*1024);
                decimalColumnVector.add(11111.22);
                dateColumnVector.add(new Date(1000));
            }
        }


        // create char columns
        for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
        {
            if (i % 100 == 0)
            {
                varCharColumnVector.addNull();
                charColumnVector.addNull();
            } else
            {
                int len = i%varCharMaxSize + 1;
                char[] charArray = new char[len];
                for (int k = 0; k < len; k++)
                {
                    charArray[k] = (char) k;
                }
                String str = new String(charArray);
                varCharColumnVector.add(str);

                len = i%charMaxSize + 1;
                charArray = new char[len];
                for (int k = 0; k < len; k++)
                {
                    charArray[k] = (char) k;
                }
                str = new String(charArray);
                charColumnVector.add(str);
            }
        }
    }

    @Test
    public void testStreamReaderAndWriter() throws IOException
    {
        Thread thread1 = new Thread(runReader());

        Thread thread2 = new Thread(runWriter());
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

    private static Runnable runWriter()
    {
        return () -> {
            PixelsWriter writer;
            try
            {
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
                String path = "stream://localhost:29920";

                writer = PixelsWriterStreamImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(path)
                        .setSchema(schema)
                        .setPixelStride(10000)
                        .setRowGroupSize(1024*512)
                        .setEncodingLevel(EncodingLevel.EL0)
                        .setPartitioned(false)
                        .setNullsPadding(nullsPadding)
                        .build();

                VectorizedRowBatch rowBatch = schema.createRowBatch();
                for (int i = 0; i < batchNum; i++)
                {
                    fillColumns(rowBatch, writer.getSchema().getChildren());
                    if (rowBatch.size != 0)
                    {
                        writer.addRowBatch(rowBatch);
                        rowBatch.reset();
                    }
                }

                writer.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        };
    }

    private static Runnable runReader()
    {
        return () -> {
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
                PixelsReaderOption option = isHeaderRight(fileSchema);
                PixelsRecordReader recordReader = reader.read(option);

                // read row group
                VectorizedRowBatch rowBatch = null;
                try
                {
                    rowBatch = recordReader.readBatch();
                } catch (IOException e)
                {
                    readerException = new IOException("wrong row batch");
                }

                while (rowBatch != null && rowBatch.size > 0)
                {
                    for (int i = 0; i < rowBatch.cols.length; i++)
                    {
                        compareColumn(i, rowBatch);
                    }

                    rowBatch = recordReader.readBatch();
                }

                reader.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        };
    }

    private static PixelsReaderOption isHeaderRight(TypeDescription fileSchema)
    {
        assert fileSchema.getCategory() == schema.getCategory();
        assert fileSchema.getChildren().size() == schema.getChildren().size();
        List<TypeDescription> types = fileSchema.getChildren();
        for (int i = 0; i < types.size(); i++)
        {
            assert types.get(i).equals(schema.getChildren().get(i));
        }

        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        String[] includeColumns = new String[types.size()];
        for (int i = 0; i < types.size(); i++)
        {
            includeColumns[i] = fileSchema.getFieldNames().get(i);
        }
        option.includeCols(includeColumns);

        return option;
    }

    private static void compareColumn(int colIdx, VectorizedRowBatch rowBatch)
    {
        ColumnVector column = rowBatch.cols[colIdx];
        if (column instanceof LongColumnVector &&
                schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.INT)
        {
            for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
            {
                assert column.noNulls == intColumnVector.noNulls;
                assert column.isNull[i] == intColumnVector.isNull[i];
                assert !intColumnVector.noNulls && column.isNull[i] ||
                        ((LongColumnVector) column).vector[i] == intColumnVector.vector[i];
            }
        } else if (column instanceof LongColumnVector )
        {
            for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
            {
                assert column.noNulls == longColumnVector.noNulls;
                assert column.isNull[i] == longColumnVector.isNull[i];
                assert !longColumnVector.noNulls && column.isNull[i] ||
                        ((LongColumnVector) column).vector[i] == longColumnVector.vector[i];
            }
        } else if (column instanceof DecimalColumnVector)
        {
            for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
            {
                assert column.noNulls == decimalColumnVector.noNulls;
                assert column.isNull[i] == decimalColumnVector.isNull[i];
                assert !decimalColumnVector.noNulls && column.isNull[i] ||
                        ((DecimalColumnVector) column).vector[i] == decimalColumnVector.vector[i];
            }
        } else if (column instanceof BinaryColumnVector &&
                schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.VARCHAR)
        {
            for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
            {
                assert column.noNulls == varCharColumnVector.noNulls;
                assert column.isNull[i] == varCharColumnVector.isNull[i];
                if (varCharColumnVector.noNulls || !column.isNull[i])
                {
                    String s1 = new String(varCharColumnVector.vector[i], varCharColumnVector.start[i], varCharColumnVector.lens[i]);
                    String s2 = new String(((BinaryColumnVector) column).vector[i],
                            ((BinaryColumnVector) column).start[i], ((BinaryColumnVector) column).lens[i]);
                    if (!s1.equals(s2))
                    {
                        System.out.println(s1 + " != " + s2);
                    }
                    assert s1.equals(s2);
                }
            }
        } else if (column instanceof BinaryColumnVector &&
                schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.CHAR)
        {
            for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
            {
                assert column.noNulls == charColumnVector.noNulls;
                assert column.isNull[i] == charColumnVector.isNull[i];
                if (charColumnVector.noNulls || !column.isNull[i])
                {
                    String s1 = new String(charColumnVector.vector[i], charColumnVector.start[i], charColumnVector.lens[i]);
                    String s = new String(((BinaryColumnVector) column).vector[i],
                            ((BinaryColumnVector) column).start[i], ((BinaryColumnVector) column).lens[i]);
                    assert s1.equals(s);
                }
            }
        } else if (column instanceof DateColumnVector &&
                schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.DATE)
        {
            for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++)
            {
                assert column.noNulls == dateColumnVector.noNulls;
                assert column.isNull[i] == dateColumnVector.isNull[i];
                assert !dateColumnVector.noNulls && column.isNull[i] ||
                        ((DateColumnVector) column).dates[i] == dateColumnVector.dates[i];
            }
        }
    }

    private static void fillColumns(VectorizedRowBatch rowBatch, List<TypeDescription> types)
    {
        for (int colIdx = 0; colIdx < rowBatch.cols.length; colIdx++)
        {
            ColumnVector column = rowBatch.cols[colIdx];
            if (types.get(colIdx).getCategory().equals(TypeDescription.Category.INT))
            {
                duplicate(column, intColumnVector);
            } else if (types.get(colIdx).getCategory().equals(TypeDescription.Category.LONG))
            {
                duplicate(column, longColumnVector);
            } else if (column instanceof DecimalColumnVector)
            {
                duplicate(column, decimalColumnVector);
            } else if (column instanceof BinaryColumnVector &&
                    schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.VARCHAR)
            {
//                column.duplicate(varCharColumnVector);
                duplicate(column, varCharColumnVector);
            } else if (column instanceof BinaryColumnVector &&
                    schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.CHAR)
            {
//                column.duplicate(charColumnVector);
                duplicate(column, charColumnVector);
            } else if (column instanceof DateColumnVector &&
                    schema.getChildren().get(colIdx).getCategory() == TypeDescription.Category.DATE)
            {
                duplicate(column, dateColumnVector);
            }
        }
        rowBatch.size += VectorizedRowBatch.DEFAULT_SIZE;
    }
    
    private static void duplicate(ColumnVector dst, ColumnVector src)
    {
        assert dst.getClass() == src.getClass();
        if (dst instanceof IntColumnVector)
        {
            dst.setWriteIndex(src.getWriteIndex());
            dst.noNulls = src.noNulls;

            System.arraycopy(((IntColumnVector) src).vector, 0,
                    ((IntColumnVector) dst).vector, 0, ((IntColumnVector) src).vector.length);
            System.arraycopy(src.isNull, 0,
                    dst.isNull, 0, src.isNull.length);
        } else if (dst instanceof LongColumnVector)
        {
            dst.setWriteIndex(src.getWriteIndex());
            dst.noNulls = src.noNulls;
            System.arraycopy(((LongColumnVector) src).vector, 0,
                    ((LongColumnVector) dst).vector, 0, ((LongColumnVector) src).vector.length);
            System.arraycopy(src.isNull, 0,
                    dst.isNull, 0, src.isNull.length);
        } else if (dst instanceof DecimalColumnVector)
        {
            dst.setWriteIndex(src.getWriteIndex());
            dst.noNulls = src.noNulls;
            System.arraycopy(((DecimalColumnVector) src).vector, 0,
                    ((DecimalColumnVector) dst).vector, 0, ((DecimalColumnVector) src).vector.length);
            System.arraycopy(src.isNull, 0,
                    dst.isNull, 0, src.isNull.length);
        } else if (dst instanceof BinaryColumnVector)
        {
            dst.duplicate(src);
            boolean[] isNull = new boolean[src.isNull.length];
            dst.isNull = isNull;
            System.arraycopy(src.isNull, 0, dst.isNull, 0, src.isNull.length);
        } else if (dst instanceof DateColumnVector)
        {
            dst.setWriteIndex(src.getWriteIndex());
            dst.noNulls = src.noNulls;
            System.arraycopy(((DateColumnVector) src).dates, 0,
                    ((DateColumnVector) dst).dates, 0, ((DateColumnVector) src).dates.length);
            System.arraycopy(src.isNull, 0, dst.isNull, 0, src.isNull.length);
        }
    }
}
