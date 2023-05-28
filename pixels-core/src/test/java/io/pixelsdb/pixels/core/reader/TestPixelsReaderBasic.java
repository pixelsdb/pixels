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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.vector.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * pixels reader basic test
 * this test is to guarantee basic correctness of the pixels reader
 *
 * @author guodong
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestPixelsReaderBasic
{
    private static final boolean DEBUG = true;
    private long elementSize = 0;

    @Test
    public void testMetadata()
    {
        String path = "file:///home/hank/Downloads/pixels/20220306043329_1.pxl";
        PixelsReader reader;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("file");
            reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            List<PixelsProto.Type> types = reader.getFooter().getTypesList();
            for (PixelsProto.Type type : types)
            {
                System.out.println(type);
            }
            System.out.println(reader.getRowGroupStats().size());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void testReadDictionary()
    {
        String path = "file:///home/hank/20230126155625_0.pxl";
        PixelsReader reader;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("file");
            reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.enableEncodedColumnVector(true);
            option.includeCols(new String[]{"o_orderpriority"});
            option.rgRange(0, 1);
            option.transId(1);
            PixelsRecordReader recordReader = reader.read(option);
            VectorizedRowBatch rowBatch = recordReader.readBatch(1000);
            DictionaryColumnVector vector = (DictionaryColumnVector) rowBatch.cols[0];
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void test0SmallNull()
    {
        String fileName = "test-small-null.pxl";
        int rowNum = 200_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum, 1528785092538L, true);
        }
    }

    @Test
    public void test1MidNull()
    {
        String fileName = "test-mid-null.pxl";
        int rowNum = 2_000_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum, 1528901945696L, true);
        }
    }

    @Test
    public void test2LargeNull()
    {
        String fileName = "test-large-null.pxl";
        int rowNum = 20_000_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(200_000);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum, 1528902023606L, true);
        }
    }

    @Test
    public void test3Small()
    {
        String fileName = "test-small.pxl";
        int rowNum = 200_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum, 1529129883948L, false);
        }
    }

    @Test
    public void test4Mid()
    {
        String fileName = "test-mid.pxl";
        int rowNum = 2_000_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum, 1529130997320L, false);
        }
    }

    private void testContent(String fileName, int batchSize, int rowNum, long time, boolean hasNull)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"a", "b", "c", "d", "e", "z"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        elementSize = 0;
        try (PixelsReader pixelsReader = getReader(fileName);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
                DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
                DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
                TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
                LongColumnVector ecv = (LongColumnVector) rowBatch.cols[4];
                BinaryColumnVector zcv = (BinaryColumnVector) rowBatch.cols[5];
                if (rowBatch.endOfFile)
                {
                    if (hasNull)
                    {
                        assertNullCorrect(rowBatch, acv, bcv, ccv, dcv, ecv, zcv, time);
                    }
                    else
                    {
                        assertCorrect(rowBatch, acv, bcv, ccv, dcv, ecv, zcv, time);
                    }
                    break;
                }
                if (hasNull)
                {
                    assertNullCorrect(rowBatch, acv, bcv, ccv, dcv, ecv, zcv, time);
                }
                else
                {
                    assertCorrect(rowBatch, acv, bcv, ccv, dcv, ecv, zcv, time);
                }
            }
            assertEquals(rowNum, elementSize);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void assertNullCorrect(VectorizedRowBatch rowBatch,
                                   LongColumnVector acv,
                                   DoubleColumnVector bcv,
                                   DoubleColumnVector ccv,
                                   TimestampColumnVector dcv,
                                   LongColumnVector ecv,
                                   BinaryColumnVector zcv,
                                   long time)
    {
        for (int i = 0; i < rowBatch.size; i++)
        {
            if (elementSize % 100 == 0)
            {
                if (DEBUG)
                {
                    if (!acv.isNull[i])
                    {
                        System.out.println("[a] size: " + elementSize + ", non null");
                    }
                    if (!bcv.isNull[i])
                    {
                        System.out.println("[b] size: " + elementSize + ", non null");
                    }
                    if (!ccv.isNull[i])
                    {
                        System.out.println("[c] size: " + elementSize + ", non null");
                    }
                    if (!dcv.isNull[i])
                    {
                        System.out.println("[d] size: " + elementSize + ", non null");
                    }
                    if (!ecv.isNull[i])
                    {
                        System.out.println("[e] size: " + elementSize + ", non null");
                    }
                    if (!zcv.isNull[i])
                    {
                        System.out.println("[z] size: " + elementSize + ", non null");
                    }
                }
                else
                {
                    assertTrue(acv.isNull[i]);
                    assertTrue(bcv.isNull[i]);
                    assertTrue(ccv.isNull[i]);
                    assertTrue(dcv.isNull[i]);
                    assertTrue(ecv.isNull[i]);
                    assertTrue(zcv.isNull[i]);
                }
            }
            else
            {
                if (DEBUG)
                {
                    if (elementSize != acv.vector[i])
                    {
                        System.out.println("[a] size: " + elementSize
                                + ", expected: " + elementSize + ", actual: " + acv.vector[i]);
                    }
                    if (Float.compare(elementSize * 3.1415f, (float) bcv.vector[i]) != 0)
                    {
                        System.out.println("[b] size: " + elementSize
                                + ", expected: " + elementSize * 3.1415f + ", actual: " + (float) bcv.vector[i]);
                    }
                    if (Math.abs(elementSize * 3.14159d - ccv.vector[i]) > 0.000001)
                    {
                        System.out.println("[c] size: " + elementSize
                                + ", expected: " + elementSize * 3.14159d + ", actual: " + ccv.vector[i]);
                    }
                    if (dcv.times[i] != time)
                    {
                        System.out.println("[d] size: " + elementSize
                                + ", expected: " + time + ", actual: " + dcv.times[i]);
                    }
                    int expectedBool = elementSize > 25 ? 1 : 0;
                    if (expectedBool != ecv.vector[i])
                    {
                        System.out.println("[e] size: " + elementSize
                                + ", expected: " + expectedBool + ", actual: " + ecv.vector[i]);
                    }
                    String actualStr = new String(zcv.vector[i], zcv.start[i], zcv.lens[i]);
                    if (!String.valueOf(elementSize).equals(actualStr))
                    {
                        System.out.println("[z] size: " + elementSize
                                + ", expected: " + String
                                .valueOf(elementSize) + ", actual: " + actualStr);
                    }
                }
                else
                {
                    assertEquals(elementSize, acv.vector[i]);
                    assertEquals(elementSize * 3.1415f, bcv.vector[i], 0.000001f);
                    assertEquals(elementSize * 3.14159d, ccv.vector[i], 0.000001d);
                    assertEquals(time, dcv.times[i]);
                    assertEquals((elementSize > 25 ? 1 : 0), ecv.vector[i]);
                    assertEquals(String.valueOf(elementSize),
                            new String(zcv.vector[i], zcv.start[i], zcv.lens[i]));
                }
            }
            elementSize++;
        }
    }

    private void assertCorrect(VectorizedRowBatch rowBatch,
                               LongColumnVector acv,
                               DoubleColumnVector bcv,
                               DoubleColumnVector ccv,
                               TimestampColumnVector dcv,
                               LongColumnVector ecv,
                               BinaryColumnVector zcv,
                               long time)
    {
        for (int i = 0; i < rowBatch.size; i++)
        {
            if (DEBUG)
            {
                if (elementSize != acv.vector[i])
                {
                    System.out.println("[a] size: " + elementSize
                            + ", expected: " + elementSize + ", actual: " + acv.vector[i]);
                }
                if (acv.isNull[i])
                {
                    System.out.println("[a] size: " + elementSize + ", null");
                }
                if (Float.compare(elementSize * 3.1415f, (float) bcv.vector[i]) != 0)
                {
                    System.out.println("[b] size: " + elementSize
                            + ", expected: " + elementSize * 3.1415f + ", actual: " + (float) bcv.vector[i]);
                }
                if (bcv.isNull[i])
                {
                    System.out.println("[b] size: " + elementSize + ", null");
                }
                if (Math.abs(elementSize * 3.14159d - ccv.vector[i]) > 0.000001)
                {
                    System.out.println("[c] size: " + elementSize
                            + ", expected: " + elementSize * 3.14159d + ", actual: " + ccv.vector[i]);
                }
                if (ccv.isNull[i])
                {
                    System.out.println("[c] size: " + elementSize + ", null");
                }
                if (dcv.times[i] != time)
                {
                    System.out.println("[d] size: " + elementSize
                            + ", expected: " + time + ", actual: " + dcv.times[i]);
                }
                if (dcv.isNull[i])
                {
                    System.out.println("[d] size: " + elementSize + ", null");
                }
                int expectedBool = elementSize > 25000 ? 1 : 0;
                if (expectedBool != ecv.vector[i])
                {
                    System.out.println("[e] size: " + elementSize
                            + ", expected: " + expectedBool + ", actual: " + ecv.vector[i]);
                }
                if (ecv.isNull[i])
                {
                    System.out.println("[e] size: " + elementSize + ", null");
                }
                String actualStr = new String(zcv.vector[i], zcv.start[i], zcv.lens[i]);
                if (!String.valueOf(elementSize).equals(actualStr))
                {
                    System.out.println("[z] size: " + elementSize
                            + ", expected: " + String
                            .valueOf(elementSize) + ", actual: " + actualStr);
                }
                if (zcv.isNull[i])
                {
                    System.out.println("[z] size: " + elementSize + ", null");
                }
            }
            else
            {
                assertFalse(acv.isNull[i]);
                assertEquals(elementSize, acv.vector[i]);
                assertFalse(bcv.isNull[i]);
                assertEquals(elementSize * 3.1415f, bcv.vector[i], 0.000001d);
                assertFalse(ccv.isNull[i]);
                assertEquals(elementSize * 3.14159d, ccv.vector[i], 0.000001f);
                assertFalse(dcv.isNull[i]);
                assertEquals(dcv.times[i], 1528901945696L);
                assertFalse(ecv.isNull[i]);
                assertEquals((elementSize > 25000 ? 1 : 0), ecv.vector[i]);
                assertFalse(zcv.isNull[i]);
                assertEquals(String.valueOf(elementSize),
                        new String(zcv.vector[i], zcv.start[i], zcv.lens[i]));
            }
            elementSize++;
        }
    }

    private PixelsReader getReader(String fileName)
    {
        PixelsReader pixelsReader = null;
        String filePath = Objects.requireNonNull(
                this.getClass().getClassLoader().getResource("files/" + fileName)).getPath();
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("hdfs");
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(filePath)
                    .build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }
}