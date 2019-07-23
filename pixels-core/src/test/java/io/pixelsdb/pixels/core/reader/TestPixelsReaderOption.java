package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * pixels reader option test
 * this test is to guarantee that the pixels reader is able to handle all kinds of options specified by users
 *
 * @author guodong
 */
public class TestPixelsReaderOption
{
    private int elementSize = 0;

    @Test
    public void test0RGRange()
            throws IOException
    {
        // `test-large-null.pxl` is set as the testing file
        // this file consists of 6 row groups
        // rg0: 5457920 rows
        // rg1: 3493888 rows
        // rg2: 3374080 rows
        // rg3: 3321856 rows
        // rg4: 3321856 rows
        // rg5: 1030400 rows
        String fileName = "test-large-null.pxl";
        PixelsReader pixelsReader = getReader(fileName);
        PixelsRecordReader recordReader;
        int batchSize = 10000;

        VectorizedRowBatch rowBatch;
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"a", "b", "c", "d", "e", "z"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        // the whole file
        option.rgRange(0, 6);
        recordReader = pixelsReader.read(option);
        elementSize = 0;
        while (true)
        {
            rowBatch = recordReader.readBatch(batchSize);
            if (rowBatch.endOfFile)
            {
                assertCorrectness(rowBatch, 1528902023606L, 0);
                break;
            }
            assertCorrectness(rowBatch, 1528902023606L, 0);
        }
        assertEquals(20_000_000, elementSize);
        System.out.println("Done with the whole file");
        recordReader.close();

        // rg0
        option.rgRange(0, 1);
        recordReader = pixelsReader.read(option);
        elementSize = 0;
        while (true)
        {
            rowBatch = recordReader.readBatch(batchSize);
            if (rowBatch.endOfFile)
            {
                assertCorrectness(rowBatch, 1528902023606L, 0);
                break;
            }
            assertCorrectness(rowBatch, 1528902023606L, 0);
        }
        assertEquals(5457920, elementSize);
        System.out.println("Done with rg0");
        recordReader.close();

        // rg1, rg2, rg3, rg4
        option.rgRange(1, 4);
        recordReader = pixelsReader.read(option);
        elementSize = 0;
        while (true)
        {
            rowBatch = recordReader.readBatch(batchSize);
            if (rowBatch.endOfFile)
            {
                assertCorrectness(rowBatch, 1528902023606L, 5457920);
                break;
            }
            assertCorrectness(rowBatch, 1528902023606L, 5457920);
        }
        assertEquals(13511680, elementSize);
        System.out.println("Done with rg1, rg2, rg3 and rg4");
        recordReader.close();

        // rg4, rg5
        option.rgRange(4, 2);
        recordReader = pixelsReader.read(option);
        elementSize = 0;
        while (true)
        {
            rowBatch = recordReader.readBatch(batchSize);
            if (rowBatch.endOfFile)
            {
                assertCorrectness(rowBatch, 1528902023606L, 15647744);
                break;
            }
            assertCorrectness(rowBatch, 1528902023606L, 15647744);
        }
        assertEquals(4352256, elementSize);
        System.out.println("Done with rg4 and rg5");
        recordReader.close();

        pixelsReader.close();
    }

    private void assertCorrectness(VectorizedRowBatch rowBatch, long time, int start)
    {
        LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
        DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
        DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
        TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
        LongColumnVector ecv = (LongColumnVector) rowBatch.cols[4];
        BinaryColumnVector zcv = (BinaryColumnVector) rowBatch.cols[5];
        for (int i = 0; i < rowBatch.size; i++)
        {
            int rowId = elementSize + start;
            if (rowId % 100 == 0)
            {
                assertTrue(acv.isNull[i]);
                assertTrue(bcv.isNull[i]);
                assertTrue(ccv.isNull[i]);
                assertTrue(dcv.isNull[i]);
                assertTrue(ecv.isNull[i]);
                assertTrue(zcv.isNull[i]);
            }
            else
            {
                assertEquals(rowId, acv.vector[i]);
                assertEquals(rowId * 3.1415f, bcv.vector[i], 0.000001f);
                assertEquals(rowId * 3.14159d, ccv.vector[i], 0.000001d);
                assertEquals(time, dcv.time[i]);
                assertEquals(rowId > 25 ? 1 : 0, ecv.vector[i]);
                assertEquals(String.valueOf(rowId),
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
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try
        {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(path)
                    .build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }
}
