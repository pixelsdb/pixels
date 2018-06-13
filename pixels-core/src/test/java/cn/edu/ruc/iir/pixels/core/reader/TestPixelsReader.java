package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * pixels reader test
 * this test is to guarantee correctness of the pixels reader
 *
 * @author guodong
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestPixelsReader
{
    private long elementSize = 0;

    private void testMetadata() {
//        assertEquals(PixelsProto.CompressionKind.NONE, pixelsReader.getCompressionKind());
//        assertEquals(TestParams.compressionBlockSize, pixelsReader.getCompressionBlockSize());
//        assertEquals(schema, pixelsReader.getFileSchema());
//        assertEquals(PixelsVersion.V1, pixelsReader.getFileVersion());
//        assertEquals(TestParams.rowNum, pixelsReader.getNumberOfRows());
//        assertEquals(TestParams.pixelStride, pixelsReader.getPixelStride());
//        assertEquals(TimeZone.getDefault().getDisplayName(), pixelsReader.getWriterTimeZone());
    }

    private void testContent(String fileName, int batchSize, int rowNum)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"a", "b", "c", "d", "e", "z"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        elementSize = 0;
        try (PixelsReader pixelsReader = getReader(fileName);
        PixelsRecordReader recordReader = pixelsReader.read(option)) {
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
                DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
                DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
                TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
                LongColumnVector ecv = (LongColumnVector) rowBatch.cols[4];
                BytesColumnVector zcv = (BytesColumnVector) rowBatch.cols[5];
                if (rowBatch.endOfFile) {
                    assertCorrect(rowBatch, acv, bcv, ccv, dcv, ecv, zcv);
                    break;
                }

            }
            assertEquals(rowNum, elementSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test0Small()
    {
        String fileName = "test-small.pxl";
        int rowNum = 200_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum);
        }
    }

    @Test
    public void test1Mid()
    {
        String fileName = "test-mid.pxl";
        int rowNum = 2_000_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum);
        }
    }

    @Test
    public void test2Large()
    {
        String fileName = "test-mid.pxl";
        int rowNum = 20_000_000;
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            int batchSize = random.nextInt(rowNum);
            System.out.println("row batch size: " + batchSize);
            testContent(fileName, batchSize, rowNum);
        }
    }

    private void assertCorrect(VectorizedRowBatch rowBatch,
                               LongColumnVector acv,
                               DoubleColumnVector bcv,
                               DoubleColumnVector ccv,
                               TimestampColumnVector dcv,
                               LongColumnVector ecv,
                               BytesColumnVector zcv)
    {
        for (int i = 0; i < rowBatch.size; i++)
        {
            if (elementSize % 100 == 0)
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
                assertEquals(elementSize, acv.vector[i]);
                assertEquals( elementSize * 3.1415f, bcv.vector[i], 0.0001d);
                assertEquals( elementSize * 3.14159d, ccv.vector[i], 0.0001f);
                assertEquals(dcv.time[i], 1528785092538L);
                assertEquals((elementSize > 25 ? 1 : 0), ecv.vector[i]);
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
                Class.class.getClassLoader().getResource("files/" + fileName)).getPath();
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(path)
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return pixelsReader;
    }
}