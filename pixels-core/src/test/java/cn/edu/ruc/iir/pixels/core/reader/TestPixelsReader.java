package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsReader {
    private TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
    private PixelsReader pixelsReader = null;

    @Before
    public void setup() {
        String filePath = TestParams.filePath;
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
    }

    @Test
    public void testMetadata() {
        if (pixelsReader == null) {
            return;
        }

//        assertEquals(PixelsProto.CompressionKind.NONE, pixelsReader.getCompressionKind());
//        assertEquals(TestParams.compressionBlockSize, pixelsReader.getCompressionBlockSize());
//        assertEquals(schema, pixelsReader.getFileSchema());
//        assertEquals(PixelsVersion.V1, pixelsReader.getFileVersion());
//        assertEquals(TestParams.rowNum, pixelsReader.getNumberOfRows());
//        assertEquals(TestParams.pixelStride, pixelsReader.getPixelStride());
//        assertEquals(TimeZone.getDefault().getDisplayName(), pixelsReader.getWriterTimeZone());

        System.out.println(">>Footer: " + pixelsReader.getFooter().toString());
        System.out.println(">>Postscript: " + pixelsReader.getPostScript().toString());

        int rowGroupNum = pixelsReader.getRowGroupNum();
        System.out.println(">>Row group num: " + rowGroupNum);

        try {
            for (int i = 0; i < rowGroupNum; i++) {
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(i);
                System.out.println(">>Row group " + i + " footer");
                System.out.println(pixelsReader.getRowGroupInfo(i));
                for (int j = 0; j < 6; j++)
                {
                    PixelsProto.ColumnChunkIndex index = rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(5);
                    System.out.println(index);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testContent()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"a", "b", "c", "d", "e", "z"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        int batchSize = 1000;
        long elementSize = 0;
        try {
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
                DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
                DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
                TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
                LongColumnVector ecv = (LongColumnVector) rowBatch.cols[4];
                BytesColumnVector zcv = (BytesColumnVector) rowBatch.cols[5];
                if (rowBatch.endOfFile) {
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
                            assertEquals(elementSize * 3.1415f, bcv.vector[i], 0.0001d);
                            assertEquals(elementSize * 3.14159d, ccv.vector[i], 0.0001f);
                            assertEquals(dcv.time[i], 1528785092538L);
                            assertEquals((elementSize > 25 ? 1 : 0), ecv.vector[i]);
                            assertEquals(String.valueOf(elementSize),
                                    new String(zcv.vector[i], zcv.start[i], zcv.lens[i]));
                        }
                        elementSize++;
                    }
                    break;
                }
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
            System.out.println("Num " + elementSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPerformance()
    {}

    @After
    public void cleanUp() {
        try {
            pixelsReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}