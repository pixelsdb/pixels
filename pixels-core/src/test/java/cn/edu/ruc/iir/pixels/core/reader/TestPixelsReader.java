package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.PixelsVersion;
import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
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
import java.util.TimeZone;

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

        assertEquals(PixelsProto.CompressionKind.NONE, pixelsReader.getCompressionKind());
        assertEquals(TestParams.compressionBlockSize, pixelsReader.getCompressionBlockSize());
        assertEquals(schema, pixelsReader.getFileSchema());
        assertEquals(PixelsVersion.V1, pixelsReader.getFileVersion());
        assertEquals(TestParams.rowNum, pixelsReader.getNumberOfRows());
        assertEquals(TestParams.pixelStride, pixelsReader.getPixelStride());
        assertEquals(TimeZone.getDefault().getDisplayName(), pixelsReader.getWriterTimeZone());

        System.out.println(">>Footer: " + pixelsReader.getFooter().toString());
        System.out.println(">>Postscript: " + pixelsReader.getPostScript().toString());

        int rowGroupNum = pixelsReader.getRowGroupNum();
        System.out.println(">>Row group num: " + rowGroupNum);

        try {
            for (int i = 0; i < rowGroupNum; i++) {
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(i);
                System.out.println(pixelsReader.getRowGroupInfo(i));
//                System.out.println(">>Row group " + i + " footer");
//                System.out.println(rowGroupFooter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testContent() throws IOException
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"id", "x", "y"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        int batchSize = 10000;
        long num = 0;
        try {
            long start = System.currentTimeMillis();
            System.out.println("Reader begin " + start);
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                if (rowBatch.endOfFile) {
//                    System.out.println("End of file");
                    num += rowBatch.size;
                    break;
                }
//                System.out.println(">>Getting next batch. Current size : " + rowBatch.size);
//                System.out.println(rowBatch.toString());
                num += rowBatch.size;
            }
            long end = System.currentTimeMillis();
            System.out.println("Reader end " + end + ", cost: " + (end - start));
            System.out.println("Num " + num);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanUp() {
        try {
            pixelsReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}