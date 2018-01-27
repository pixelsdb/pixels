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
public class TestPixelsReader
{
    @Test
    public void validateWriter()
    {
        String filePath = TestParams.filePath;
        Path path = new Path(filePath);
        TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try (FileSystem fs = FileSystem.get(URI.create(filePath), conf);
             PixelsReader pixelsReader = PixelsReaderImpl.newBuilder().
                     setFS(fs).setPath(path).setSchema(schema)
                     .build()) {
            assertEquals(PixelsProto.CompressionKind.NONE, pixelsReader.getCompressionKind());
            assertEquals(0, pixelsReader.getCompressionBlockSize());
            assertEquals(schema, pixelsReader.getFileSchema());
            assertEquals(PixelsVersion.V1, pixelsReader.getFileVersion());
            assertEquals(TestParams.rowNum, pixelsReader.getNumberOfRows());
            assertEquals(10000, pixelsReader.getPixelStride());
            assertEquals(TimeZone.getDefault().getDisplayName(), pixelsReader.getWriterTimeZone());

            PixelsReaderOption option = new PixelsReaderOption();
            String[] cols = {"a", "b", "c", "d", "e", "z"};
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);

            PixelsRecordReader recordReader = pixelsReader.read(option);
            VectorizedRowBatch rowBatch = schema.createRowBatch(10000);
            while (recordReader.nextBatch(rowBatch)) {
                System.out.println("Getting next batch. Current size : " + rowBatch.size);
            }
            recordReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
