package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
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

public class TestReader {

    String filePath = "hdfs://presto00:9000/po_compare/test.pxl";
    String tSchema = "struct<orderkey:int,orderstatus:string,totalprice:double>";

    private TypeDescription schema = TypeDescription.fromString(tSchema);
    private PixelsReader pixelsReader = null;

    @Test
    public void testContent() {
        //setup
//        String filePath = TestParams.testFilePath;
//        String filePath = "hdfs://presto00:9000/po_compare/test.pxl";
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(path)
                    .setSchema(schema)
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"orderstatus", "totalprice", "orderkey"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);
        // read
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        int batchSize = 10000;
        try {
            long start = System.currentTimeMillis();
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                System.out.println(rowBatch.toString());
                int size = rowBatch.size;
                System.out.println(size);
                if (rowBatch.endOfFile) {
                    System.out.println("End of file");
                    break;
                }
            }
            rowBatch = recordReader.readBatch(batchSize);
            int size = rowBatch.size;
            System.out.println(size);
            long end = System.currentTimeMillis();
            System.out.println(recordReader.getRowNumber() + ", time: " + (end - start));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // cleanUp
        try {
            pixelsReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
