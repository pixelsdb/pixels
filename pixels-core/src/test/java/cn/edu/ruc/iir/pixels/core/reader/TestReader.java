package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestReader {
    //ETL test
//    String filePath = "hdfs://presto00:9000/po_compare/test.pxl";
//    String tSchema = "struct<orderkey:int,orderstatus:string,totalprice:double>";
//    String[] cols = {"orderstatus", "totalprice", "orderkey"};
    // Point test
//    String filePath = "hdfs://10.77.40.236:9000/pixels/v2/point2000w.pxl";
    static String tSchema = "struct<id:int,x:double,y:double>";
    //    static String[] cols = {"x", "y", "id"};
    //    static String[] cols = new String[0];

    // 1000 columns
    String filePath = "hdfs://10.77.40.236:9000/pixels/test30G_pixels/201805171350200.pxl";
    static String[] cols = {"Column_1", "Column_3", "Column_13"};

    private static PixelsReader pixelsReader = null;

    @Test
    public void testContent() {
        //setup
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
        PixelsReaderOption option = new PixelsReaderOption();
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
//                System.out.println(size);
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

    public static void main(String[] args) {
        String path = "hdfs://presto00:9000/pixels/v2/";
        String filePath = null;
        for (int i = 1; i <= 4; i++) {
            filePath = path + "point" + i + ".pxl";
            String finalFilePath = filePath;
            Thread t = new Thread(() -> ReadFileByPath(finalFilePath));
            t.start();
        }
    }

    private static void ReadFileByPath(String filePath) {
        System.out.println("RadFile: " + filePath);
        //setup
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
        PixelsReaderOption option = new PixelsReaderOption();
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
