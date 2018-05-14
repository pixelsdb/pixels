package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class Main
{
    private static String schemaStr = "struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>";

    public static void main(String[] args)
    {
        if (args.length < 3) {
            System.out.println("prog write path rowNum\nprog read fileDir col col ...");
            System.exit(-1);
        }
        String rwFlag = args[0];
        String path = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        if (rwFlag.equalsIgnoreCase("write")) {
            int count = Integer.parseInt(args[2]);
            write(path, conf, count);
        }
        else if (rwFlag.equalsIgnoreCase("read")) {
            String[] schema = new String[args.length - 2];
            System.arraycopy(args, 2, schema, 0, args.length - 2);
            try {
                FileSystem fs = FileSystem.get(URI.create(path), conf);
                FileStatus[] fileStatuses = fs.listStatus(new Path(path));
                long processBegin = System.currentTimeMillis();
                List<Thread> threads = new ArrayList<>();
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.getPath().getName().startsWith(".")) {
                        continue;
                    }
                    MockPixelsReader mockPixelsReader =
                            new MockPixelsReader(fs, fileStatus.getPath(), schema);
                    Thread mockThread = new Thread(mockPixelsReader);
                    threads.add(mockThread);
                    mockThread.start();
                }
                for (Thread t : threads) {
                    t.join();
                }
                long processEnd = System.currentTimeMillis();
                System.out.println("[process] " + processBegin + processEnd + ", cost: " + (processEnd - processBegin));
            }
            catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        else {
            System.out.println("prog write path rowNum\nprog read fileDir col col ...");
            System.exit(-1);
        }
    }

    private static void write(String path, Configuration conf, int count)
    {
        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try
        {
            FileSystem fs = FileSystem.get(URI.create(path), conf);
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) rowBatch.cols[0];              // int
            DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[1];          // float
            DoubleColumnVector c = (DoubleColumnVector) rowBatch.cols[2];          // double
            TimestampColumnVector d = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
            LongColumnVector e = (LongColumnVector) rowBatch.cols[4];              // boolean
            BytesColumnVector z = (BytesColumnVector) rowBatch.cols[5];            // string

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64 * 1024 * 1024)
                            .setFS(fs)
                            .setFilePath(new Path(path))
                            .setBlockSize(1024 * 1024 * 1024)
                            .setReplication((short) 1)
                            .setBlockPadding(true)
                            .setEncoding(true)
                            .setCompressionBlockSize(1)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
//            System.out.println(curT + ", nanos: " + timestamp.getNanos() + ",  time: " + timestamp.getTime());
            for (int i = 0; i < count; i++) {
                int row = rowBatch.size++;
                a.vector[row] = i;
                b.vector[row] = i * 3.1415f;
                c.vector[row] = i * 3.14159d;
                d.set(row, timestamp);
                e.vector[row] = i > 25000 ? 1 : 0;
                z.setVal(row, String.valueOf(i).getBytes());
                if (rowBatch.size == rowBatch.getMaxSize()) {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }
            if (rowBatch.size != 0) {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }
            pixelsWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
