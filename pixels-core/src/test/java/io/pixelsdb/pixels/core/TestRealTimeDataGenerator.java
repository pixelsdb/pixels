package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Random;

/**
 * Created at: 28.09.20
 * Author: bian
 */
public class TestRealTimeDataGenerator
{
    @Test
    public void generateData () throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/hank/data/realtime.csv"));
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < 100*1000*1000; ++i)
        {
            writer.write(i + ",");
            for (int j = 0; j < 8; ++j)
            {
                writer.write(random.nextInt() + ",");
            }
            writer.write(random.nextInt() + "\n");
        }
        writer.close();
    }

    @Test
    public void generatePixelsFiles () throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader("/home/hank/data/realtime.csv"));
        String line;
        PixelsWriter pixelsWriter = null;

        // construct pixels schema based on the column order of the latest writing layout
        StringBuilder schemaBuilder = new StringBuilder("struct<");
        for (int i = 0; i < 10; i++)
        {
            schemaBuilder.append("col" + i).append(":").append("bigint")
                    .append(",");
        }
        schemaBuilder.replace(schemaBuilder.length() - 1, schemaBuilder.length(), ">");
        TypeDescription schema = TypeDescription.fromString(schemaBuilder.toString());
        VectorizedRowBatch rowBatch = schema.createRowBatch(10000);
        int[] orderMap = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("file:///home/hank/data/realtime/"), conf);

        for (int i = 0; i < 10; ++i)
        {
            String pixelsFilePath = "file:///home/hank/data/realtime/" + i + ".pxl";
            pixelsWriter = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setPixelStride(10000)
                    .setRowGroupSize(128*1024*1024)
                    .setFS(fs)
                    .setFilePath(new Path(pixelsFilePath))
                    .setBlockSize(1024L*1024L*1024L)
                    .setReplication((short) 1)
                    .setBlockPadding(true)
                    .setEncoding(true)
                    .setCompressionBlockSize(1)
                    .build();

            for (int j = 0; j < 10*1000*1000; ++j)
            {
                line = reader.readLine();
                rowBatch.size++;
                String[] colsInLine = line.split(",");
                for (int k = 0; k < rowBatch.cols.length; ++k)
                {
                    rowBatch.cols[k].add(colsInLine[k]);
                }
                if (rowBatch.size >= rowBatch.getMaxSize())
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }

            pixelsWriter.close();
        }
/*
        while ((line = reader.readLine()) != null)
        {
            if (rowCounter % (10*1000*1000) == 0)
            {
                // we create a new pixels file if we can read a next line from the source file.
                String pixelsFilePath = "file:///home/hank/data/realtime/" + (rowCounter / (10*1000*1000)) + ".pxl";
                pixelsWriter = PixelsWriterImpl.newBuilder()
                        .setSchema(schema)
                        .setPixelStride(10000)
                        .setRowGroupSize(128*1024*1024)
                        .setFS(fs)
                        .setFilePath(new Path(pixelsFilePath))
                        .setBlockSize(1024L*1024L*1024L)
                        .setReplication((short) 1)
                        .setBlockPadding(true)
                        .setEncoding(true)
                        .setCompressionBlockSize(1)
                        .build();
            }
            rowCounter++;

            rowBatch.size++;
            String[] colsInLine = line.split(",");
            rowBatch.putRow(GlobalTsManager.Instance().getTimestamp(), colsInLine, orderMap);

            if (rowBatch.size >= rowBatch.getMaxSize())
            {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
                if (rowCounter % (10*1000*1000) == 0)
                {
                    pixelsWriter.close();
                }
            }
        }
*/

        reader.close();
    }

    @Test
    public void readPixelsFiles () throws IOException
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"col0", "col2", "col4"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        PixelsReader pixelsReader = null;
        String filePath = "file:///home/hank/data/realtime/2.pxl";
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try
        {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            pixelsReader = PixelsReaderImpl
                    .newBuilder()
                    .setFS(fs)
                    .setPath(path)
                    .setEnableCache(false)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        PixelsRecordReader recordReader = pixelsReader.read(option);

        recordReader.prepareBatch(20000);
        VectorizedRowBatch rowBatch = recordReader.readBatch(20000);
        System.out.println(rowBatch.size);

        LongColumnVector col0 = (LongColumnVector) rowBatch.cols[0];
        LongColumnVector col3 = (LongColumnVector) rowBatch.cols[1];
        LongColumnVector version = (LongColumnVector) rowBatch.cols[2];

        for (int i = 0; i < rowBatch.size; ++i)
        {
            System.out.println(col0.vector[i] + "," + col3.vector[i] + "," + version.vector[i]);
        }

        recordReader.close();
        pixelsReader.close();
    }
}
