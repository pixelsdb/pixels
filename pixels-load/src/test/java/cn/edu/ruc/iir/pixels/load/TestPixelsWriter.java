package cn.edu.ruc.iir.pixels.load;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.StringUtil;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.load.multi.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created at: 18-11-19
 * Author: hank
 */
public class TestPixelsWriter
{
    @Test
    public void test () throws IOException, MetadataException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        Config config = new Config("pixels", "test_105", 300000, "\t", "pixels");
        config.load(configFactory);
        String loadingDataPath = config.getPixelsPath();
        String schemaStr = config.getSchema();
        int[] orderMapping = config.getOrderMapping();
        int maxRowNum = config.getMaxRowNum();
        String regex = config.getRegex();

        int pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size")) * 1024 * 1024;
        long blockSize = Long.parseLong(configFactory.getProperty("block.size")) * 1024l * 1024l;
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(loadingDataPath), conf);
        TypeDescription schema = TypeDescription.fromString(schemaStr);
        VectorizedRowBatch rowBatch = schema.createRowBatch();
        ColumnVector[] columnVectors = rowBatch.cols;

        fs.deleteOnExit(new Path(loadingDataPath + "test_5000_lines.pxl"));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(
                new Path("hdfs://dbiir10:9000/pixels/pixels/test_105/source_small/000148_0_small"))));
        String line;

        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setFS(fs)
                .setFilePath(new Path(loadingDataPath + "test_5000_lines.pxl"))
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncoding(true)
                .setCompressionBlockSize(1)
                .build();

        int rowCounter = 0;

        while ((line = reader.readLine()) != null)
        {
            line = StringUtil.replaceAll(line, "false", "0");
            line = StringUtil.replaceAll(line, "False", "0");
            line = StringUtil.replaceAll(line, "true", "1");
            line = StringUtil.replaceAll(line, "True", "1");
            int rowId = rowBatch.size++;
            rowCounter++;
            if (regex.equals("\\s"))
            {
                regex = " ";
            }
            String[] colsInLine = line.split(regex);
            for (int i = 0; i < columnVectors.length; i++)
            {
                int valueIdx = orderMapping[i];
                if (colsInLine[valueIdx].equalsIgnoreCase("\\N"))
                {
                    columnVectors[i].isNull[rowId] = true;
                } else
                {
                    columnVectors[i].add(colsInLine[valueIdx]);
                }
            }

            if (rowBatch.size >= rowBatch.getMaxSize())
            {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
                if (rowCounter >= maxRowNum)
                {
                    pixelsWriter.close();
                    rowCounter = 0;
                }
            }
        }

        reader.close();

        if (rowCounter > 0)
        {
            // left last file to write
            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }
            pixelsWriter.close();
        }
    }
}
