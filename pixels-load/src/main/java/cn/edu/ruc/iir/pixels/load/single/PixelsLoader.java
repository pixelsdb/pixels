package cn.edu.ruc.iir.pixels.load.single;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import cn.edu.ruc.iir.pixels.common.utils.StringUtil;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsLoader
        extends Loader
{
    PixelsLoader(String originalDataPath, String dbName, String tableName, int maxRowNum, String regex)
    {
        super(originalDataPath, dbName, tableName, maxRowNum, regex);
    }

    @Override
    protected boolean executeLoad(String originalDataPath, String loadingDataPath, String schemaStr,
                                  int[] orderMapping, ConfigFactory configFactory, int maxRowNum, String regex)
            throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(loadingDataPath), conf);
        TypeDescription schema = TypeDescription.fromString(schemaStr);
        VectorizedRowBatch rowBatch = schema.createRowBatch();
        ColumnVector[] columnVectors = rowBatch.cols;
        int pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size")) * 1024 * 1024;
        long blockSize = Long.parseLong(configFactory.getProperty("block.size")) * 1024l * 1024l;
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));

        // read original data
        FileStatus[] fileStatuses = fs.listStatus(new Path(originalDataPath));
        List<Path> originalFilePaths = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses)
        {
            if (fileStatus.isFile())
            {
                originalFilePaths.add(fileStatus.getPath());
            }
        }
        BufferedReader reader = null;
        String line;
        String loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".pxl";
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setFS(fs)
                .setFilePath(new Path(loadingFilePath))
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncoding(true)
                .setCompressionBlockSize(1)
                .build();
        int rowCounter = 0;
        for (Path originalFilePath : originalFilePaths)
        {
            reader = new BufferedReader(new InputStreamReader(fs.open(originalFilePath)));
            while ((line = reader.readLine()) != null)
            {
                line = StringUtil.replaceAll(line, "false", "0");
                line = StringUtil.replaceAll(line, "False", "0");
                line = StringUtil.replaceAll(line, "true", "1");
                line = StringUtil.replaceAll(line, "True", "1");
                int rowId = rowBatch.size++;
                rowCounter++;
                if(regex.equals("\\s")){
                    regex = " ";
                }
                String[] colsInLine = line.split(regex);
                for (int i = 0; i < columnVectors.length; i++)
                {
                    int valueIdx = orderMapping[i];
                    if (colsInLine[valueIdx].equalsIgnoreCase("\\N"))
                    {
                        columnVectors[i].isNull[rowId] = true;
                    }
                    else
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
                        loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".pxl";
                        pixelsWriter = PixelsWriterImpl.newBuilder()
                                .setSchema(schema)
                                .setPixelStride(pixelStride)
                                .setRowGroupSize(rowGroupSize)
                                .setFS(fs)
                                .setFilePath(new Path(loadingFilePath))
                                .setBlockSize(blockSize)
                                .setReplication(replication)
                                .setBlockPadding(true)
                                .setEncoding(true)
                                .setCompressionBlockSize(1)
                                .build();
                        rowCounter = 0;
                    }
                }
            }
        }
        if (rowBatch.size != 0)
        {
            pixelsWriter.addRowBatch(rowBatch);
            rowBatch.reset();
        }
        pixelsWriter.close();
        if (reader != null)
        {
            reader.close();
        }

        return true;
    }
}
