package cn.edu.ruc.iir.pixels.load.pc;

import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import cn.edu.ruc.iir.pixels.common.utils.StringUtil;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.pc
 * @ClassName: PixelsConsumer
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-30 15:18
 **/
public class PixelsConsumer extends Consumer {

    private BlockingQueue<Path> queue;
    private Properties prop;
    private Config config;

    public Properties getProp() {
        return prop;
    }

    public PixelsConsumer(BlockingQueue<Path> queue, Properties prop, Config config) {
        this.queue = queue;
        this.prop = prop;
        this.config = config;
    }

    @Override
    public void run() {
        System.out.println("Start PixelsConsumer, " + Thread.currentThread().getName() + ", time: " + DateUtil.formatTime(new Date()));

        boolean isRunning = true;
        try {
            String loadingDataPath = config.getPixelsPath();
            String schemaStr = config.getSchema();
            int[] orderMapping = config.getOrderMapping();
            int maxRowNum = config.getMaxRowNum();
            String regex = config.getRegex();

            Properties prop = getProp();
            int pixelStride = Integer.parseInt(prop.getProperty("pixel.stride"));
            int rowGroupSize = Integer.parseInt(prop.getProperty("row.group.size")) * 1024 * 1024;
            long blockSize = Long.parseLong(prop.getProperty("block.size")) * 1024l * 1024l;
            short replication = Short.parseShort(prop.getProperty("block.replication"));

            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(URI.create(loadingDataPath), conf);
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            ColumnVector[] columnVectors = rowBatch.cols;

            BufferedReader reader = null;
            String line;
            PixelsWriter pixelsWriter = null;

            while (isRunning) {
                Path originalFilePath = queue.poll(2, TimeUnit.SECONDS);
                if (null != originalFilePath) {

                    String loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".pxl";
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

                    int rowCounter = 0;

                    reader = new BufferedReader(new InputStreamReader(fs.open(originalFilePath)));

                    while ((line = reader.readLine()) != null) {
                        line = StringUtil.replaceAll(line, "false", "0");
                        line = StringUtil.replaceAll(line, "False", "0");
                        line = StringUtil.replaceAll(line, "true", "1");
                        line = StringUtil.replaceAll(line, "True", "1");
                        int rowId = rowBatch.size++;
                        rowCounter++;
                        if (regex.equals("\\s")) {
                            regex = " ";
                        }
                        String[] colsInLine = line.split(regex);
                        for (int i = 0; i < columnVectors.length; i++) {
                            int valueIdx = orderMapping[i];
                            if (colsInLine[valueIdx].equalsIgnoreCase("\\N")) {
                                columnVectors[i].isNull[rowId] = true;
                            } else {
                                columnVectors[i].add(colsInLine[valueIdx]);
                            }
                        }

                        if (rowBatch.size >= rowBatch.getMaxSize()) {
                            pixelsWriter.addRowBatch(rowBatch);
                            rowBatch.reset();
                            if (rowCounter >= maxRowNum) {
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


                } else {
                    // over 2s， assume all the produce line is out， consumer exit
                    isRunning = false;
                }

            }

            // left last file to write
            if (rowBatch.size != 0) {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }

            pixelsWriter.close();

            if (reader != null) {
                reader.close();
            }

        } catch (InterruptedException e) {
            System.out.println("PixelsConsumer: " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Exit PixelsConsumer, " + Thread.currentThread().getName() + ", time: " + DateUtil.formatTime(new Date()));
        }
    }
}
