/*
 * Copyright 2018 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.load.single;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.common.utils.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

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
public class ORCLoader
        extends Loader
{
    ORCLoader(String originalDataPath, String dbName, String tableName, int maxRowNum, String regex)
    {
        super(originalDataPath, dbName, tableName, maxRowNum, regex);
    }

    private enum VECTOR_CLAZZ
    {
        BytesColumnVector, DoubleColumnVector, LongColumnVector
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
        int strideSize = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size")) * 1024 * 1024;
        int blockSize = Integer.parseInt(configFactory.getProperty("block.size")) * 1024 * 1024;

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
        String loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".orc";
        Writer orcWriter = OrcFile.createWriter(new Path(loadingFilePath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .rowIndexStride(strideSize)
                        .stripeSize(rowGroupSize)
                        .blockSize(blockSize)
                        .compress(CompressionKind.NONE)
                        .blockPadding(true)
                        .fileSystem(fs)
        );
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
                        String value = colsInLine[valueIdx];
                        VECTOR_CLAZZ clazz = VECTOR_CLAZZ.valueOf(columnVectors[i].getClass().getSimpleName());
                        switch (clazz)
                        {
                            case BytesColumnVector:
                                BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVectors[i];
                                bytesColumnVector.setVal(rowId, value.getBytes());
                                break;
                            case DoubleColumnVector:
                                DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVectors[i];
                                doubleColumnVector.vector[rowId] = Double.valueOf(value);
                                break;
                            case LongColumnVector:
                                LongColumnVector longColumnVector = (LongColumnVector) columnVectors[i];
                                longColumnVector.vector[rowId] = Long.valueOf(value);
                                break;
                        }
                    }
                }

                if (rowBatch.size >= rowBatch.getMaxSize())
                {
                    orcWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                    if (rowCounter >= maxRowNum)
                    {
                        orcWriter.close();
                        loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".orc";
                        orcWriter = OrcFile.createWriter(new Path(loadingFilePath),
                                OrcFile.writerOptions(conf)
                                        .setSchema(schema)
                                        .rowIndexStride(strideSize)
                                        .stripeSize(rowGroupSize)
                                        .blockSize(blockSize)
                                        .compress(CompressionKind.NONE)
                                        .blockPadding(true)
                                        .fileSystem(fs));
                        rowCounter = 0;
                    }
                }
            }
        }
        if (rowBatch.size != 0)
        {
            orcWriter.addRowBatch(rowBatch);
            rowBatch.reset();
        }
        orcWriter.close();
        if (reader != null)
        {
            reader.close();
        }

        return true;
    }
}
