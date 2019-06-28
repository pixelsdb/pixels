/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.hive.mapred;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.hive.PixelsFile;
import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * A MapReduce/Hive input format for PIXELS files.
 * refer: [OrcInputFormat](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/OrcInputFormat.java)
 */
public class PixelsInputFormat
        extends FileInputFormat<NullWritable, PixelsStruct>
{
    private static Logger log = LogManager.getLogger(PixelsInputFormat.class);

    /**
     * Splits files returned by {@link #listStatus(JobConf)} when
     * they're too big. set
     * hive.input.format=cn.edu.ruc.iir.pixels.hive.mapred.PixelsInputFormat
     * in hive to use this method.
     *
     * @param job
     * @param numSplits
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
    {
        InputSplit[] splits = super.getSplits(job, numSplits);
        log.info("number of splits for pixels: " + splits.length);
        return splits;
    }

    @Override
    public RecordReader<NullWritable, PixelsStruct>
    getRecordReader(InputSplit inputSplit,
                    JobConf conf,
                    Reporter reporter) throws IOException
    {
        FileSplit split = (FileSplit) inputSplit;
        PixelsFile.ReaderOptions option = PixelsFile.readerOptions(conf, split);
        log.info(split.toString());
        PixelsReader reader = PixelsFile.createReader(split.getPath(), option);
        return new PixelsMapredRecordReader<>(reader,
                option.setOption(reader.getFileSchema()));
    }

}
