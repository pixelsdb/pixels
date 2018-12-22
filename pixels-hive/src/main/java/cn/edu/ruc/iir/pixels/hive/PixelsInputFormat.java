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

package cn.edu.ruc.iir.pixels.hive;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.hive.core.PixelsConf;
import cn.edu.ruc.iir.pixels.hive.core.PixelsFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An ORC input format that satisfies the org.apache.hadoop.mapreduce API.
 */
public class PixelsInputFormat<V extends WritableComparable>
        extends FileInputFormat<NullWritable, V> {

    /**
     * Put the given SearchArgument into the configuration for an PixelsInputFormat.
     *
     * @param conf        the configuration to modify
     * @param sarg        the SearchArgument to put in the configuration
     * @param columnNames the list of column names for the SearchArgument
     */
    public static void setSearchArgument(Configuration conf,
                                         SearchArgument sarg,
                                         String[] columnNames) {
        PixelsInputFormat.setSearchArgument(conf, sarg,
                columnNames);
    }

    @Override
    public RecordReader<NullWritable, V>
    createRecordReader(InputSplit inputSplit,
                       TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        PixelsReader file = PixelsFile.createReader(split.getPath(),
                PixelsFile.readerOptions(conf)
                        .maxLength(PixelsConf.MAX_FILE_LENGTH.getLong(conf)));
//        return new PixelsMapreduceRecordReader<>(file,
//                cn.edu.ruc.iir.pixels.hive.mapred.PixelsInputFormat.buildOptions(conf,
//                        file, split.getStart(), split.getLength()));
        return null;
    }

    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> complete = super.listStatus(job);
        List<FileStatus> result = new ArrayList<>(complete.size());
        for (FileStatus stat : complete) {
            if (stat.getLen() != 0) {
                result.add(stat);
            }
        }
        return result;
    }
}
