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

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.hive.PixelsConf;
import cn.edu.ruc.iir.pixels.hive.PixelsRW;
import cn.edu.ruc.iir.pixels.hive.PixelsSerDe.PixelsRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * An PIXELS output format that satisfies the org.apache.hadoop.mapred API.
 *
 * This class is not finished.
 *
 * refers to {@link org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat}
 */
public class PixelsOutputFormat
        extends FileOutputFormat<NullWritable, PixelsRow> implements HiveOutputFormat<NullWritable, PixelsRow>
{

    /**
     * This function builds the options for the PIXELS Writer based on the JobConf.
     *
     * @param conf the job configuration
     * @return a new options object
     */
    public static PixelsRW.WriterOptions buildOptions(Configuration conf)
    {
        return PixelsRW.writerOptions(conf)
                .setSchema(TypeDescription.fromString(PixelsConf.MAPRED_OUTPUT_SCHEMA
                        .getString(conf)))
                .rowIndexStride((int) PixelsConf.ROW_INDEX_STRIDE.getLong(conf))
                .stripeSize(PixelsConf.STRIPE_SIZE.getLong(conf))
                .blockSize(PixelsConf.BLOCK_SIZE.getLong(conf))
                .blockPadding(PixelsConf.BLOCK_PADDING.getBoolean(conf))
                .encodingStrategy(PixelsConf.ENCODING_STRATEGY.getBoolean(conf))
                .compressionStrategy((int) PixelsConf.COMPRESSION_STRATEGY.getLong(conf));
    }

    @Override
    public RecordWriter<NullWritable, PixelsRow> getRecordWriter(FileSystem fileSystem,
                                                                                  JobConf conf,
                                                                                  String name,
                                                                                  Progressable progressable
    ) throws IOException
    {
        Path path = getTaskOutputPath(conf, name);
        PixelsWriter writer = PixelsRW.createWriter(path,
                buildOptions(conf).fileSystem(fileSystem));
        return new PixelsMapredRecordWriter(writer);
    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path path, Class<? extends Writable> aClass, boolean b, Properties properties, Progressable progressable) throws IOException
    {
        return null;
    }
}
