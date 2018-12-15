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
import cn.edu.ruc.iir.pixels.hive.core.PixelsConf;
import cn.edu.ruc.iir.pixels.hive.core.PixelsFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * An ORC output format that satisfies the org.apache.hadoop.mapred API.
 */
public class PixelsOutputFormat<V extends Writable>
        extends FileOutputFormat<NullWritable, V> {

    /**
     * This function builds the options for the ORC Writer based on the JobConf.
     *
     * @param conf the job configuration
     * @return a new options object
     */
    public static PixelsFile.WriterOptions buildOptions(Configuration conf) {
        return PixelsFile.writerOptions(conf)
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
    public RecordWriter<NullWritable, V> getRecordWriter(FileSystem fileSystem,
                                                         JobConf conf,
                                                         String name,
                                                         Progressable progressable
    ) throws IOException {
        Path path = getTaskOutputPath(conf, name);
        PixelsWriter writer = PixelsFile.createWriter(path,
                buildOptions(conf).fileSystem(fileSystem));
        return new PixelsMapredRecordWriter<>(writer);
    }
}
