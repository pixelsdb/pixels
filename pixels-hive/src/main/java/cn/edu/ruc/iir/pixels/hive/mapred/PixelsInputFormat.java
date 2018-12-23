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
import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import cn.edu.ruc.iir.pixels.hive.core.PixelsFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * A MapReduce/Hive input format for PIXELS files.
 * refer: [OrcInputFormat](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/OrcInputFormat.java)
 */
public class PixelsInputFormat<V extends WritableComparable>
        extends FileInputFormat<NullWritable, PixelsStruct> {

    @Override
    public RecordReader<NullWritable, PixelsStruct>
    getRecordReader(InputSplit inputSplit,
                    JobConf conf,
                    Reporter reporter) throws IOException {
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        PixelsFile.ReaderOptions option = PixelsFile.readerOptions(conf).filesystem(conf);

        PixelsReader reader = PixelsFile.createReader(path, option);
        return new PixelsMapredRecordReader<>(reader,
                option.setOption(reader.getFileSchema()));
    }

}
