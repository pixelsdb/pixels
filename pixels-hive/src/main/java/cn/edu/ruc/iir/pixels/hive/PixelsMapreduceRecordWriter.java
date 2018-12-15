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

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.hive.mapred.PixelsKey;
import cn.edu.ruc.iir.pixels.hive.mapred.PixelsMapredRecordWriter;
import cn.edu.ruc.iir.pixels.hive.mapred.PixelsValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class PixelsMapreduceRecordWriter<V extends Writable>
        extends RecordWriter<NullWritable, V> {

    private final PixelsWriter writer;
    private final VectorizedRowBatch batch;
    private final TypeDescription schema;
    private final boolean isTopStruct;

    public PixelsMapreduceRecordWriter(PixelsWriter writer) {
        this.writer = writer;
        schema = writer.getSchema();
        this.batch = schema.createRowBatch();
        isTopStruct = schema.getCategory() == TypeDescription.Category.STRUCT;
    }

    @Override
    public void write(NullWritable nullWritable, V v) throws IOException {
        // if the batch is full, write it out.
        if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        // add the new row
        int row = batch.size++;
        // skip over the PixelsKey or PixelsValue
        if (v instanceof PixelsKey) {
            v = (V) ((PixelsKey) v).key;
        } else if (v instanceof PixelsValue) {
            v = (V) ((PixelsValue) v).value;
        }
        if (isTopStruct) {
            for (int f = 0; f < schema.getChildren().size(); ++f) {
                PixelsMapredRecordWriter.setColumn(schema.getChildren().get(f),
                        batch.cols[f], row, ((PixelsStruct) v).getFieldValue(f));
            }
        } else {
            PixelsMapredRecordWriter.setColumn(schema, batch.cols[0], row, v);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
        if (batch.size != 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }
}
