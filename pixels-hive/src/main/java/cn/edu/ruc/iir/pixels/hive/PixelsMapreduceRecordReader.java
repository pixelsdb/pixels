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
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.hive.mapred.PixelsMapredRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * This record reader implements the org.apache.hadoop.mapreduce API.
 * It is in the org.apache.orc.mapred package to share implementation with
 * the mapred API record reader.
 * @param <V> the root type of the file
 */
public class PixelsMapreduceRecordReader<V extends WritableComparable>
        extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, V> {
    private final TypeDescription schema;
    private final PixelsRecordReader batchReader;
    private final VectorizedRowBatch batch;
    private int rowInBatch;
    private final V row;

    public PixelsMapreduceRecordReader(PixelsRecordReader reader,
                                       TypeDescription schema) throws IOException {
        this.batchReader = reader;
        this.batch = schema.createRowBatch();
        this.schema = schema;
        rowInBatch = 0;
        this.row = (V) PixelsStruct.createValue(schema);
    }

    public PixelsMapreduceRecordReader(PixelsReader fileReader,
                                       PixelsReaderOption options) throws IOException {
        this.batchReader = fileReader.read(options);
        this.schema = fileReader.getFileSchema();
        this.batch = schema.createRowBatch();
        rowInBatch = 0;
        this.row = (V) PixelsStruct.createValue(schema);
    }

    /**
     * If the current batch is empty, get a new one.
     * @return true if we have rows available.
     * @throws IOException
     */
    boolean ensureBatch() throws IOException {
        if (rowInBatch >= batch.size) {
            rowInBatch = 0;
            return batchReader.readBatch().endOfFile;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        batchReader.close();
    }

    @Override
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext taskAttemptContext) {
        // nothing required
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!ensureBatch()) {
            return false;
        }
        if (schema.getCategory() == TypeDescription.Category.STRUCT) {
            PixelsStruct result = (PixelsStruct) row;
            List<TypeDescription> children = schema.getChildren();
            int numberOfChildren = children.size();
            for (int i = 0; i < numberOfChildren; ++i) {
                result.setFieldValue(i, PixelsMapredRecordReader.nextValue(batch.cols[i], rowInBatch,
                        children.get(i), result.getFieldValue(i)));
            }
        } else {
            PixelsMapredRecordReader.nextValue(batch.cols[0], rowInBatch, schema, row);
        }
        rowInBatch += 1;
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return row;
    }

    @Override
    public float getProgress() throws IOException {
        // todo batchReader.getProgress()
        return 0;
    }
}
