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
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.*;
import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.List;

/**
 * This record reader implements the org.apache.hadoop.mapred API.
 *
 * @param <V> the root type of the file
 */
public class PixelsMapredRecordReader<V extends WritableComparable>
        implements org.apache.hadoop.mapred.RecordReader<NullWritable, V> {
    private final TypeDescription schema;
    private final PixelsRecordReader batchReader;
    private final VectorizedRowBatch batch;
    private int rowInBatch;

    public PixelsMapredRecordReader(PixelsRecordReader reader,
                                    TypeDescription schema) throws IOException {
        this.batchReader = reader;
        this.batch = schema.createRowBatch();
        this.schema = schema;
        rowInBatch = 0;
    }

    protected PixelsMapredRecordReader(PixelsReader fileReader,
                                       PixelsReaderOption options) throws IOException {
        this.batchReader = fileReader.read(options);
        this.schema = fileReader.getFileSchema();
        this.batch = schema.createRowBatch();
        rowInBatch = 0;
    }

    /**
     * If the current batch is empty, get a new one.
     *
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
    public boolean next(NullWritable key, V value) throws IOException {
        if (!ensureBatch()) {
            return false;
        }
        if (schema.getCategory() == TypeDescription.Category.STRUCT) {
            PixelsStruct result = (PixelsStruct) value;
            List<TypeDescription> children = schema.getChildren();
            int numberOfChildren = children.size();
            for (int i = 0; i < numberOfChildren; ++i) {
                result.setFieldValue(i, nextValue(batch.cols[i], rowInBatch,
                        children.get(i), result.getFieldValue(i)));
            }
        } else {
            nextValue(batch.cols[0], rowInBatch, schema, value);
        }
        rowInBatch += 1;
        return true;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public V createValue() {
        return (V) PixelsStruct.createValue(schema);
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        batchReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    static BooleanWritable nextBoolean(ColumnVector vector,
                                       int row,
                                       Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            BooleanWritable result;
            if (previous == null || previous.getClass() != BooleanWritable.class) {
                result = new BooleanWritable();
            } else {
                result = (BooleanWritable) previous;
            }
            result.set(((LongColumnVector) vector).vector[row] != 0);
            return result;
        } else {
            return null;
        }
    }

    static ByteWritable nextByte(ColumnVector vector,
                                 int row,
                                 Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            ByteWritable result;
            if (previous == null || previous.getClass() != ByteWritable.class) {
                result = new ByteWritable();
            } else {
                result = (ByteWritable) previous;
            }
            result.set((byte) ((LongColumnVector) vector).vector[row]);
            return result;
        } else {
            return null;
        }
    }

    static ShortWritable nextShort(ColumnVector vector,
                                   int row,
                                   Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            ShortWritable result;
            if (previous == null || previous.getClass() != ShortWritable.class) {
                result = new ShortWritable();
            } else {
                result = (ShortWritable) previous;
            }
            result.set((short) ((LongColumnVector) vector).vector[row]);
            return result;
        } else {
            return null;
        }
    }

    static IntWritable nextInt(ColumnVector vector,
                               int row,
                               Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            IntWritable result;
            if (previous == null || previous.getClass() != IntWritable.class) {
                result = new IntWritable();
            } else {
                result = (IntWritable) previous;
            }
            result.set((int) ((LongColumnVector) vector).vector[row]);
            return result;
        } else {
            return null;
        }
    }

    static LongWritable nextLong(ColumnVector vector,
                                 int row,
                                 Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            LongWritable result;
            if (previous == null || previous.getClass() != LongWritable.class) {
                result = new LongWritable();
            } else {
                result = (LongWritable) previous;
            }
            result.set(((LongColumnVector) vector).vector[row]);
            return result;
        } else {
            return null;
        }
    }

    static FloatWritable nextFloat(ColumnVector vector,
                                   int row,
                                   Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            FloatWritable result;
            if (previous == null || previous.getClass() != FloatWritable.class) {
                result = new FloatWritable();
            } else {
                result = (FloatWritable) previous;
            }
            result.set((float) ((DoubleColumnVector) vector).vector[row]);
            return result;
        } else {
            return null;
        }
    }

    static DoubleWritable nextDouble(ColumnVector vector,
                                     int row,
                                     Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            DoubleWritable result;
            if (previous == null || previous.getClass() != DoubleWritable.class) {
                result = new DoubleWritable();
            } else {
                result = (DoubleWritable) previous;
            }
            result.set(((DoubleColumnVector) vector).vector[row]);
            return result;
        } else {
            return null;
        }
    }

    static Text nextString(ColumnVector vector,
                           int row,
                           Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            Text result;
            if (previous == null || previous.getClass() != Text.class) {
                result = new Text();
            } else {
                result = (Text) previous;
            }
            BytesColumnVector bytes = (BytesColumnVector) vector;
            result.set(bytes.vector[row], bytes.start[row], bytes.lens[row]);
            return result;
        } else {
            return null;
        }
    }

    static BytesWritable nextBinary(ColumnVector vector,
                                    int row,
                                    Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            BytesWritable result;
            if (previous == null || previous.getClass() != BytesWritable.class) {
                result = new BytesWritable();
            } else {
                result = (BytesWritable) previous;
            }
            BytesColumnVector bytes = (BytesColumnVector) vector;
            result.set(bytes.vector[row], bytes.start[row], bytes.lens[row]);
            return result;
        } else {
            return null;
        }
    }

    static DateWritable nextDate(ColumnVector vector,
                                 int row,
                                 Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            DateWritable result;
            if (previous == null || previous.getClass() != DateWritable.class) {
                result = new DateWritable();
            } else {
                result = (DateWritable) previous;
            }
            int date = (int) ((LongColumnVector) vector).vector[row];
            result.set(date);
            return result;
        } else {
            return null;
        }
    }

    static PixelsStruct nextStruct(ColumnVector vector,
                                   int row,
                                   TypeDescription schema,
                                   Object previous) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            PixelsStruct result;
            List<TypeDescription> childrenTypes = schema.getChildren();
            int numChildren = childrenTypes.size();
            if (previous == null || previous.getClass() != PixelsStruct.class) {
                result = new PixelsStruct(schema);
            } else {
                result = (PixelsStruct) previous;
            }
            StructColumnVector struct = (StructColumnVector) vector;
            for (int f = 0; f < numChildren; ++f) {
                result.setFieldValue(f, nextValue(struct.fields[f], row,
                        childrenTypes.get(f), result.getFieldValue(f)));
            }
            return result;
        } else {
            return null;
        }
    }

    public static WritableComparable nextValue(ColumnVector vector,
                                               int row,
                                               TypeDescription schema,
                                               Object previous) {
        switch (schema.getCategory()) {
            case BOOLEAN:
                return nextBoolean(vector, row, previous);
            case BYTE:
                return nextByte(vector, row, previous);
            case SHORT:
                return nextShort(vector, row, previous);
            case INT:
                return nextInt(vector, row, previous);
            case LONG:
                return nextLong(vector, row, previous);
            case FLOAT:
                return nextFloat(vector, row, previous);
            case DOUBLE:
                return nextDouble(vector, row, previous);
            case STRING:
            case CHAR:
            case VARCHAR:
                return nextString(vector, row, previous);
            case BINARY:
                return nextBinary(vector, row, previous);
            case DATE:
                return nextDate(vector, row, previous);
            case STRUCT:
                return nextStruct(vector, row, schema, previous);
            default:
                throw new IllegalArgumentException("Unknown type " + schema);
        }
    }
}
