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
import cn.edu.ruc.iir.pixels.core.vector.*;
import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
/**
 * This record writer implements the org.apache.hadoop.mapred API.
 * refer: [WriterImpl](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/WriterImpl.java)
 * @param <V> the root type of the file
 */
public class PixelsMapredRecordWriter<V extends Writable>
        implements RecordWriter<NullWritable, V> {
    private final PixelsWriter writer;
    private final VectorizedRowBatch batch;
    private final TypeDescription schema;
    private final boolean isTopStruct;

    public PixelsMapredRecordWriter(PixelsWriter writer) {
        this.writer = writer;
        schema = writer.getSchema();
        this.batch = schema.createRowBatch();
        isTopStruct = schema.getCategory() == TypeDescription.Category.STRUCT;
    }

    static void setLongValue(ColumnVector vector, int row, long value) {
        ((LongColumnVector) vector).vector[row] = value;
    }

    static void setDoubleValue(ColumnVector vector, int row, double value) {
        ((DoubleColumnVector) vector).vector[row] = value;
    }

    static void setBinaryValue(ColumnVector vector, int row,
                               BinaryComparable value) {
        ((BytesColumnVector) vector).setVal(row, value.getBytes(), 0,
                value.getLength());
    }

    static void setBinaryValue(ColumnVector vector, int row,
                               BinaryComparable value, int maxLength) {
        ((BytesColumnVector) vector).setVal(row, value.getBytes(), 0,
                Math.min(maxLength, value.getLength()));
    }

    private static final ThreadLocal<byte[]> SPACE_BUFFER =
            new ThreadLocal<byte[]>() {
                @Override
                protected byte[] initialValue() {
                    byte[] result = new byte[100];
                    Arrays.fill(result, (byte) ' ');
                    return result;
                }
            };

    static void setCharValue(BytesColumnVector vector,
                             int row,
                             Text value,
                             int length) {
        // we need to trim or pad the string with spaces to required length
        int actualLength = value.getLength();
        if (actualLength >= length) {
            setBinaryValue(vector, row, value, length);
        } else {
            byte[] spaces = SPACE_BUFFER.get();
            if (length - actualLength > spaces.length) {
                spaces = new byte[length - actualLength];
                Arrays.fill(spaces, (byte) ' ');
                SPACE_BUFFER.set(spaces);
            }
            vector.setConcat(row, value.getBytes(), 0, actualLength, spaces, 0,
                    length - actualLength);
        }
    }

    static void setStructValue(TypeDescription schema,
                               StructColumnVector vector,
                               int row,
                               PixelsStruct value) {
        List<TypeDescription> children = schema.getChildren();
        for (int c = 0; c < value.getNumFields(); ++c) {
            setColumn(children.get(c), vector.fields[c], row, value.getFieldValue(c));
        }
    }

    public static void setColumn(TypeDescription schema,
                                 ColumnVector vector,
                                 int row,
                                 Writable value) {
        if (value == null) {
            vector.noNulls = false;
            vector.isNull[row] = true;
        } else {
            switch (schema.getCategory()) {
                case BOOLEAN:
                    setLongValue(vector, row, ((BooleanWritable) value).get() ? 1 : 0);
                    break;
                case BYTE:
                    setLongValue(vector, row, ((ByteWritable) value).get());
                    break;
                case SHORT:
                    setLongValue(vector, row, ((ShortWritable) value).get());
                    break;
                case INT:
                    setLongValue(vector, row, ((IntWritable) value).get());
                    break;
                case LONG:
                    setLongValue(vector, row, ((LongWritable) value).get());
                    break;
                case FLOAT:
                    setDoubleValue(vector, row, ((FloatWritable) value).get());
                    break;
                case DOUBLE:
                    setDoubleValue(vector, row, ((DoubleWritable) value).get());
                    break;
                case STRING:
                    setBinaryValue(vector, row, (Text) value);
                    break;
                case CHAR:
                    setCharValue((BytesColumnVector) vector, row, (Text) value,
                            schema.getMaxLength());
                    break;
                case VARCHAR:
                    setBinaryValue(vector, row, (Text) value, schema.getMaxLength());
                    break;
                case BINARY:
                    setBinaryValue(vector, row, (BytesWritable) value);
                    break;
                case DATE:
                    setLongValue(vector, row, ((DateWritable) value).getDays());
                    break;
                case STRUCT:
                    setStructValue(schema, (StructColumnVector) vector, row,
                            (PixelsStruct) value);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + schema);
            }
        }
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
                setColumn(schema.getChildren().get(f), batch.cols[f], row,
                        ((PixelsStruct) v).getFieldValue(f));
            }
        } else {
            setColumn(schema, batch.cols[0], row, v);
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }
}
