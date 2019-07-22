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
package io.pixelsdb.pixels.hive.mapred;

import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.hive.common.PixelsStruct;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.StructColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.hive.PixelsSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * This class is currently not finished, so that write is not supported by pixels-hive.
 *
 * refers to {@link org.apache.hadoop.hive.ql.io.orc.WriterImpl}
 *
 */
public class PixelsMapredRecordWriter
        implements RecordWriter<NullWritable, PixelsSerDe.PixelsRow>
{
    private final PixelsWriter writer;
    private final VectorizedRowBatch batch;
    private final TypeDescription schema;
    private final ObjectInspector inspector;
    private final StructField[] fields;

    public PixelsMapredRecordWriter(PixelsWriter writer)
    {
        this.writer = writer;
        this.schema = writer.getSchema();
        this.inspector = null;
        this.batch = schema.createRowBatch();
        this.fields = initializeFieldsFromOi(inspector);
    }

    private static StructField[] initializeFieldsFromOi(ObjectInspector inspector)
    {
        if (inspector instanceof StructObjectInspector)
        {
            List<? extends StructField> fieldList =
                    ((StructObjectInspector) inspector).getAllStructFieldRefs();
            StructField[] fields = new StructField[fieldList.size()];
            fieldList.toArray(fields);
            return fields;
        } else
        {
            return null;
        }
    }

    static void setLongValue(ColumnVector vector, int row, long value)
    {
        ((LongColumnVector) vector).vector[row] = value;
    }

    static void setDoubleValue(ColumnVector vector, int row, double value)
    {
        ((DoubleColumnVector) vector).vector[row] = Double.doubleToLongBits(value);
    }

    static void setBinaryValue(ColumnVector vector, int row,
                               BinaryComparable value)
    {
        ((BinaryColumnVector) vector).setVal(row, value.getBytes(), 0,
                value.getLength());
    }

    static void setBinaryValue(ColumnVector vector, int row,
                               BinaryComparable value, int maxLength)
    {
        ((BinaryColumnVector) vector).setVal(row, value.getBytes(), 0,
                Math.min(maxLength, value.getLength()));
    }

    private static final ThreadLocal<byte[]> SPACE_BUFFER =
            new ThreadLocal<byte[]>()
            {
                @Override
                protected byte[] initialValue()
                {
                    byte[] result = new byte[100];
                    Arrays.fill(result, (byte) ' ');
                    return result;
                }
            };

    static void setCharValue(BinaryColumnVector vector,
                             int row,
                             Text value,
                             int length)
    {
        // we need to trim or pad the string with spaces to required length
        int actualLength = value.getLength();
        if (actualLength >= length)
        {
            setBinaryValue(vector, row, value, length);
        } else
        {
            byte[] spaces = SPACE_BUFFER.get();
            if (length - actualLength > spaces.length)
            {
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
                               PixelsStruct value)
    {
        List<TypeDescription> children = schema.getChildren();
        for (int c = 0; c < value.getNumFields(); ++c)
        {
            setColumn(children.get(c), vector.fields[c], row, value.getFieldValue(c));
        }
    }

    public static void setColumn(TypeDescription schema,
                                 ColumnVector vector,
                                 int row,
                                 Object value)
    {
        if (value == null)
        {
            vector.noNulls = false;
            vector.isNull[row] = true;
        } else
        {
            switch (schema.getCategory())
            {
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
                    setCharValue((BinaryColumnVector) vector, row, (Text) value,
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

    static void setColumn(int rowId, ColumnVector column,
                          ObjectInspector inspector, Object obj)
    {
        if (obj == null)
        {
            column.noNulls = false;
            column.isNull[rowId] = true;
        } else
        {
            switch (inspector.getCategory())
            {
                case PRIMITIVE:
                    switch (((PrimitiveObjectInspector) inspector)
                            .getPrimitiveCategory())
                    {
                        case BOOLEAN:
                        {
                            LongColumnVector vector = (LongColumnVector) column;
                            vector.vector[rowId] =
                                    ((BooleanObjectInspector) inspector).get(obj) ? 1 : 0;
                            break;
                        }
                        case BYTE:
                        {
                            LongColumnVector vector = (LongColumnVector) column;
                            vector.vector[rowId] = ((ByteObjectInspector) inspector).get(obj);
                            break;
                        }
                        case SHORT:
                        {
                            LongColumnVector vector = (LongColumnVector) column;
                            vector.vector[rowId] =
                                    ((ShortObjectInspector) inspector).get(obj);
                            break;
                        }
                        case INT:
                        {
                            LongColumnVector vector = (LongColumnVector) column;
                            vector.vector[rowId] = ((IntObjectInspector) inspector).get(obj);
                            break;
                        }
                        case LONG:
                        {
                            LongColumnVector vector = (LongColumnVector) column;
                            vector.vector[rowId] = ((LongObjectInspector) inspector).get(obj);
                            break;
                        }
                        case FLOAT:
                        {
                            DoubleColumnVector vector = (DoubleColumnVector) column;
                            vector.vector[rowId] =
                                    Float.floatToIntBits(((FloatObjectInspector) inspector).get(obj));
                            break;
                        }
                        case DOUBLE:
                        {
                            DoubleColumnVector vector = (DoubleColumnVector) column;
                            vector.vector[rowId] =
                                    Double.doubleToLongBits(((DoubleObjectInspector) inspector).get(obj));
                            break;
                        }
                        case BINARY:
                        {
                            BinaryColumnVector vector = (BinaryColumnVector) column;
                            BytesWritable blob = ((BinaryObjectInspector) inspector)
                                    .getPrimitiveWritableObject(obj);
                            vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
                            break;
                        }
                        case STRING:
                        {
                            BinaryColumnVector vector = (BinaryColumnVector) column;
                            Text blob = ((StringObjectInspector) inspector)
                                    .getPrimitiveWritableObject(obj);
                            vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
                            break;
                        }
                        case VARCHAR:
                        {
                            BinaryColumnVector vector = (BinaryColumnVector) column;
                            Text blob = ((HiveVarcharObjectInspector) inspector)
                                    .getPrimitiveWritableObject(obj).getTextValue();
                            vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
                            break;
                        }
                        case CHAR:
                        {
                            BinaryColumnVector vector = (BinaryColumnVector) column;
                            Text blob = ((HiveCharObjectInspector) inspector)
                                    .getPrimitiveWritableObject(obj).getTextValue();
                            vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
                            break;
                        }
                        case TIMESTAMP:
                        {
                            TimestampColumnVector vector = (TimestampColumnVector) column;
                            Timestamp ts = ((TimestampObjectInspector) inspector)
                                    .getPrimitiveJavaObject(obj);
                            vector.set(rowId, ts);
                            break;
                        }
                        case DATE:
                        {
                            LongColumnVector vector = (LongColumnVector) column;
                            vector.vector[rowId] = ((DateObjectInspector) inspector)
                                    .getPrimitiveWritableObject(obj).getDays();
                            break;
                        }
                    }
                    break;
                case STRUCT:
                {
                    StructColumnVector vector = (StructColumnVector) column;
                    StructObjectInspector oi = (StructObjectInspector) inspector;
                    List<? extends StructField> fields = oi.getAllStructFieldRefs();
                    for (int c = 0; c < vector.fields.length; ++c)
                    {
                        StructField field = fields.get(c);
                        setColumn(rowId, vector.fields[c], field.getFieldObjectInspector(),
                                oi.getStructFieldData(obj, field));
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown ObjectInspector kind " +
                            inspector.getCategory());
            }
        }
    }

    @Override
    public void write(NullWritable nullWritable, PixelsSerDe.PixelsRow row) throws IOException
    {
        // if the batch is full, write it out.
        if (batch.size == batch.getMaxSize())
        {
            writer.addRowBatch(batch);
            batch.reset();
        }

        // add the new row
        int rowId = batch.size++;
        // skip over the PixelsKey or PixelsValue
        if (fields != null)
        {
            StructObjectInspector soi = (StructObjectInspector) inspector;
            for (int i = 0; i < fields.length; ++i)
            {
                setColumn(rowId, batch.cols[i],
                        fields[i].getFieldObjectInspector(),
                        soi.getStructFieldData(row, fields[i]));
            }
        } else
        {
            setColumn(rowId, batch.cols[0], inspector, row);
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException
    {
        if (batch != null && batch.size != 0)
        {
            writer.addRowBatch(batch);
            batch.reset();
        }
        if (writer != null)
        {
            writer.close();
        }
    }
}
