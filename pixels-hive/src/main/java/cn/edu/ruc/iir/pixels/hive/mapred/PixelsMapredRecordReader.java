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
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.*;
import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import cn.edu.ruc.iir.pixels.hive.PixelsFile;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * This record reader implements the org.apache.hadoop.mapred API.
 * refer: [RecordReaderImpl](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/RecordReaderImpl.java)
 *
 * @param <V> the root type of the file
 */
public class PixelsMapredRecordReader<V extends WritableComparable>
        implements org.apache.hadoop.mapred.RecordReader<NullWritable, PixelsStruct>, StatsProvidingRecordReader
{
    private static Logger log = LogManager.getLogger(PixelsMapredRecordReader.class);
    private static final int BATCH_SIZE = 10000;

    private final TypeDescription schema;
    private final PixelsRecordReader batchReader;
    private VectorizedRowBatch batch;
    private int rowInBatch;
    private List<Integer> included;
    private List<TypeDescription> columnTypes;
    private int numColumn;
    private final SerDeStats stats;
    private final PixelsReader file;

    public PixelsMapredRecordReader(PixelsReader fileReader,
                                    PixelsFile.ReaderOptions options) throws IOException
    {
        this.file = fileReader;
        this.batchReader = fileReader.read(options.getOption());
        this.schema = fileReader.getFileSchema();
        // schema should be of struct type.
        assert schema.getCategory() == TypeDescription.Category.STRUCT;
        this.columnTypes = schema.getChildren();
        this.numColumn = columnTypes.size();
        this.batch = batchReader.readBatch(BATCH_SIZE);
        this.rowInBatch = 0;
        this.included = options.getIncluded();
        this.stats = new SerDeStats();
    }

    /**
     * If the current batch is empty, get a new one.
     *
     * @return true if we have rows available.
     * @throws IOException
     */
    boolean ensureBatch() throws IOException
    {
        if (rowInBatch >= batch.size)
        {
            rowInBatch = 0;
            batch = batchReader.readBatch(BATCH_SIZE);
            if (this.batch.size <= 0 || this.batch.endOfFile)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean next(NullWritable key, PixelsStruct value) throws IOException
    {
        // value is created by createValue, is should not be null.
        assert value != null;

        if (!ensureBatch())
        {
            return false;
        }

        if (this.included.size() == 0)
        {
            rowInBatch += 1;
            return true;
        }

        int numberOfIncluded = this.included.size();
        for (int i = 0; i < numberOfIncluded; ++i)
        {
            value.setFieldValue(included.get(i), nextValue(batch.cols[i], rowInBatch,
                    columnTypes.get(included.get(i)), value.getFieldValue(included.get(i))));
        }

        rowInBatch += 1;
        return true;
    }

    @Override
    public NullWritable createKey()
    {
        return NullWritable.get();
    }

    @Override
    public PixelsStruct createValue()
    {
        return new PixelsStruct(this.numColumn);
    }

    @Override
    public long getPos() throws IOException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        batchReader.close();
    }

    // todo get progress
    @Override
    public float getProgress() throws IOException
    {
        return 0;
    }

    static BooleanWritable nextBoolean(ColumnVector vector,
                                       int row,
                                       Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            BooleanWritable result;
            if (previous == null || previous.getClass() != BooleanWritable.class)
            {
                result = new BooleanWritable();
            } else
            {
                result = (BooleanWritable) previous;
            }
            result.set(((ByteColumnVector) vector).vector[row] != 0);
            return result;
        } else
        {
            return null;
        }
    }

    static ByteWritable nextByte(ColumnVector vector,
                                 int row,
                                 Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            ByteWritable result;
            if (previous == null || previous.getClass() != ByteWritable.class)
            {
                result = new ByteWritable();
            } else
            {
                result = (ByteWritable) previous;
            }
            result.set((byte) ((LongColumnVector) vector).vector[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static ShortWritable nextShort(ColumnVector vector,
                                   int row,
                                   Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            ShortWritable result;
            if (previous == null || previous.getClass() != ShortWritable.class)
            {
                result = new ShortWritable();
            } else
            {
                result = (ShortWritable) previous;
            }
            result.set((short) ((LongColumnVector) vector).vector[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static IntWritable nextInt(ColumnVector vector,
                               int row,
                               Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            IntWritable result;
            if (previous == null || previous.getClass() != IntWritable.class)
            {
                result = new IntWritable();
            } else
            {
                result = (IntWritable) previous;
            }
            result.set((int) ((LongColumnVector) vector).vector[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static LongWritable nextLong(ColumnVector vector,
                                 int row,
                                 Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            LongWritable result;
            if (previous == null || previous.getClass() != LongWritable.class)
            {
                result = new LongWritable();
            } else
            {
                result = (LongWritable) previous;
            }
            result.set(((LongColumnVector) vector).vector[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static FloatWritable nextFloat(ColumnVector vector,
                                   int row,
                                   Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            FloatWritable result;
            if (previous == null || previous.getClass() != FloatWritable.class)
            {
                result = new FloatWritable();
            } else
            {
                result = (FloatWritable) previous;
            }
            result.set((float) ((DoubleColumnVector) vector).vector[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static DoubleWritable nextDouble(ColumnVector vector,
                                     int row,
                                     Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            DoubleWritable result;
            if (previous == null || previous.getClass() != DoubleWritable.class)
            {
                result = new DoubleWritable();
            } else
            {
                result = (DoubleWritable) previous;
            }
            result.set(((DoubleColumnVector) vector).vector[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static Text nextString(ColumnVector vector,
                           int row,
                           Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            Text result;
            if (previous == null || previous.getClass() != Text.class)
            {
                result = new Text();
            } else
            {
                result = (Text) previous;
            }
            BinaryColumnVector bytes = (BinaryColumnVector) vector;

            result.set(bytes.vector[row], bytes.start[row], bytes.lens[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static BytesWritable nextBinary(ColumnVector vector,
                                    int row,
                                    Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            BytesWritable result;
            if (previous == null || previous.getClass() != BytesWritable.class)
            {
                result = new BytesWritable();
            } else
            {
                result = (BytesWritable) previous;
            }
            BinaryColumnVector bytes = (BinaryColumnVector) vector;
            result.set(bytes.vector[row], bytes.start[row], bytes.lens[row]);
            return result;
        } else
        {
            return null;
        }
    }

    static DateWritable nextDate(ColumnVector vector,
                                 int row,
                                 Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            DateWritable result;
            if (previous == null || previous.getClass() != DateWritable.class)
            {
                result = new DateWritable();
            } else
            {
                result = (DateWritable) previous;
            }
            int date = (int) ((LongColumnVector) vector).vector[row];
            result.set(date);
            return result;
        } else
        {
            return null;
        }
    }

    PixelsStruct nextStruct(ColumnVector vector,
                            int row,
                            TypeDescription schema,
                            Object previous)
    {
        if (vector.isRepeating)
        {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row])
        {
            PixelsStruct result;
            List<TypeDescription> childrenTypes = schema.getChildren();
            int numChildren = childrenTypes.size();
            if (previous == null || previous.getClass() != PixelsStruct.class)
            {
                result = new PixelsStruct(numChildren);
            } else
            {
                result = (PixelsStruct) previous;
            }
            StructColumnVector struct = (StructColumnVector) vector;
            for (int f = 0; f < numChildren; ++f)
            {
                result.setFieldValue(f, nextValue(struct.fields[f], row,
                        childrenTypes.get(f), result.getFieldValue(f)));
            }
            return result;
        } else
        {
            return null;
        }
    }

    public Object nextValue(ColumnVector vector,
                            int row,
                            TypeDescription schema,
                            Object previous)
    {
        switch (schema.getCategory())
        {
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

    @Override
    public SerDeStats getStats()
    {
        stats.setRawDataSize(file.getCompressionBlockSize());
        stats.setRowCount(file.getNumberOfRows());
        return stats;
    }
}
