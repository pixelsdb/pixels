package cn.edu.ruc.iir.pixels.hive.common;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.*;

import java.util.List;

/**
 * Created at: 19-6-30
 * Author: hank
 */
public class PixelsValue
{
    private static BooleanWritable nextBoolean(ColumnVector vector,
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

    private static ByteWritable nextByte(ColumnVector vector,
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

    private static ShortWritable nextShort(ColumnVector vector,
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

    private static IntWritable nextInt(ColumnVector vector,
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

    private static LongWritable nextLong(ColumnVector vector,
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

    private static FloatWritable nextFloat(ColumnVector vector,
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

    private static DoubleWritable nextDouble(ColumnVector vector,
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

    private static Text nextString(ColumnVector vector,
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

    private static BytesWritable nextBinary(ColumnVector vector,
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

    private static DateWritable nextDate(ColumnVector vector,
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

    private static PixelsStruct nextStruct(ColumnVector vector,
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

    public static Writable nextValue(ColumnVector vector,
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
}
