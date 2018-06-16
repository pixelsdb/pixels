package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.IOException;

/**
 * pixels column reader.
 * Read from file, and decode column values
 *
 * @author guodong
 */
public abstract class ColumnReader
{
    private final TypeDescription type;

    int elementIndex = 0;
    boolean hasNull = true;

    public static ColumnReader newColumnReader(TypeDescription type)
    {
        switch (type.getCategory())
        {
            case BINARY:
                return new BinaryColumnReader(type);
            case BOOLEAN:
                return new BooleanColumnReader(type);
            case BYTE:
                return new ByteColumnReader(type);
            case CHAR:
                return new CharColumnReader(type);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnReader(type);
            case DOUBLE:
                return new DoubleColumnReader(type);
            case FLOAT:
                return new FloatColumnReader(type);
            case STRING:
                return new StringColumnReader(type);
            case TIMESTAMP:
                return new TimestampColumnReader(type);
            case VARCHAR:
                return new VarcharColumnReader(type);
                default:
                    throw new IllegalArgumentException("Bad schema type: " + type.getCategory());
        }
    }

    /**
     * Read values from input buffer.
     * Values after specified offset are gonna be put into the specified vector.
     * @param input input buffer
     * @param encoding encoding type
     * @param offset starting reading offset of values
     * @param size number of values to read
     * @param vector vector to read into
     * @throws java.io.IOException
     * */
    public abstract void read(byte[] input, PixelsProto.ColumnEncoding encoding,
                              int offset, int size, int pixelStride, final int vectorIndex,
                              ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
            throws IOException;

    public ColumnReader(TypeDescription type)
    {
        this.type = type;
    }

    public TypeDescription getType()
    {
        return type;
    }
}
