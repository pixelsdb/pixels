package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;

/**
 * pixels column reader.
 * Read from file, and decode column values
 *
 * @author guodong
 */
public abstract class ColumnReader
{
    private final TypeDescription type;

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

    public byte[][] readBinaries(byte[] input, boolean isEncoding, int num)
    {
        throw new UnsupportedOperationException("Cannot read binaries");
    }

    public boolean[] readBooleans(byte[] input, int num)
    {
        throw new UnsupportedOperationException("Cannot read booleans");
    }

    public byte[] readBytes(byte[] input, boolean isEncoding, int num)
    {
        throw new UnsupportedOperationException("Cannot read bytes");
    }

    public char[] readChars(byte[] input, boolean isEncoding, int num)
    {
        throw new UnsupportedOperationException("Cannot read chars");
    }

    public double[] readDoubles(byte[] input, int num) throws IOException
    {
        throw new UnsupportedOperationException("Cannot read doubles");
    }

    public float[] readFloats(byte[] input, int num) throws IOException
    {
        throw new UnsupportedOperationException("Cannot read floats");
    }

    public long[] readInts(byte[] input, boolean isEncoding, int num) throws IOException
    {
        throw new UnsupportedOperationException("Cannot read integers");
    }

    public String[] readStrings(byte[] input, boolean isEncoding, int num)
    {
        throw new UnsupportedOperationException("Cannot read strings");
    }

    public Timestamp[] readTimestamps(byte[] input, boolean isEncoding, int num) throws IOException
    {
        throw new UnsupportedOperationException("Cannot read timestamps");
    }

    public char[][] readVarchars(byte[] input, boolean isEncoding, int num)
    {
        throw new UnsupportedOperationException("Cannot read varchars");
    }

    public ColumnReader(TypeDescription type)
    {
        this.type = type;
    }

    public TypeDescription getType()
    {
        return type;
    }
}
