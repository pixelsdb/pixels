package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanColumnReader
        extends ColumnReader
{
    public BooleanColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Read input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param size     number of values to read
     * @param vector   vector to read into
     */
    @Override
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, ColumnVector vector)
    {
        int index = 0;
        // input byte length should be GE than (num * 8) and be L than (num + 1) * 8
        checkArgument(input.length >= size * 8 && input.length < (size + 1) * 8,
                "Input bytes length " + input.length + " is not correct");
        for (int i = 0; i < input.length && index < size; i++) {
            for (int j = 7; j >= 0; j--) {
                vector.add(((0x1 << i) & input[i]) == (byte) 1);
                index++;
            }
        }
    }
}
