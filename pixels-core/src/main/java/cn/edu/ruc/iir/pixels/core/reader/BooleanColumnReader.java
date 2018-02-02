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
    BooleanColumnReader(TypeDescription type)
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
        // input byte length should be GE than (num * 8) and be L than (num + 1) * 8
        checkArgument(input.length >= size / 8 && input.length <= (size + 8) / 8,
                "Input bytes length " + input.length + " is not correct");
        byte[] bits = bitWiseDeCompact(input, size);
        for (int i = 0; i < size; i++) {
            vector.add(bits[i] == 1);
        }
    }

    private byte[] bitWiseDeCompact(byte[] input, int size)
    {
        byte[] result = new byte[size];

        int bitsToRead = 1;
        int bitsLeft = 8;
        int current = 0;
        byte mask = 0x01;

        int index = 0;
        for (byte b : input) {
            while (bitsLeft > 0) {
                if (index >= size) {
                    return result;
                }
                bitsLeft -= bitsToRead;
                current = mask & (b >> bitsLeft);
                result[index] = (byte) current;
                index++;
            }
            bitsLeft = 8;
        }
        return result;
    }
}
