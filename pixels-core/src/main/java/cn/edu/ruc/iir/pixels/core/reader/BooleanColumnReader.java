package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanColumnReader
        extends ColumnReader
{
    private byte[] bits;

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
        if (offset == 0) {
            bits = bitWiseDeCompact(input);
        }
        for (int i = 0; i < size; i++) {
            vector.add(bits[i + offset] == 1);
        }
    }

    private byte[] bitWiseDeCompact(byte[] input)
    {
        byte[] result = new byte[input.length * 8];

        int bitsToRead = 1;
        int bitsLeft = 8;
        int current = 0;
        byte mask = 0x01;

        int index = 0;
        for (byte b : input) {
            while (bitsLeft > 0) {
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
