package cn.edu.ruc.iir.pixels.core.utils;

import java.io.ByteArrayOutputStream;

/**
 * pixels
 *
 * @author guodong
 */
public class BitUtils
{
    private BitUtils()
    {}

    public static byte[] bitWiseCompact(boolean[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        int bitsToWrite = 1;
        int bitsLeft = 8;
        byte current = 0;

        for (int i = 0; i < length; i++) {
            long v = values[i] ? 1 : 0;
            bitsLeft -= bitsToWrite;
            current |= v << bitsLeft;
            if (bitsLeft == 0) {
                bitWiseOutput.write(current);
                current = 0;
                bitsLeft = 8;
            }
        }

        if (bitsLeft != 8) {
            bitWiseOutput.write(current);
        }

        return bitWiseOutput.toByteArray();
    }

    public static byte[] bitWiseCompact(long[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        int bitsToWrite = 1;
        int bitsLeft = 8;
        byte current = 0;

        for (int i = 0; i < length; i++) {
            long v = values[i];
            bitsLeft -= bitsToWrite;
            current |= v << bitsLeft;
            if (bitsLeft == 0) {
                bitWiseOutput.write(current);
                current = 0;
                bitsLeft = 8;
            }
        }

        if (bitsLeft != 8) {
            bitWiseOutput.write(current);
        }

        return bitWiseOutput.toByteArray();
    }
}
