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

    public static byte[] bitWiseDeCompact(byte[] input)
    {
        byte[] result = new byte[input.length * 8];

        int bitsToRead = 1;
        int bitsLeft = 8;
        int current = 0;
        byte mask = 0x01;

        int index = 0;
        for (byte b : input)
        {
            while (bitsLeft > 0)
            {
                bitsLeft -= bitsToRead;
                current = mask & (b >> bitsLeft);
                result[index] = (byte) current;
                index++;
            }
            bitsLeft = 8;
        }
        return result;
    }

    public static byte[] bitWiseDeCompact(byte[] input, int offset, int size)
    {
        byte[] result = new byte[size];
        int skipBytes = offset / 8;
        int skipBits = offset % 8;
        double res = Math.ceil((double)size / 8.0d);
        int readBytes = (int) res;

        int bitsToRead = 1;
        int bitsLeft = 8;
        int current = 0;
        byte mask = 0x01;

        int index = 0;
        for (int i = skipBytes; i < skipBytes + readBytes; i++)
        {
            while (bitsLeft > 0 && index < size)
            {
                bitsLeft -= bitsToRead;
                if (skipBits > 0)
                {
                    skipBits--;
                }
                else
                {
                    current = mask & (input[i] >> bitsLeft);
                    result[index] = (byte) current;
                    index++;
                }
            }
            bitsLeft = 8;
        }

        return result;
    }
}
