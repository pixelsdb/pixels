package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * pixels
 *
 * @author guodong
 */
public class TestEncoding
{
    @Test
    public void runLengthTest()
    {
        Random random = new Random();
        long[] values = new long[TestParams.rowNum];
        values[0] = 0;
        values[1] = -1;
        values[2] = -2;
        for (int i = 3; i < TestParams.rowNum; i++)
        {
            values[i] = random.nextInt();
        }
        long[] decoderValues = new long[TestParams.rowNum];
        RunLenIntEncoder encoder = new RunLenIntEncoder(true, true);
        try
        {
            byte[] bytes = encoder.encode(values);
            IntDecoder decoder = new RunLenIntDecoder(new ByteArrayInputStream(bytes), true);
            int i = 0;
            while (decoder.hasNext())
            {
                decoderValues[i++] = decoder.next();
            }
            System.out.println(bytes.length);
            assertArrayEquals(values, decoderValues);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void longTest()
    {
        long value = 0L;
        int high = (int) Math.pow(2, 30);
        long highL = (long) high;
        int low = 2;
        value |= (highL << 32);
        value |= low;
        System.out.println(value);

        long lowMask = 0x0000ffff;
        long highMask = 0xffff0000;
        int l = (int) (value & lowMask);
        long temp = (value & highMask);
        int h = (int) ((value & highMask) >>> 32);
        System.out.println("temp: " + temp + ", high: " + h + ", low: " + l);
    }

    @Test
    public void byteTest()
    {
        int[] bytes = new int[256];
        for (int i = 0; i < 256; i++)
        {
            int b = (Byte.MIN_VALUE + i) & 0xff;
            bytes[i] = b;
            System.out.println(b);
        }
        for (int i = 0; i < 256; i++)
        {
            byte b = (byte) (bytes[i]);
            System.out.println(b);
        }
    }

    @Test
    public void booleanBitWistCompactTest()
    {
        TestParams.rowNum = 300;
        boolean[] exp = new boolean[TestParams.rowNum];
        boolean[] cur = new boolean[TestParams.rowNum];
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            cur[i] = i > 25;
            exp[i] = i > 25;
        }
        byte[] input = BitUtils.bitWiseCompact(cur, TestParams.rowNum);

        boolean[] res = new boolean[TestParams.rowNum];
        byte[] bytesRes = new byte[input.length * 8];
        BitUtils.bitWiseDeCompact(bytesRes, input);
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            res[i] = bytesRes[i] == 1;
        }
        assertArrayEquals(exp, res);

        bytesRes = new byte[8];
        res = new boolean[8];
        BitUtils.bitWiseDeCompact(bytesRes, input, 3, 1);
        for (int i = 0; i < 8; i++)
        {
            res[i] = bytesRes[i] == 1;
        }
        exp = new boolean[]{false, false, true, true, true, true, true, true};
        assertArrayEquals(exp, res);
    }

    @Test
    public void byteBitWiseCompactTest()
    {
        TestParams.rowNum = 300;
        byte[] exp = new byte[TestParams.rowNum];
        byte[] cur = new byte[TestParams.rowNum];
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            cur[i] = i > 25 ? (byte)1  : (byte) 0;
            exp[i] = i > 25 ? (byte)1  : (byte) 0;
        }

        byte[] compactedBytes = BitUtils.bitWiseCompact(cur, TestParams.rowNum);
        byte[] bytesRes = new byte[compactedBytes.length * 8];
        BitUtils.bitWiseDeCompact(bytesRes, compactedBytes);
        byte[] bytes = new byte[TestParams.rowNum];
        System.arraycopy(bytesRes, 0, bytes, 0, TestParams.rowNum);
        assertArrayEquals(exp, bytes);
    }
}
