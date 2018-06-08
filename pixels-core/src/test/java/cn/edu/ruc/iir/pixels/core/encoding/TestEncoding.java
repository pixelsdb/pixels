package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
        for (int i = 3; i < TestParams.rowNum; i++) {
            values[i] = random.nextInt();
        }
        long[] decoderValues = new long[TestParams.rowNum];
        RunLenIntEncoder encoder = new RunLenIntEncoder(true, true);
        try {
            byte[] bytes = encoder.encode(values);
            IntDecoder decoder = new RunLenIntDecoder(new ByteArrayInputStream(bytes), true);
            int i = 0;
            while (decoder.hasNext()) {
                decoderValues[i++] = decoder.next();
            }
            System.out.println(bytes.length);
            assertArrayEquals(values, decoderValues);
        }
        catch (IOException e) {
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
        for (int i = 0; i < 256; i++) {
            int b = (Byte.MIN_VALUE + i) & 0xff;
            bytes[i] = b;
            System.out.println(b);
        }
        for (int i = 0; i < 256; i++) {
            byte b = (byte) (bytes[i]);
            System.out.println(b);
        }
    }

    @Test
    public void bitWistCompactTest()
    {
        boolean[] exp = new boolean[TestParams.rowNum];
        long[] cur = new long[TestParams.rowNum];
        for (int i = 0; i < TestParams.rowNum; i++) {
            cur[i] = i > 25 ? 1 : 0;
            exp[i] = i > 25;
        }
        byte[] input = BitUtils.bitWiseCompact(cur, TestParams.rowNum);

        boolean[] res = new boolean[TestParams.rowNum];
        byte[] bytesRes = BitUtils.bitWiseDeCompact(input);
        for (int i = 0; i < TestParams.rowNum; i++) {
            res[i] = bytesRes[i] == 1;
        }
        assertArrayEquals(exp, res);

        int offset = 20;
        int size = 9;
        byte[] result = BitUtils.bitWiseDeCompact(input, offset, size);
        for (int i = 0; i < 6; i++)
        {
            assertEquals(0, result[i]);
        }
        for (int i = 6; i < size; i++)
        {
            assertEquals(1, result[i]);
        }
    }
}
