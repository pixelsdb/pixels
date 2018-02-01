package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.core.TestParams;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;

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
        for (int i = 0; i < TestParams.rowNum; i++) {
            values[i] = random.nextInt(10000000);
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
}
