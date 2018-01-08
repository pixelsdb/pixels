package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.encoding.IntDecoder;
import cn.edu.ruc.iir.pixels.core.encoding.RleDecoder;
import cn.edu.ruc.iir.pixels.core.encoding.RleEncoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

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
        long[] values = {100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L};
        RleEncoder encoder = new RleEncoder(false, true);
        try
        {
            byte[] bytes = encoder.encode(values);
            IntDecoder decoder = new RleDecoder(new ByteArrayInputStream(bytes), false);
            while (decoder.hasNext()) {
                System.out.println(decoder.next());
            }
//            System.out.println(bytes.length);
//            System.out.println(Arrays.toString(bytes));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void test()
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
}
