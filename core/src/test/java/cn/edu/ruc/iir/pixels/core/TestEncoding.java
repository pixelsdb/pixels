package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.encoding.IntDecoder;
import cn.edu.ruc.iir.pixels.core.encoding.RleDecoder;
import cn.edu.ruc.iir.pixels.core.encoding.RleEncoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

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
        long[] values = {100L, 100L, 100L, 100L, 100L, 100L, 102L, 101L, 90L, 100L, 100L};
        RleEncoder encoder = new RleEncoder(false, true);
        try
        {
            byte[] bytes = encoder.encode(values);
            IntDecoder decoder = new RleDecoder(new ByteArrayInputStream(bytes), false);
            while (decoder.hasNext()) {
                System.out.println(decoder.next());
            }
            System.out.println(bytes.length);
            System.out.println(Arrays.toString(bytes));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
