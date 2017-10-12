package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.encoding.RleEncoder;
import org.junit.Test;

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
        long[] values = {100L, 100L, 100L, 100L, 101L, 102L, 103L, 104L, 105L, 106L, 200L, 99L};
        RleEncoder encoder = new RleEncoder(false, true);
        try
        {
            byte[] bytes = encoder.encode(values);
            System.out.println(bytes.length);
            System.out.println(Arrays.toString(bytes));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
