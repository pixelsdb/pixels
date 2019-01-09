package cn.edu.ruc.iir.pixels.listener;

import org.junit.Test;

/**
 * Created at: 19-1-10
 * Author: hank
 */
public class TestDecimal
{
    @Test
    public void test()
    {
        String a = "2.992888626509181E-5";
        System.out.println(Double.parseDouble(a)*123);
    }
}
