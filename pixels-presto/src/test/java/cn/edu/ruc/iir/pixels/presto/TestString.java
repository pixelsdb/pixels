package cn.edu.ruc.iir.pixels.presto;

import org.junit.Test;

public class TestString
{
    @Test
    public void test ()
    {
        byte[] bytes = "hello world".getBytes();
        System.out.println(new String(bytes));
    }
}
