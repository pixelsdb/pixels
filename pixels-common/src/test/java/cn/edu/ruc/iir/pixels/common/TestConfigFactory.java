package cn.edu.ruc.iir.pixels.common;


import org.junit.Test;

public class TestConfigFactory
{
    @Test
    public void test ()
    {
        System.out.println(ConfigFactory.Instance().getProperty("hello"));
    }
}
