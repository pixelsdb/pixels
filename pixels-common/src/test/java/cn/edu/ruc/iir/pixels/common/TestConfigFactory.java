package cn.edu.ruc.iir.pixels.common;


import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.junit.Test;

public class TestConfigFactory
{
    @Test
    public void test ()
    {
        System.out.println(ConfigFactory.Instance().getProperty("hello"));
    }
}
