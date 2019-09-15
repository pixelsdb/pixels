package io.pixelsdb.pixels.common;


import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Test;

public class TestConfigFactory
{
    @Test
    public void test()
    {
        System.out.println(ConfigFactory.Instance().getProperty("hello"));
    }
}
