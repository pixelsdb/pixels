package cn.edu.ruc.iir.pixels.common;

public class TestConfigFactory
{
    public static void main(String[] args)
    {
        System.out.println(ConfigFactory.Instance().getProperty("hello"));
    }
}
