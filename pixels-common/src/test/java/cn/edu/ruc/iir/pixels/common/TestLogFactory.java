package cn.edu.ruc.iir.pixels.common;

import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import org.junit.Test;

import java.io.IOException;

public class TestLogFactory
{
    @Test
    public void test ()
    {
        LogFactory.Instance().getLog().error("hello world.", new IOException());
    }
}
