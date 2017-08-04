package cn.edu.ruc.iir.pixels.core;

import org.junit.Test;

/**
 * pixel
 *
 * @author guodong
 */
public class TestPixel
{
    @Test
    public void test ()
    {
        Pixels.FileTail.Builder fileTailBuilder = Pixels.FileTail.newBuilder().setFooterLength(1);
        Pixels.FileTail fileTail = fileTailBuilder.build();
        System.out.println(fileTail.toString());
        // fileTail.to
    }
}
