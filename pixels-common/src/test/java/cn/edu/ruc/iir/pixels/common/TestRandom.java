package cn.edu.ruc.iir.pixels.common;

import org.junit.Test;

import java.util.Random;

/**
 * Created at: 18-12-23
 * Author: hank
 */
public class TestRandom
{
    @Test
    public void test()
    {
        Random random = new Random(System.nanoTime());
        System.out.println(random.nextInt(1));
    }
}
