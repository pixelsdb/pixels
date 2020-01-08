package io.pixelsdb.pixels.core.utils;

import org.junit.Test;

/**
 * Created at: 20-1-8
 * Author: hank
 */
public class TestDynamicIntArray
{
    @Test
    public void test ()
    {
        DynamicIntArray array = new DynamicIntArray(64);
        for (int i = 0; i < 1024; ++i)
        {
            array.add(i);
        }
        int[] ints = array.toArray();
        for (int i = 0; i < array.size(); ++i)
        {
            assert i == ints[i];
        }
    }
}
