package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

/**
 * pixels
 *
 * @author guodong
 */
public class TestDynamicArray
{
    private static final int TEST_NUM = 102508;

    @Test
    public void testAdd()
    {
        DynamicArray<Integer> array = new DynamicArray<>();
        for (int i = 0; i < TEST_NUM; i++) {
            array.add(i);
        }
        assert array.size() == TEST_NUM;
        System.out.println("Capacity: " + array.capacity());
        for (int i = 0; i < TEST_NUM; i++) {
            assert i == array.get(i);
        }
    }

    @Test
    public void testSet()
    {
        DynamicArray<Integer> array = new DynamicArray<>();
        for (int i = 0; i < TEST_NUM; i++) {
            array.add(i);
        }
        for (int i = 0; i < TEST_NUM; i++) {
            array.set(i, i * 2);
        }
        assert array.size() == TEST_NUM;
        for (int i = 0; i < TEST_NUM; i++) {
            assert i * 2 == array.get(i);
        }
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testException()
    {
        DynamicArray<Integer> array = new DynamicArray<>();
        array.get(Integer.MAX_VALUE);
    }
}
