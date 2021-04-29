package io.pixelsdb.pixels.load;

import org.junit.Test;

/**
 * Created at: 29/04/2021
 * Author: hank
 */
public class TestSplitString
{
    @Test
    public void test()
    {
        String s = "1|3689999|O|224560.83|1996-01-02|5-LOW|Clerk#000095055|0|nstructions sleep furiously among |";
        String reg = "\\\\|";
        String splits[] = s.split(reg);
        System.out.println(splits.length);
    }
}
