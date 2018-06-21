package cn.edu.ruc.iir.pixels.presto;

import org.junit.Test;

public class TestString
{
    @Test
    public void test ()
    {
        StringBuilder sb = new StringBuilder("<");
        for (int i = 0; i < 5; i++)
        {
            sb.append(":,");
        }
        sb.replace(sb.length() - 1, sb.length(), ">");
        System.out.println(sb.toString());
    }
}
