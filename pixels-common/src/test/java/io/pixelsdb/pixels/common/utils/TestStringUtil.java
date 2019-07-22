package io.pixelsdb.pixels.common.utils;

import org.junit.Test;

/**
 * pixels
 *
 * @author guodong
 */
public class TestStringUtil
{
    @Test
    public void testReplaceAll()
    {
        String s = "abcdTrueidsdFalseddtruetureddssd";
        s = StringUtil.replaceAll(s, "True", "1");
        s = StringUtil.replaceAll(s, "False", "0");
        s = StringUtil.replaceAll(s, "true", "1");
        s = StringUtil.replaceAll(s, "false", "0");
        System.out.println(s);
    }
}
