package cn.edu.ruc.iir.pixels.common.utils;

/**
 * pixels
 *
 * @author guodong
 */
public class StringUtil
{
    private StringUtil()
    {}

    public static String replaceAll(String text, String searchString, String replacement)
    {
        if (text.isEmpty() || searchString.isEmpty() || replacement.isEmpty())
        {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1)
        {
            return text;
        }
        int repLen = searchString.length();
        StringBuilder buf = new StringBuilder();
        while (end != -1)
        {
            buf.append(text, start, end).append(replacement);
            start = end + repLen;
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }
}
