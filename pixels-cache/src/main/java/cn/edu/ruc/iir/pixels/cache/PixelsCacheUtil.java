package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheUtil
{
    private static final int writeMask = 0x80000000;
    private static final int readMask = 0x7FFFFFFF;
    private static final int readCountMax = 2 ^ 31 - 1;

    public static final int INDEX_SIZE_OFFSET = 128;
    public static final int INDEX_FIELD_OFFSET = 192;

    public static int setHeaderRW(int header, boolean write)
    {
        if (write) {
            header = writeMask | header;
        }
        else {
            header = readMask & header;
        }

        return header;
    }

    /**
     * Get read write flag.
     * @return false for read, true for write
     * */
    public static boolean getHeaderRW(int header)
    {
        return header < 0;
    }

    public static int getReadCount(int header)
    {
        return header & readMask;
    }

    public static int incrementReadCount(int header)
    {
        if ((header & readMask) < readCountMax) {
            return ++header;
        }

        return header;
    }

    public static int decrementReadCount(int header)
    {
        if ((header & readMask) > 0) {
            return --header;
        }

        return header;
    }
}
