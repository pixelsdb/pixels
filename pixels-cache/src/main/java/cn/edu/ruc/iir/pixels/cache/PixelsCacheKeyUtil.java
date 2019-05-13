package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheKeyUtil
{
    public static void getBytes(long blockId, short rowGroupId, short columnId, ByteBuffer keyBuffer)
    {
        keyBuffer.clear();
        keyBuffer.putLong(blockId);
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
    }
}
