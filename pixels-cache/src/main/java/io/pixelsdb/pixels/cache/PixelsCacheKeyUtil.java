package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheKeyUtil
{
    public static void getBytes(ByteBuffer keyBuffer, long blockId, short rowGroupId, short columnId)
    {
        keyBuffer.clear();
        keyBuffer.putLong(blockId);
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
    }
}
