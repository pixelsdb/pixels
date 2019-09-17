/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author guodong
 */
public class TestSerialization
{
//    @Test
//    public void testSer()
//    {
//        String blockId = "hdfs://dbiir27:9000/pixels/pixels/test_1887/v_1_compact/20190102094644_0.compact_copy_20190103025917_0.pxl";
//        short rowGroupId = 0;
//        short columnId = 0;
//        long len = 0;
//        long serStart = System.nanoTime();
//        for (int i = 0; i < 10000; i++)
//        {
//            PixelsCacheKey cacheKey = new PixelsCacheKey(blockId, rowGroupId, columnId);
//            byte[] result = cacheKey.getBytes();
//            len += result.length;
//        }
//        long serEnd = System.nanoTime();
//        System.out.println("Cost: " + (serEnd - serStart) + ", " + len);
//    }

    private void optimizedSer(String blockId, short rowGroupId, short columnId)
    {

    }

    @Test
    public void testOrder()
    {
        ByteOrder order = ByteOrder.nativeOrder();
        System.out.println("native order: " + order.toString());
        ByteBuffer buffer = ByteBuffer.allocate(80);
        buffer.order(ByteOrder.BIG_ENDIAN);

        for (int i = 0; i < 4; i++)
        {
            buffer.putInt(i);
        }

        buffer.flip();
        byte[] res = new byte[16];
        buffer.get(res);
        System.out.println(res.length);
    }
}
