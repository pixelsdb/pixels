 /*
  * Copyright 2025 PixelsDB.
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

 package io.pixelsdb.pixels.common.node;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.NodeProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

 public class TestBucketCache
{
    @Test
    public void testBucketCache()
    {
        BucketCache bucketCache = BucketCache.getInstance();
        NodeProto.NodeInfo nodeInfo0 = bucketCache.getRetinaNodeInfoByBucketId(0);
        Assertions.assertNotNull(nodeInfo0);
        NodeProto.NodeInfo nodeInfo1 = bucketCache.getRetinaNodeInfoByBucketId(1);
        Assertions.assertNotNull(nodeInfo1);
        NodeProto.NodeInfo nodeInfo0_cache = bucketCache.getRetinaNodeInfoByBucketId(0);
        Assertions.assertEquals(nodeInfo0, nodeInfo0_cache);
    }

    @Test
    public void testBucketStats()
    {
        int bucketNum = Integer.parseInt(
                ConfigFactory.Instance().getProperty("node.bucket.num")
        );

        BucketCache bucketCache = BucketCache.getInstance();
        Map<String, Integer> bucketStats = new HashMap<>();

        for (int i = 0; i < bucketNum; ++i)
        {
            NodeProto.NodeInfo node = bucketCache.getRetinaNodeInfoByBucketId(i);
            String addr = node.getAddress();

            bucketStats.compute(addr, (k, v) -> v == null ? 1 : v + 1);
        }

        for (Map.Entry<String, Integer> entry : bucketStats.entrySet()) {
            System.out.println(entry.getKey() + " => " + entry.getValue());
        }
    }

}
