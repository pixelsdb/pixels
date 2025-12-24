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

 package io.pixelsdb.pixels.common.utils;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.node.BucketCache;
import io.pixelsdb.pixels.common.retina.RetinaService;

 public class RetinaUtils
{
    private static volatile RetinaUtils instance;
    private final int bucketNum;
    private final int defaultRetinaPort;

    private RetinaUtils()
    {
        ConfigFactory config = ConfigFactory.Instance();
        this.bucketNum = Integer.parseInt(config.getProperty("node.bucket.num"));
        this.defaultRetinaPort = Integer.parseInt(config.getProperty("retina.server.port"));
    }

    private static RetinaUtils getInstance()
    {
        if (instance == null)
        {
            synchronized (RetinaUtils.class)
            {
                if (instance == null)
                {
                    instance = new RetinaUtils();
                }
            }
        }
        return instance;
    }

    /**
     * Calculates the bucketId on the hash ring for the input data.
     * Uses MurmurHash3_32 and the modulo operation to constrain the result to the
     * discrete range [0, bucketNum - 1].
     * * @param byteString The input data to be hashed
     *
     * @return The calculated bucketId
     */
    public static int getBucketIdFromByteBuffer(ByteString byteString)
    {
        // Get the singleton instance to access bucketNum
        RetinaUtils retinaUtils = getInstance();

        // 1. Calculate the hash using MurmurHash3_32
        int hash = Hashing.murmur3_32_fixed()
                .hashBytes(byteString.toByteArray())
                .asInt();

        // 2. Take the absolute value (MurmurHash3_32 can return negative integers)
        int absHash = Math.abs(hash);

        // 3. Apply modulo operation to compress the hash value to the range [0, bucketNum - 1]
        return absHash % retinaUtils.bucketNum;
    }

    public static String getRetinaHostNameFromBucketId(int bucketId)
    {
        return BucketCache.getInstance().getRetinaNodeInfoByBucketId(bucketId).getAddress();
    }

    public static RetinaService getRetinaServiceFromBucketId(int bucketId)
    {
        String retinaHost = getRetinaHostNameFromBucketId(bucketId);
        return RetinaService.CreateInstance(retinaHost, getInstance().defaultRetinaPort);
    }

    public static RetinaService getRetinaServiceFromPath(String path)
    {
        String retinaHost = extractRetinaHostNameFromPath(path);
        if(retinaHost == null || retinaHost.equals(Constants.LOAD_DEFAULT_RETINA_PREFIX))
        {
            return RetinaService.Instance();
        }
        return RetinaService.CreateInstance(retinaHost, getInstance().defaultRetinaPort);
    }

    private static String extractRetinaHostNameFromPath(String path)
    {
        if (path == null || path.isEmpty()) {
            return null;
        }
        int lastSlashIndex = path.lastIndexOf('/');
        String baseName = (lastSlashIndex == -1) ? path : path.substring(lastSlashIndex + 1);
        int firstUnderscoreIndex = baseName.indexOf('_');
        if (firstUnderscoreIndex > 0) {
            // The substring from the start of baseName up to (but not including) the first underscore is the hostname.
            return baseName.substring(0, firstUnderscoreIndex);
        }
        return null;
    }
}
