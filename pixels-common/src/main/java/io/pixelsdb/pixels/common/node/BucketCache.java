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

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.NodeProto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Component responsible for managing the cache of bucketId to RetinaNodeInfo mappings.
 * It uses the Singleton pattern and lazy initialization to ensure a single instance
 * and deferred creation.
 * * NOTE: The cache invalidation logic (when the hash ring changes) needs to be integrated
 * with NodeServiceImpl, which is simplified in this example.
 */
public class BucketCache
{

    // Lock object for thread-safe singleton initialization
    private static final Object lock = new Object();
    // Lazy-loaded Singleton instance
    private static volatile BucketCache instance;
    // Thread-safe map cache: Key: bucketId (0 to bucketNum - 1), Value: RetinaNodeInfo
    private final Map<Integer, NodeProto.NodeInfo> bucketToNodeMap;

    // NodeService client stub (would be used for actual RPC calls in a real application)
    // private final NodeServiceGrpc.NodeServiceBlockingStub nodeServiceStub;
    // The total number of discrete hash points (M) loaded from configuration
    private final int bucketNum;
    private final NodeService nodeService;

    /**
     * Private constructor to enforce the Singleton pattern.
     */
    private BucketCache()
    {
        // In a real application, bucketNum should be fetched from ConfigFactory
        ConfigFactory config = ConfigFactory.Instance();
        this.bucketNum = Integer.parseInt(config.getProperty("node.bucket.num"));

        // Initialize the cache structure
        this.bucketToNodeMap = new ConcurrentHashMap<>(bucketNum);
        this.nodeService = NodeService.Instance();
    }

    /**
     * Retrieves the singleton instance of BucketToNodeCache. Uses double-checked
     * locking for thread-safe lazy initialization.
     * * @return The BucketToNodeCache instance
     */
    public static BucketCache getInstance()
    {
        if (instance == null)
        {
            synchronized (lock)
            {
                if (instance == null)
                {
                    instance = new BucketCache();
                }
            }
        }
        return instance;
    }

    /**
     * Core lookup method: Retrieves the corresponding RetinaNodeInfo for a given bucketId.
     * Uses a cache-aside strategy (lazy loading) to populate the cache upon miss.
     * * @param bucketId The hash bucket ID of the data (range 0 to bucketNum - 1)
     *
     * @return The corresponding RetinaNodeInfo, or null if lookup fails
     */
    public NodeProto.NodeInfo getRetinaNodeInfoByBucketId(int bucketId)
    {
        // 1. Try to get from cache
        NodeProto.NodeInfo nodeInfo = bucketToNodeMap.get(bucketId);
        if (nodeInfo != null)
        {
            return nodeInfo;
        }

        // 2. Cache miss: Fetch from the authoritative source (NodeService RPC)
        NodeProto.NodeInfo fetchedNodeInfo = fetchNodeInfoFromNodeService(bucketId);

        if (fetchedNodeInfo != null)
        {
            // 3. Put into cache
            bucketToNodeMap.put(bucketId, fetchedNodeInfo);
            return fetchedNodeInfo;
        }

        return null;
    }

    private NodeProto.NodeInfo fetchNodeInfoFromNodeService(int bucketId)
    {
        return nodeService.getRetinaByBucket(bucketId);
    }
}
