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

 import java.util.Iterator;
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

     private static final Object lock = new Object();
     // Threshold for triggering eviction
     private static final int MAX_ENTRIES = 1024 * 1024;
     private static volatile BucketCache instance;
     // Using ConcurrentHashMap for thread-safe access
     private final Map<Integer, NodeProto.NodeInfo> bucketToNodeMap;
     private final NodeService nodeService;

     private BucketCache()
     {
         ConfigFactory config = ConfigFactory.Instance();
         int bucketNum = Integer.parseInt(config.getProperty("node.bucket.num"));
         // Initialize with initial capacity to reduce resizing
         this.bucketToNodeMap = new ConcurrentHashMap<>(Math.min(MAX_ENTRIES, bucketNum));
         this.nodeService = NodeService.Instance();
     }

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
      * Retrieves NodeInfo for a given bucketId.
      * If the cache exceeds MAX_ENTRIES, a random entry is evicted.
      */
     public NodeProto.NodeInfo getRetinaNodeInfoByBucketId(int bucketId)
     {
         NodeProto.NodeInfo nodeInfo = bucketToNodeMap.get(bucketId);
         if (nodeInfo != null)
         {
             return nodeInfo;
         }

         NodeProto.NodeInfo fetchedNodeInfo = fetchNodeInfoFromNodeService(bucketId);

         if (fetchedNodeInfo != null)
         {
             if (bucketToNodeMap.size() >= MAX_ENTRIES)
             {
                 evictRandomEntry();
             }
             bucketToNodeMap.put(bucketId, fetchedNodeInfo);
             return fetchedNodeInfo;
         }

         return null;
     }

     /**
      * Performs a simple random eviction.
      * In ConcurrentHashMap, the iterator returns elements in an arbitrary order
      * based on hash bin distribution, effectively acting as random selection.
      */
     private void evictRandomEntry()
     {
         // Use the iterator to pick the "first" available element in the current traversal
         Iterator<Integer> iterator = bucketToNodeMap.keySet().iterator();
         if (iterator.hasNext())
         {
             Integer randomKey = iterator.next();
             bucketToNodeMap.remove(randomKey);
         }
     }

     private NodeProto.NodeInfo fetchNodeInfoFromNodeService(int bucketId)
     {
         return nodeService.getRetinaByBucket(bucketId);
     }
 }