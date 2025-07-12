/*
 * Copyright 2024 PixelsDB.
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

import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created at: 2024/1/20
 *
 * @author alph00
 */
class PixelsLocator {
    private static final Logger logger = LogManager.getLogger(PixelsLocator.class);
    // the number of nodes in the hash cycle, now it is equal to the number of lazy zone
    private int nodeNum = 1;
    // the number of replicas of each bucket
    private int replicaNum = 160;
    // specific hash function
    private PixelsHasher hasher;
    // the number of hash slots, copy from Redis
    private static final int NUM_HASH_SLOTS = 16384;
    // the suffix of virtual node
    private static final String VNODE_SUFFIX = "VNODE&&";
    // the prefix of node
    private static final String NODE_PREFIX = "BUCKET#";

    private TreeMap<Long,Long> hashCycle;

    private Map<String,Long> slotMap;

    public PixelsLocator(int nodeNum) {
        this.nodeNum = nodeNum;
        init();
    }


    public PixelsLocator(int nodeNum, int replicaNum) {
        this.nodeNum = nodeNum;
        this.replicaNum = replicaNum;
        init();
    }

    public PixelsLocator(int nodeNum, PixelsHasher hasher) {
        this.nodeNum = nodeNum;
        this.hasher = hasher;
        init();
    }

    public PixelsLocator(int nodeNum, int replicaNum, PixelsHasher hasher) {
        this.nodeNum = nodeNum;
        this.replicaNum = replicaNum;
        this.hasher = hasher;
        init();
    }

    private void init(){
        if(hasher == null){
            hasher = new PixelsMurmurHasher();
        }
        hashCycle = new TreeMap<>();
        slotMap = new HashMap<>();
        for (int j = 0; j < replicaNum; j++) {
            for (int i = 0; i < nodeNum; i++) {
                String vnode = VNODE_SUFFIX + j;
                String nodeName = NODE_PREFIX + i + "-" + vnode;
                long hash = hasher.getHashCode(nodeName) % NUM_HASH_SLOTS;
                int cnt = 0;
                while(hashCycle.containsKey(hash)){
                    hash = (hash + 1) % NUM_HASH_SLOTS;
                    cnt++;
                    if(cnt > NUM_HASH_SLOTS){
                        logger.error("init failed, bucketId: " + i + " replicaId: " + j);
                        break;
                    }
                }
                hashCycle.put(hash, (long) i);
                slotMap.put(nodeName, hash);
            }
        }
    }

    public long getLocation(PixelsCacheKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Cache key cannot be null");
        }
        // build cache key by rowGroupId, columnId and offset
        String keyStr = String.format("%d:%d:%d", 
            key.blockId,
            key.rowGroupId,
            key.columnId);
        long keyHash = hasher.getHashCode(keyStr) % NUM_HASH_SLOTS;
        Long replicaHash = hashCycle.ceilingKey(keyHash);
        return replicaHash == null ? hashCycle.get(hashCycle.firstKey()) : hashCycle.get(replicaHash);
    }

    public int getNodeNum(){
        return nodeNum;
    }

    public int getReplicaNum(){
        return replicaNum;
    }

    // used in cache scaling
    public boolean isDataInReplica(PixelsCacheKey key, long bucketId, int replicaId) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        String keyStr = String.format("%d:%d:%d", 
            key.blockId,
            key.rowGroupId,
            key.columnId);
        long hash = hasher.getHashCode(keyStr) % NUM_HASH_SLOTS;
        String vnode = VNODE_SUFFIX + replicaId;
        String nodeName = NODE_PREFIX + bucketId + "-" + vnode;
        Long bucketHashValue = slotMap.get(nodeName);
        Long prevHashValue = hashCycle.lowerKey(bucketHashValue);
        if (prevHashValue == null) {
            Long lastHashValue = hashCycle.lastKey();
            return hash > lastHashValue || hash <= bucketHashValue;
        } else {
            return hash > prevHashValue && hash <= bucketHashValue;
        }
    }

    // used in cache scaling
    public long findNextBucket(long bucketId, int replicaId) {
        String vnode = VNODE_SUFFIX + replicaId;
        String nodeName = NODE_PREFIX + bucketId + "-" + vnode;
        Long hash = slotMap.get(nodeName);
        Long nextBucketHash = hashCycle.higherKey(hash);
        if(nextBucketHash == null){
            nextBucketHash = hashCycle.firstKey();
        }
        Long nextBucketId = hashCycle.get(nextBucketHash);
        int cnt = 0;
        while(nextBucketId == bucketId){
            nextBucketHash = hashCycle.higherKey(nextBucketHash);
            if(nextBucketHash == null){
                nextBucketHash = hashCycle.firstKey();
            }
            nextBucketId = hashCycle.get(nextBucketHash);
            cnt++;
            if(cnt > NUM_HASH_SLOTS){
                logger.error("findNextBucket failed, bucketId: " + bucketId + " replicaId: " + replicaId);
                break;
            }
        }
        return nextBucketId;
    }

    public void addNode() {
        long bucketId = nodeNum;
        for (int j = 0; j < replicaNum; j++) {
            String vnode = VNODE_SUFFIX + j;
            String nodeName = NODE_PREFIX + bucketId + "-" + vnode;
            long hash = hasher.getHashCode(nodeName) % NUM_HASH_SLOTS;
            int cnt = 0;
            while(hashCycle.containsKey(hash)){
                hash = (hash + 1) % NUM_HASH_SLOTS;
                cnt++;
                if(cnt > NUM_HASH_SLOTS){
                    logger.error("addNode failed, bucketId: " + bucketId + " replicaId: " + j);
                    break;
                }
            }
            hashCycle.put(hash, bucketId);
            slotMap.put(nodeName, hash);
        }
        nodeNum++;
    }

    public void removeNode() {
        long bucketId = nodeNum - 1;
        for (int j = 0; j < replicaNum; j++) {
            String vnode = VNODE_SUFFIX + j;
            String nodeName = NODE_PREFIX + bucketId + "-" + vnode;
            Long actualHash = slotMap.remove(nodeName);
            hashCycle.remove(actualHash);
        }
        nodeNum--;
    }

    public void close() {
        hashCycle.clear();
    }
}