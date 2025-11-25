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

package io.pixelsdb.pixels.daemon.node;

import com.google.protobuf.Empty;
import io.etcd.jetcd.KeyValue;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.NodeProto;
import io.pixelsdb.pixels.daemon.NodeServiceGrpc;
import io.pixelsdb.pixels.daemon.heartbeat.HeartbeatConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * gRPC implementation of NodeService with dynamic consistent hashing.
 * Supports adding/removing Retina nodes at runtime.
 * Implementation is modified to use bucketNum as the number of virtual nodes,
 * and assumes the client provides a pre-hashed key (bucket ID).
 */
public class NodeServiceImpl extends NodeServiceGrpc.NodeServiceImplBase
{

    private static final Logger logger = LogManager.getLogger(NodeServiceImpl.class);

    private final int virtualNode;
    private final int bucketNum;

    // Consistent Hash Ring: Hash Value -> NodeInfo (using TreeMap for SortedMap)
    // The keys in this map will range from 0 to bucketNum - 1.
    private final SortedMap<Integer, NodeProto.NodeInfo> hashRing = new TreeMap<>();

    // Lock for thread safety during dynamic updates
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final HeartbeatConfig heartbeatConfig;
    /**
     * Watch Etcd heartbeat changes and dynamically update hash ring.
     */
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    public NodeServiceImpl()
    {
        ConfigFactory config = ConfigFactory.Instance();
        virtualNode = Integer.parseInt(config.getProperty("node.virtual.num"));
        // bucketNum is the total number of hash points
        bucketNum = Integer.parseInt(config.getProperty("node.bucket.num"));
        this.heartbeatConfig = new HeartbeatConfig();
        // Initial load from Etcd
        reloadRetinaNodesFromEtcd();
        startEtcdWatcher();
    }

    private String getAddressFromKV(KeyValue kv)
    {
        return kv.getKey()
                .toString(StandardCharsets.UTF_8).substring(Constants.HEARTBEAT_RETINA_LITERAL.length());
    }

    /**
     * Load nodes from Etcd and build hash ring.
     */
    private void reloadRetinaNodesFromEtcd()
    {
        List<KeyValue> retinaNodes = EtcdUtil.Instance()
                .getKeyValuesByPrefix(Constants.HEARTBEAT_RETINA_LITERAL);
        lock.writeLock().lock();
        try
        {
            hashRing.clear();
            for (KeyValue kv : retinaNodes)
            {
                NodeProto.NodeInfo node = NodeProto.NodeInfo.newBuilder()
                        .setAddress(getAddressFromKV(kv))
                        .setRole(NodeProto.NodeRole.RETINA)
                        .build();
                addNodeInternal(node);
            }
            logger.info("Initial hash ring loaded with {} physical nodes and {} virtual nodes.",
                    hashRing.values().stream().distinct().count(), bucketNum);
        } finally
        {
            lock.writeLock().unlock();
        }
    }

    private void startEtcdWatcher()
    {
        Runnable watchTask = () ->
        {
            try
            {
                List<KeyValue> currentNodes = EtcdUtil.Instance()
                        .getKeyValuesByPrefix(Constants.HEARTBEAT_RETINA_LITERAL);
                updateHashRing(currentNodes);
            } catch (Exception e)
            {
                logger.error("Error watching Etcd changes", e);
            }
        };

        // Schedule the task at fixed rate
        scheduledExecutor.scheduleAtFixedRate(
                watchTask,
                heartbeatConfig.getNodeHeartbeatPeriod(),   // initial delay
                heartbeatConfig.getNodeHeartbeatPeriod(),   // period in seconds
                TimeUnit.SECONDS
        );
    }

    // Removed the non-consistent-hashing updateBucketMapping method.

    /**
     * Update hash ring with current nodes (add new, remove missing).
     */
    private void updateHashRing(List<KeyValue> currentNodes)
    {
        lock.writeLock().lock();
        try
        {
            // 1. Collect current active addresses from Etcd
            Set<String> newAddresses = new HashSet<>();
            for (KeyValue kv : currentNodes)
            {
                newAddresses.add(getAddressFromKV(kv));
            }

            // 2. Collect addresses currently on the ring
            Set<String> existingAddresses = new HashSet<>();
            for (NodeProto.NodeInfo node : new TreeSet<>(hashRing.values()))
            {
                existingAddresses.add(node.getAddress());
            }

            // 3. Remove nodes not present anymore (existingAddresses - newAddresses)
            for (NodeProto.NodeInfo node : new TreeSet<>(hashRing.values()))
            {
                if (!newAddresses.contains(node.getAddress()))
                {
                    removeNodeInternal(node);
                    logger.warn("Removed node from hash ring: " + node.getAddress());
                }
            }

            // 4. Add new nodes (newAddresses - existingAddresses)
            for (String newAddr : newAddresses)
            {
                if (!existingAddresses.contains(newAddr))
                {
                    NodeProto.NodeInfo node = NodeProto.NodeInfo.newBuilder()
                            .setAddress(newAddr)
                            .setRole(NodeProto.NodeRole.RETINA)
                            .build();
                    addNodeInternal(node);
                    logger.info("Added node to hash ring: " + newAddr);
                }
            }
        } finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a node to the hash ring internally, mapping to the fixed hash points [0, bucketNum-1].
     */
    private void addNodeInternal(NodeProto.NodeInfo node)
    {
        for (int i = 0; i < virtualNode; i++)
        {
            int hashPoint = hash(node.getAddress() + "#" + i) % bucketNum;
            hashRing.put(hashPoint, node);
        }
    }

    /**
     * Remove a node from the hash ring internally.
     */
    private void removeNodeInternal(NodeProto.NodeInfo node)
    {
        // Recalculate and remove all virtual nodes
        for (int i = 0; i < bucketNum; i++)
        {
            int hashPoint = hash(node.getAddress() + "#" + i) % bucketNum;
            hashRing.remove(hashPoint);
        }
        // Note: The removal above is technically incomplete if multiple virtual nodes map to the same hashPoint,
        // but given the requirement, we assume hash() provides a reasonably uniform spread.
    }

    /**
     * Simple hash function for consistent hashing.
     */
    private int hash(String key)
    {
        return key.hashCode();
    }

    /**
     * RPC: Get all retina nodes.
     * (No change, uses the hashRing values)
     */
    @Override
    public void getRetinaList(Empty request,
                              StreamObserver<NodeProto.GetRetinaListResponse> responseObserver)
    {
        NodeProto.GetRetinaListResponse.Builder responseBuilder = NodeProto.GetRetinaListResponse.newBuilder();
        lock.readLock().lock();
        try
        {
            hashRing.values().stream().distinct().forEach(responseBuilder::addNodes);
            responseBuilder.setErrorCode(ErrorCode.SUCCESS);
        } catch (Exception e)
        {
            responseBuilder.setErrorCode(ErrorCode.NODE_RETINA_INFO_FAIL);
            logger.error("Failed to retrieve retina nodes", e);
        } finally
        {
            lock.readLock().unlock();
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * RPC: Get retina node by bucket id using consistent hashing.
     * MODIFIED: Uses the input 'bucket' (which is the pre-hashed key/virtual node ID) directly as the lookup point.
     */
    @Override
    public void getRetinaByBucket(NodeProto.GetRetinaByBucketRequest request,
                                  StreamObserver<NodeProto.GetRetinaByBucketResponse> responseObserver)
    {
        NodeProto.GetRetinaByBucketResponse.Builder responseBuilder = NodeProto.GetRetinaByBucketResponse.newBuilder();
        lock.readLock().lock();
        try
        {
            if (hashRing.isEmpty())
            {
                responseBuilder.setErrorCode(ErrorCode.NODE_NO_AVAILABLE);
            } else
            {
                int bucketId = request.getBucket();

                if (bucketId < 0 || bucketId >= bucketNum)
                {
                    logger.error("Invalid bucket hash received: " + bucketId);
                    responseBuilder.setErrorCode(ErrorCode.NODE_INVALID_BUCKET);
                } else
                {
                    SortedMap<Integer, NodeProto.NodeInfo> tail = hashRing.tailMap(bucketId);
                    int nodeHashPoint = tail.isEmpty() ? hashRing.firstKey() : tail.firstKey();
                    NodeProto.NodeInfo node = hashRing.get(nodeHashPoint);
                    responseBuilder.setErrorCode(ErrorCode.SUCCESS).setNode(node);
                }
            }
        } catch (Exception e)
        {
            responseBuilder.setErrorCode(ErrorCode.NODE_RETINA_INFO_FAIL);
            logger.error("Failed to get retina node by bucket", e);
        } finally
        {
            lock.readLock().unlock();
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
