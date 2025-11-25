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

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import io.pixelsdb.pixels.daemon.NodeProto;
import io.pixelsdb.pixels.daemon.NodeServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class NodeService
{
    private static final Logger logger = LogManager.getLogger(NodeService.class);

    private static final NodeService defaultInstance;
    private static final Map<HostAddress, NodeService> otherInstances = new ConcurrentHashMap<>();

    static
    {
        String host = ConfigFactory.Instance().getProperty("node.server.host");
        int port = Integer.parseInt(ConfigFactory.Instance().getProperty("node.server.port"));

        defaultInstance = new NodeService(host, port);

        ShutdownHookManager.Instance().registerShutdownHook(NodeService.class, false, () ->
        {
            try
            {
                defaultInstance.shutdown();
                for (NodeService client : otherInstances.values())
                {
                    client.shutdown();
                }
                otherInstances.clear();
            } catch (InterruptedException e)
            {
                logger.error("Failed to shutdown NodeService", e);
            }
        });
    }

    private final ManagedChannel channel;
    private final NodeServiceGrpc.NodeServiceBlockingStub stub;
    private volatile boolean isShutDown;
    private NodeService(String host, int port)
    {
        assert host != null;
        assert port > 0 && port <= 65535;

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        this.stub = NodeServiceGrpc.newBlockingStub(channel);
        this.isShutDown = false;
    }

    public static NodeService Instance()
    {
        return defaultInstance;
    }

    public static synchronized NodeService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        NodeService client = otherInstances.get(address);
        if (client != null)
        {
            return client;
        }
        client = new NodeService(host, port);
        otherInstances.put(address, client);
        return client;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutDown)
        {
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutDown = true;
        }
    }

    public List<NodeProto.NodeInfo> getRetinaList()
    {
        NodeProto.GetRetinaListResponse resp;

        try
        {
            resp = stub.getRetinaList(Empty.getDefaultInstance());
        } catch (Exception e)
        {
            logger.error("Failed to call GetRetinaList", e);
            throw e;
        }

        if (resp.getErrorCode() != 0)
        {
            logger.error("GetRetinaList returned error code {}", resp.getErrorCode());
            return Collections.emptyList();
        }

        return resp.getNodesList();
    }

    public NodeProto.NodeInfo getRetinaByBucket(int bucketId)
    {
        NodeProto.GetRetinaByBucketRequest req =
                NodeProto.GetRetinaByBucketRequest.newBuilder()
                        .setBucket(bucketId)
                        .build();

        NodeProto.GetRetinaByBucketResponse resp;

        try
        {
            resp = stub.getRetinaByBucket(req);
        } catch (Exception e)
        {
            logger.error("Failed to call GetRetinaByBucket", e);
            throw e;
        }

        if (resp.getErrorCode() != 0)
        {
            logger.error("GetRetinaByBucket returned error={}", resp.getErrorCode());
            return null; // or throw exception
        }

        return resp.getNode();
    }
}
