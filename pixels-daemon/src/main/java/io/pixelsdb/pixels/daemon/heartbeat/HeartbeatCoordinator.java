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
package io.pixelsdb.pixels.daemon.heartbeat;

import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the coordinator of the heartbeat mechanism.
 * The heartbeat was only used by pixels-cache, so it was implemented inside the cache coordinator and worker.
 * However, it is now needed by more features such as the distributed range index. Hence, we move it to separate classes.
 * @author hank
 * @create 2024-06-17 partially from {@link io.pixelsdb.pixels.daemon.cache.CacheCoordinator}
 */
public class HeartbeatCoordinator implements Server
{
    private static final Logger logger = LogManager.getLogger(HeartbeatCoordinator.class);
    private static final AtomicInteger currentStatus = new AtomicInteger(NodeStatus.INIT.StatusCode);
    private final HeartbeatConfig heartbeatConfig = new HeartbeatConfig();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private String hostName;
    private CoordinatorRegister coordinatorRegister = null;
    private boolean initializeSuccess = false;
    private CountDownLatch runningLatch;

    public HeartbeatCoordinator()
    {
        this.hostName = System.getenv("HOSTNAME");
        logger.debug("HostName from system env: " + hostName);
        if (hostName == null)
        {
            try
            {
                this.hostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: " + hostName);
            }
            catch (UnknownHostException e)
            {
                logger.debug("Hostname is null. Exit");
                return;
            }
        }
        initialize();
    }

    /**
     * Initialize Coordinator:
     *
     * 1. check if there is an existing coordinator, if yes, return.
     * 2. register the coordinator.
     */
    private void initialize()
    {
        // 1. if another cache coordinator exists, stop initialization
        KeyValue coordinatorKV = EtcdUtil.Instance().getKeyValueByPrefix(Constants.HEARTBEAT_COORDINATOR_LITERAL);
        if (coordinatorKV != null && coordinatorKV.getLease() > 0)
        {
            try
            {
                // When the lease of the heartbeat coordinator key exists, wait for the lease ttl to expire.
                Thread.sleep(heartbeatConfig.getNodeLeaseTTL() * 1000L);
            } catch (InterruptedException e)
            {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
            coordinatorKV = EtcdUtil.Instance().getKeyValueByPrefix(Constants.HEARTBEAT_COORDINATOR_LITERAL);
            if (coordinatorKV != null && coordinatorKV.getLease() > 0)
            {
                // another coordinator exists
                logger.error("Another heartbeat coordinator exists, exit...");
                return;
            }
        }

        try
        {
            // 2. register the coordinator
            Lease leaseClient = EtcdUtil.Instance().getClient().getLeaseClient();
            long leaseId = leaseClient.grant(heartbeatConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            String key = Constants.HEARTBEAT_COORDINATOR_LITERAL + hostName;
            EtcdUtil.Instance().putKeyValueWithLeaseId(Constants.HEARTBEAT_COORDINATOR_LITERAL + hostName,
                    String.valueOf(currentStatus.get()), leaseId);
            this.coordinatorRegister = new CoordinatorRegister(key, leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(coordinatorRegister,
                    0, heartbeatConfig.getNodeHeartbeatPeriod(), TimeUnit.SECONDS);
            initializeSuccess = true;
            currentStatus.set(NodeStatus.READY.StatusCode);
            logger.info("Heartbeat coordinator on " + hostName + " is initialized");
        } catch (Exception e)
        {
            logger.error("failed to initialize heartbeat coordinator", e);
        }
    }

    @Override
    public boolean isRunning()
    {
        int status = currentStatus.get();
        return status == NodeStatus.INIT.StatusCode || status == NodeStatus.READY.StatusCode;
    }

    @Override
    public void shutdown()
    {
        currentStatus.set(NodeStatus.EXIT.StatusCode);
        logger.debug("Shutting down heartbeat coordinator...");
        scheduledExecutor.shutdownNow();
        if (coordinatorRegister != null)
        {
            coordinatorRegister.stop();
        }
        EtcdUtil.Instance().deleteByPrefix(Constants.HEARTBEAT_COORDINATOR_LITERAL);
        if (runningLatch != null)
        {
            runningLatch.countDown();
        }
        EtcdUtil.Instance().getClient().close();
        logger.info("Heartbeat coordinator on '" + hostName + "' is shutdown");
    }

    @Override
    public void run()
    {
        logger.info("Starting heartbeat coordinator");
        if (!initializeSuccess)
        {
            logger.error("Heartbeat coordinator initialization failed, stop now...");
            return;
        }
        runningLatch = new CountDownLatch(1);

        try
        {
            // Wait for this coordinator to be shutdown.
            runningLatch.await();
        } catch (InterruptedException e)
        {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static class CoordinatorRegister implements Runnable
    {
        private final String coordinatorKey;
        private final Lease leaseClient;
        private final long leaseId;

        CoordinatorRegister(String coordinatorKey, Lease leaseClient, long leaseId)
        {
            this.coordinatorKey = coordinatorKey;
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run()
        {
            leaseClient.keepAliveOnce(leaseId);
            EtcdUtil.Instance().putKeyValueWithLeaseId(coordinatorKey,
                    String.valueOf(currentStatus.get()), leaseId);
        }

        public void stop()
        {
            leaseClient.revoke(leaseId);
            leaseClient.close();
        }
    }
}
