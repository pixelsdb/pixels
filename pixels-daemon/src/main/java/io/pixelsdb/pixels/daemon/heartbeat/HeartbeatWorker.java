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
 * @author hank
 * @create 2024-06-17
 */
public class HeartbeatWorker implements Server
{
    private static final Logger logger = LogManager.getLogger(HeartbeatWorker.class);
    private static final AtomicInteger currentStatus = new AtomicInteger(NodeStatus.READY.StatusCode);
    private final HeartbeatConfig heartbeatConfig = new HeartbeatConfig();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private String hostName;
    private WorkerRegister workerRegister;
    private boolean initializeSuccess = false;
    private CountDownLatch runningLatch;

    public HeartbeatWorker()
    {
        this.hostName = System.getenv("HOSTNAME");
        logger.debug("HostName from system env: {}", hostName);
        if (hostName == null)
        {
            try
            {
                this.hostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: {}", hostName);
            } catch (UnknownHostException e)
            {
                logger.debug("Hostname is null. Exit");
                return;
            }
        }
        logger.debug("HostName: {}", hostName);
        initialize();
    }

    /**
     * Initialize heartbeat worker:
     * <p>
     * 1. check if the cache coordinator exists. If not, initialization is failed and return;
     * 2. register the Worker by updating the status of CacheWorker in etcd.
     * </p>
     * */
    private void initialize()
    {
        try
        {
            // 1. check the existence of the cache coordinator.
            KeyValue cacheCoordinatorKV = EtcdUtil.Instance().getKeyValueByPrefix(Constants.HEARTBEAT_COORDINATOR_LITERAL);
            if (cacheCoordinatorKV == null)
            {
                logger.error("No heartbeat coordinator found, exit...");
                return;
            }
            // 2. register the worker
            Lease leaseClient = EtcdUtil.Instance().getClient().getLeaseClient();
            long leaseId = leaseClient.grant(heartbeatConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            String key = Constants.HEARTBEAT_WORKER_LITERAL + hostName;
            EtcdUtil.Instance().putKeyValueWithLeaseId(key, String.valueOf(currentStatus.get()), leaseId);
            // start a scheduled thread to update node status periodically
            this.workerRegister = new WorkerRegister(key, leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(workerRegister,
                    0, heartbeatConfig.getNodeHeartbeatPeriod(), TimeUnit.SECONDS);
            initializeSuccess = true;
            currentStatus.set(NodeStatus.READY.StatusCode);
            logger.info("Heartbeat worker on {} is initialized", hostName);
        } catch (Exception e)
        {
            logger.error("failed to initialize heartbeat worker", e);
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
        logger.info("Shutting down heartbeat worker...");
        scheduledExecutor.shutdownNow();
        if (workerRegister != null)
        {
            workerRegister.stop();
        }
        EtcdUtil.Instance().deleteByPrefix(Constants.HEARTBEAT_WORKER_LITERAL);
        if (runningLatch != null)
        {
            runningLatch.countDown();
        }
        EtcdUtil.Instance().getClient().close();
        logger.info("Heartbeat worker on '{}' is shutdown.", hostName);
    }

    @Override
    public void run()
    {
        logger.info("Starting heartbeat worker");
        if (!initializeSuccess)
        {
            logger.error("Heartbeat worker initialization failed, exit now...");
            return;
        }
        runningLatch = new CountDownLatch(1);
        try
        {
            // Wait for this heartbeat worker to be shutdown.
            logger.info("Heartbeat worker is running");
            runningLatch.await();
        } catch (InterruptedException e)
        {
            logger.error("Heartbeat worker interrupted when waiting on the running latch", e);
        }
    }

    /**
     * Register to update worker node status and keep its lease alive.
     * It should be run periodically by a scheduled executor.
     * */
    private static class WorkerRegister implements Runnable
    {
        private final String workerKey;
        private final Lease leaseClient;
        private final long leaseId;

        WorkerRegister(String workerKey, Lease leaseClient, long leaseId)
        {
            this.workerKey = workerKey;
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run()
        {
            leaseClient.keepAliveOnce(leaseId);
            EtcdUtil.Instance().putKeyValueWithLeaseId(workerKey, String.valueOf(currentStatus.get()), leaseId);
        }

        public void stop()
        {
            leaseClient.revoke(leaseId);
            leaseClient.close();
        }
    }
}
