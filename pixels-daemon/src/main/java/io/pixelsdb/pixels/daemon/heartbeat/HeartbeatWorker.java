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

import io.etcd.jetcd.Lease;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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


    @Override
    public boolean isRunning()
    {
        return false;
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public void run()
    {

    }

    /**
     * Register to update worker node status and keep its lease alive.
     * It should be run periodically by a scheduled executor.
     * */
    private static class WorkerRegister implements Runnable
    {
        private final String coordinatorKey;
        private final Lease leaseClient;
        private final long leaseId;

        WorkerRegister(String coordinatorKey, Lease leaseClient, long leaseId)
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
