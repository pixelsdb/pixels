/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.daemon.transaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.etcd.jetcd.KeyValue;
import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.heartbeat.NodeStatus;

/**
 * @author hank
 * @create 2022-02-20
 */
public class TransServer implements Server
{
    private static final Logger log = LogManager.getLogger(TransServer.class);

    /**
     * Default time to wait for all expected Retina nodes to reach READY before giving up
     * and aborting the trans server boot. Overridable by {@code trans.server.retina.readiness.timeout.ms}.
     */
    private static final long DEFAULT_RETINA_READINESS_TIMEOUT_MS = 10 * 60 * 1000L;
    private static final long RETINA_READINESS_POLL_INTERVAL_MS = 1_000L;

    private boolean running = false;
    private final io.grpc.Server rpcServer;

    public TransServer(int port)
    {
        assert (port > 0 && port <= 65535);
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new TransServiceImpl()).build();
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
        try
        {
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when shutdown transaction server.", e);
        }
    }

    @Override
    public void run()
    {
        try
        {
            awaitRetinaReady();
            this.rpcServer.start();
            this.running = true;
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running.", e);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when running.", e);
        } finally
        {
            this.shutdown();
        }
    }

    /**
     * Boot-time gate. When {@code retina.enable=true}, blocks until every node listed in
     * {@code $PIXELS_HOME/etc/retina} reports {@code NodeStatus.READY} via heartbeat. When
     * {@code retina.enable=false}, returns immediately. On timeout, throws so that
     * {@link #run()} aborts and the supervisor can restart the process.
     *
     * <p>This is intentionally a one-shot check executed before the gRPC server starts.
     * Once the trans server is serving, it does not re-check Retina lifecycle state.
     */
    private void awaitRetinaReady()
    {
        ConfigFactory config = ConfigFactory.Instance();
        if (!Boolean.parseBoolean(config.getProperty("retina.enable")))
        {
            return;
        }

        // Load expected Retina nodes from $PIXELS_HOME/etc/retina.
        Path retinaFile = Paths.get(config.getProperty("pixels.home"), "etc", "retina");
        if (!Files.isRegularFile(retinaFile))
        {
            throw new IllegalStateException(retinaFile + " is missing");
        }
        Set<String> expected = new LinkedHashSet<>();
        try
        {
            for (String raw : Files.readAllLines(retinaFile, StandardCharsets.UTF_8))
            {
                String line = raw.trim();
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue;
                }
                String host = line.split("\\s+", 2)[0];
                expected.add(host);
            }
        } catch (IOException e)
        {
            throw new IllegalStateException("Failed to load expected Retina nodes from "
                    + "$PIXELS_HOME/etc/retina", e);
        }
        if (expected.isEmpty())
        {
            throw new IllegalStateException(
                    "retina.enable=true but $PIXELS_HOME/etc/retina has no nodes");
        }

        long deadline = System.currentTimeMillis() + DEFAULT_RETINA_READINESS_TIMEOUT_MS;
        EtcdUtil etcd = EtcdUtil.Instance();
        String prefix = Constants.HEARTBEAT_RETINA_LITERAL;
        int prefixLen = prefix.length();
        log.info("Waiting for {} Retina node(s) to report READY (timeout {} ms)",
                expected.size(), DEFAULT_RETINA_READINESS_TIMEOUT_MS);
        while (true)
        {
            String reason = null;
            // Poll all Retina heartbeat keys once and check whether every expected node is READY.
            Map<String, KeyValue> observed;
            try
            {
                List<KeyValue> all = etcd.getKeyValuesByPrefix(prefix);
                observed = new HashMap<>(all.size() * 2);
                for (KeyValue kv : all)
                {
                    String key = kv.getKey().toString(StandardCharsets.UTF_8);
                    if (key.length() > prefixLen)
                    {
                        observed.put(key.substring(prefixLen), kv);
                    }
                }
            } catch (RuntimeException e)
            {
                observed = null;
                reason = "etcd heartbeat read failed: " + e.getMessage();
            }
            if (reason == null)
            {
                for (String host : expected)
                {
                    KeyValue kv = observed.get(host);
                    if (kv == null)
                    {
                        reason = "Retina node " + host + " has no heartbeat status";
                        break;
                    }
                    if (kv.getLease() <= 0)
                    {
                        reason = "Retina node " + host + " has heartbeat status without lease";
                        break;
                    }
                    String status = kv.getValue().toString(StandardCharsets.UTF_8).trim();
                    if (!String.valueOf(NodeStatus.READY.StatusCode).equals(status))
                    {
                        reason = "Retina node " + host + " heartbeat status is " + status;
                        break;
                    }
                }
            }
            if (reason == null)
            {
                log.info("All Retina nodes are READY, starting trans server");
                return;
            }
            if (System.currentTimeMillis() >= deadline)
            {
                throw new IllegalStateException(
                        "Timed out waiting for Retina readiness after "
                                + DEFAULT_RETINA_READINESS_TIMEOUT_MS
                                + " ms; last reason: " + reason);
            }
            try
            {
                Thread.sleep(RETINA_READINESS_POLL_INTERVAL_MS);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting for Retina readiness", e);
            }
        }
    }
}
