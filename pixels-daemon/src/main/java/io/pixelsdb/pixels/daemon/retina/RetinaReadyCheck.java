/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.daemon.retina;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.options.GetOption;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.StartupCheck;
import io.pixelsdb.pixels.daemon.heartbeat.NodeStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

/**
 * Waits until every configured Retina node has a leased READY heartbeat.
 *
 * @author PixelsDB
 */
public class RetinaReadyCheck implements StartupCheck
{
    private static final Logger log = LogManager.getLogger(RetinaReadyCheck.class);
    private static final long ETCD_READ_TIMEOUT_MS = 1_000L;
    private static final long RETRY_INTERVAL_MS = 1_000L;

    @Override
    public String getDescription()
    {
        return "all configured Retina nodes to report READY";
    }

    @Override
    public void awaitReady(long deadlineNanos) throws InterruptedException
    {
        ConfigFactory config = ConfigFactory.Instance();
        if (!Boolean.parseBoolean(config.getProperty("retina.enable")))
        {
            return;
        }

        Set<String> expected = loadExpectedNodes(config);
        if (expected.isEmpty())
        {
            throw new IllegalStateException(
                    "retina.enable=true but $PIXELS_HOME/etc/retina has no nodes");
        }

        String lastReason = null;
        log.info("Waiting for {} Retina node(s) to report READY", expected.size());

        while (true)
        {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0)
            {
                throw new IllegalStateException("Timed out waiting for Retina readiness");
            }
            String reason = getReadinessReason(expected, remainingNanos);
            if (reason == null)
            {
                log.info("All Retina nodes are READY");
                return;
            }
            if (!reason.equals(lastReason))
            {
                log.info("Retina readiness is pending: {}", reason);
                lastReason = reason;
            }
            remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0)
            {
                throw new IllegalStateException(
                        "Timed out waiting for Retina readiness; last reason: " + reason);
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(
                    TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MS), remainingNanos));
        }
    }

    private Set<String> loadExpectedNodes(ConfigFactory config)
    {
        String pixelsHome = config.getProperty("pixels.home");
        if (pixelsHome == null || pixelsHome.isEmpty())
        {
            throw new IllegalStateException("pixels.home is not configured");
        }
        Path retinaFile = Paths.get(pixelsHome, "etc", "retina");
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
                expected.add(line.split("\\s+", 2)[0]);
            }
        }
        catch (Exception e)
        {
            throw new IllegalStateException(
                    "Failed to load expected Retina nodes from " + retinaFile, e);
        }
        return expected;
    }

    private String getReadinessReason(Set<String> expected, long timeoutNanos)
            throws InterruptedException
    {
        String prefix = Constants.HEARTBEAT_RETINA_LITERAL;
        Map<String, KeyValue> observed;
        try
        {
            ByteSequence prefixBytes = ByteSequence.from(prefix, StandardCharsets.UTF_8);
            GetOption getOption = GetOption.builder().isPrefix(true).build();
            List<KeyValue> all = EtcdUtil.Instance().getClient().getKVClient()
                    .get(prefixBytes, getOption)
                    .get(Math.min(
                                    TimeUnit.MILLISECONDS.toNanos(ETCD_READ_TIMEOUT_MS),
                                    timeoutNanos),
                            TimeUnit.NANOSECONDS)
                    .getKvs();
            observed = new HashMap<>(all.size() * 2);
            for (KeyValue kv : all)
            {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);
                if (key.length() > prefix.length())
                {
                    observed.put(key.substring(prefix.length()), kv);
                }
            }
        }
        catch (InterruptedException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            return "Etcd heartbeat read failed: " + e.getMessage();
        }

        for (String host : expected)
        {
            KeyValue kv = observed.get(host);
            if (kv == null)
            {
                return "Retina node " + host + " has no heartbeat status";
            }
            if (kv.getLease() <= 0)
            {
                return "Retina node " + host + " has heartbeat status without lease";
            }
            String status = kv.getValue().toString(StandardCharsets.UTF_8).trim();
            if (!String.valueOf(NodeStatus.READY.StatusCode).equals(status))
            {
                return "Retina node " + host + " heartbeat status is " + status;
            }
        }
        return null;
    }
}
