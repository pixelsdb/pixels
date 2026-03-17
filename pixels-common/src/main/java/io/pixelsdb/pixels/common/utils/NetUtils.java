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
package io.pixelsdb.pixels.common.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Common network utilities.
 *
 * @author hank
 * @create 2026-03-16
 */
public final class NetUtils
{
    private static final Logger logger = LogManager.getLogger(NetUtils.class);

    private NetUtils() {}

    /**
     * Returns the hostname of the local machine.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>The {@code HOSTNAME} environment variable — reliable in container / k8s environments
     *       where {@link InetAddress#getLocalHost()} may return an unpredictable pod address.</li>
     *   <li>{@link InetAddress#getLocalHost()#getHostName()} — standard JVM resolution.</li>
     *   <li>{@code "localhost"} — last-resort fallback; a warning is logged.</li>
     * </ol>
     *
     * @return the local hostname, never {@code null}
     */
    public static String getLocalHostName()
    {
        String hostName = System.getenv("HOSTNAME");
        if (hostName != null && !hostName.isEmpty())
        {
            return hostName;
        }
        try
        {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e)
        {
            logger.warn("Failed to resolve local hostname via InetAddress, falling back to 'localhost'", e);
            return "localhost";
        }
    }
}
