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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Execution deadline includes preparation. Legacy cleanup budgets remain for configuration compatibility. */
public final class QueryExecutionConfig
{
    final long timeoutMs;
    final long abortGraceMs;
    final long cleanupTimeoutMs;
    final Path manifestDirectory;

    public QueryExecutionConfig(long timeoutMs, long abortGraceMs, long cleanupTimeoutMs, Path manifestDirectory)
    {
        if (timeoutMs <= 0 || abortGraceMs <= 0 || cleanupTimeoutMs <= 0 || manifestDirectory == null)
        {
            throw new IllegalArgumentException("query budgets must be positive and manifest directory must be set");
        }
        this.timeoutMs = timeoutMs;
        this.abortGraceMs = abortGraceMs;
        this.cleanupTimeoutMs = cleanupTimeoutMs;
        this.manifestDirectory = manifestDirectory;
    }

    public static QueryExecutionConfig fromConfig()
    {
        String directory = ConfigFactory.Instance().getProperty("executor.s3qs.manifest.directory");
        if (directory == null || directory.trim().isEmpty())
        {
            String pixelsHome = System.getenv("PIXELS_HOME");
            if (pixelsHome == null || pixelsHome.trim().isEmpty())
            {
                throw new IllegalStateException("PIXELS_HOME or executor.s3qs.manifest.directory is required");
            }
            directory = Paths.get(pixelsHome, "var", "s3qs").toString();
        }
        return new QueryExecutionConfig(value("executor.s3qs.query.timeout.ms", 600000),
                value("executor.s3qs.abort.grace.ms", 30000),
                value("executor.s3qs.cleanup.timeout.ms", 10000), Paths.get(directory));
    }

    private static long value(String key, long fallback)
    {
        String value = ConfigFactory.Instance().getProperty(key);
        return value == null || value.trim().isEmpty() ? fallback : Long.parseLong(value);
    }
}
