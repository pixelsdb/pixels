/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Budgets include resource preparation and apply to coordinated S3QS execution. */
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
