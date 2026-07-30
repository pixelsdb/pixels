/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Immutable configuration for query-local S3QS stage worker scaling.
 */
public class StageScalingConfig
{
    private final boolean enabled;
    private final long samplePeriodMs;
    private final int initialWorkers;
    private final int minWorkers;
    private final int maxWorkers;
    private final int targetPendingTasksPerWorker;
    private final int maxScaleOutStep;
    private final int maxScaleInStep;
    private final int scaleInStableSamples;
    private final long scaleInCooldownMs;

    public StageScalingConfig(boolean enabled, long samplePeriodMs, int initialWorkers,
                              int minWorkers, int maxWorkers, int targetPendingTasksPerWorker,
                              int maxScaleOutStep, int maxScaleInStep,
                              int scaleInStableSamples, long scaleInCooldownMs)
    {
        checkArgument(samplePeriodMs > 0, "samplePeriodMs must be positive");
        checkArgument(minWorkers >= 0, "minWorkers is negative");
        checkArgument(initialWorkers >= minWorkers, "initialWorkers is less than minWorkers");
        checkArgument(maxWorkers >= initialWorkers, "maxWorkers is less than initialWorkers");
        checkArgument(targetPendingTasksPerWorker > 0,
                "targetPendingTasksPerWorker must be positive");
        checkArgument(maxScaleOutStep > 0, "maxScaleOutStep must be positive");
        checkArgument(maxScaleInStep > 0, "maxScaleInStep must be positive");
        checkArgument(scaleInStableSamples > 0, "scaleInStableSamples must be positive");
        checkArgument(scaleInCooldownMs >= 0, "scaleInCooldownMs is negative");
        this.enabled = enabled;
        this.samplePeriodMs = samplePeriodMs;
        this.initialWorkers = initialWorkers;
        this.minWorkers = minWorkers;
        this.maxWorkers = maxWorkers;
        this.targetPendingTasksPerWorker = targetPendingTasksPerWorker;
        this.maxScaleOutStep = maxScaleOutStep;
        this.maxScaleInStep = maxScaleInStep;
        this.scaleInStableSamples = scaleInStableSamples;
        this.scaleInCooldownMs = scaleInCooldownMs;
    }

    public static StageScalingConfig fromConfig()
    {
        ConfigFactory config = ConfigFactory.Instance();
        return new StageScalingConfig(
                booleanProperty(config, "executor.s3qs.scaling.enabled", false),
                longProperty(config, "executor.s3qs.scaling.sample.period.ms", 1000L),
                intProperty(config, "executor.s3qs.scaling.initial.workers", 1),
                intProperty(config, "executor.s3qs.scaling.min.workers", 1),
                intProperty(config, "executor.s3qs.scaling.max.workers", 32),
                intProperty(config, "executor.s3qs.scaling.target.pending.tasks.per.worker", 1),
                intProperty(config, "executor.s3qs.scaling.max.scale.out.step", 4),
                intProperty(config, "executor.s3qs.scaling.max.scale.in.step", 1),
                intProperty(config, "executor.s3qs.scaling.scale.in.stable.samples", 3),
                longProperty(config, "executor.s3qs.scaling.scale.in.cooldown.ms", 5000L));
    }

    private static boolean booleanProperty(ConfigFactory config, String key, boolean defaultValue)
    {
        String value = config.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : Boolean.parseBoolean(value);
    }

    private static int intProperty(ConfigFactory config, String key, int defaultValue)
    {
        String value = config.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : Integer.parseInt(value);
    }

    private static long longProperty(ConfigFactory config, String key, long defaultValue)
    {
        String value = config.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : Long.parseLong(value);
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public long getSamplePeriodMs()
    {
        return samplePeriodMs;
    }

    public int getInitialWorkers()
    {
        return initialWorkers;
    }

    public int getMinWorkers()
    {
        return minWorkers;
    }

    public int getMaxWorkers()
    {
        return maxWorkers;
    }

    public int getTargetPendingTasksPerWorker()
    {
        return targetPendingTasksPerWorker;
    }

    public int getMaxScaleOutStep()
    {
        return maxScaleOutStep;
    }

    public int getMaxScaleInStep()
    {
        return maxScaleInStep;
    }

    public int getScaleInStableSamples()
    {
        return scaleInStableSamples;
    }

    public long getScaleInCooldownMs()
    {
        return scaleInCooldownMs;
    }
}
