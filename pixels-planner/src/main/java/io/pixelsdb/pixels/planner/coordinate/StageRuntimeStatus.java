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

/**
 * Read-only snapshot of one coordinator-managed stage.
 */
public class StageRuntimeStatus
{
    private final int desiredWorkerCount;
    private final int activeAttemptCount;
    private final int startingAttemptCount;
    private final int activeRegisteredWorkerCount;
    private final int acceptingWorkerCount;
    private final int drainingWorkerCount;
    private final int pendingTaskCount;
    private final int runningTaskCount;
    private final int completedTaskCount;
    private final int failedTaskCount;
    private final int totalTaskCount;
    private final boolean logicalTasksTerminal;
    private final long sampleTimeMs;

    public StageRuntimeStatus(int desiredWorkerCount, int activeAttemptCount, int startingAttemptCount,
                              int activeRegisteredWorkerCount, int acceptingWorkerCount,
                              int drainingWorkerCount, int pendingTaskCount,
                              int runningTaskCount, int completedTaskCount,
                              int failedTaskCount, int totalTaskCount,
                              boolean logicalTasksTerminal, long sampleTimeMs)
    {
        this.desiredWorkerCount = desiredWorkerCount;
        this.activeAttemptCount = activeAttemptCount;
        this.startingAttemptCount = startingAttemptCount;
        this.activeRegisteredWorkerCount = activeRegisteredWorkerCount;
        this.acceptingWorkerCount = acceptingWorkerCount;
        this.drainingWorkerCount = drainingWorkerCount;
        this.pendingTaskCount = pendingTaskCount;
        this.runningTaskCount = runningTaskCount;
        this.completedTaskCount = completedTaskCount;
        this.failedTaskCount = failedTaskCount;
        this.totalTaskCount = totalTaskCount;
        this.logicalTasksTerminal = logicalTasksTerminal;
        this.sampleTimeMs = sampleTimeMs;
    }

    public int getDesiredWorkerCount()
    {
        return desiredWorkerCount;
    }

    public int getActiveAttemptCount()
    {
        return activeAttemptCount;
    }

    public int getStartingAttemptCount()
    {
        return startingAttemptCount;
    }

    public int getActiveRegisteredWorkerCount()
    {
        return activeRegisteredWorkerCount;
    }

    public int getAcceptingWorkerCount()
    {
        return acceptingWorkerCount;
    }

    public int getDrainingWorkerCount()
    {
        return drainingWorkerCount;
    }

    public int getPendingTaskCount()
    {
        return pendingTaskCount;
    }

    public int getRunningTaskCount()
    {
        return runningTaskCount;
    }

    public int getCompletedTaskCount()
    {
        return completedTaskCount;
    }

    public int getFailedTaskCount()
    {
        return failedTaskCount;
    }

    public int getTotalTaskCount()
    {
        return totalTaskCount;
    }

    public boolean isLogicalTasksTerminal()
    {
        return logicalTasksTerminal;
    }

    public long getSampleTimeMs()
    {
        return sampleTimeMs;
    }
}
