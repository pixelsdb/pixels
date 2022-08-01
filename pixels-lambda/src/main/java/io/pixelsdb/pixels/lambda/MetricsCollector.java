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
package io.pixelsdb.pixels.lambda;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hank
 * @date 02/08/2022
 */
public class MetricsCollector
{
    AtomicInteger numReadRequests = new AtomicInteger(0);
    AtomicInteger numWriteRequests = new AtomicInteger(0);
    AtomicLong readBytes = new AtomicLong(0);
    AtomicLong writeBytes = new AtomicLong(0);
    AtomicInteger inputDurationMs = new AtomicInteger(0);
    AtomicInteger outputDurationMs = new AtomicInteger(0);
    AtomicInteger computeDurationMs = new AtomicInteger(0);

    public AtomicInteger getNumReadRequests()
    {
        return numReadRequests;
    }

    public AtomicInteger getNumWriteRequests()
    {
        return numWriteRequests;
    }

    public AtomicLong getReadBytes()
    {
        return readBytes;
    }

    public AtomicLong getWriteBytes()
    {
        return writeBytes;
    }

    public AtomicInteger getInputDurationMs()
    {
        return inputDurationMs;
    }

    public AtomicInteger getOutputDurationMs()
    {
        return outputDurationMs;
    }

    public AtomicInteger getComputeDurationMs()
    {
        return computeDurationMs;
    }

    public void addNumReadRequests(int numReadRequests)
    {
        this.numReadRequests.addAndGet(numReadRequests);
    }

    public void addNumWriteRequests(int numWriteRequests)
    {
        this.numWriteRequests.addAndGet(numWriteRequests);
    }

    public void addReadBytes(long readBytes)
    {
        this.readBytes.addAndGet(readBytes);
    }

    public void addWriteBytes(long writeBytes)
    {
        this.writeBytes.addAndGet(writeBytes);
    }

    public void addInputDurationMs(int inputDurationMs)
    {
        this.inputDurationMs.addAndGet(inputDurationMs);
    }

    public void addOutputDurationMs(int outputDurationMs)
    {
        this.outputDurationMs.addAndGet(outputDurationMs);
    }

    public void addComputeDurationMs(int computeDurationMs)
    {
        this.computeDurationMs.addAndGet(computeDurationMs);
    }
}
