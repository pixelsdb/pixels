/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.plan.physical.domain;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.turbo.Invoker;

/** Producer budgets from backend resources and an optional conservative planning cap. */
public class PartitionMemoryPolicy
{
    private long workerBytes;
    private long inputBytes;
    private long outputBytes;
    private int readerThreads;
    private int windowRowGroups;

    public PartitionMemoryPolicy() {}

    public PartitionMemoryPolicy(long workerBytes, long inputBytes, long outputBytes,
                                 int readerThreads, int windowRowGroups)
    {
        this.workerBytes = workerBytes;
        this.inputBytes = inputBytes;
        this.outputBytes = outputBytes;
        this.readerThreads = readerThreads;
        this.windowRowGroups = windowRowGroups;
        validate();
    }

    public static boolean enabled(String phase)
    {
        return Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.partition.memory." + phase + ".enabled"));
    }

    /** A planning cap can lower memory; CPU and actual backend resources stay unchanged. */
    public static PartitionMemoryPolicy fromBackend(Invoker invoker)
    {
        int backendMB = invoker == null ? 0 : invoker.getMemoryMB();
        if (backendMB <= 0)
        {
            throw new IllegalArgumentException("backend must report positive worker memory for partition planning");
        }
        String key = "executor.partition.memory.planning.mb";
        String configured = ConfigFactory.Instance().getProperty(key);
        long capMB;
        try { capMB = configured == null ? 0 : Long.parseLong(configured.trim()); }
        catch (NumberFormatException invalid) { throw new IllegalArgumentException(key + " must be a nonnegative integer in MiB", invalid); }
        if (capMB < 0) { throw new IllegalArgumentException(key + " must be nonnegative; 0 uses backend memory"); }
        long plannedMB = capMB == 0 ? backendMB : Math.min(backendMB, capMB);
        return forResources(Math.multiplyExact(plannedMB, 1024L * 1024), invoker.getCpuCount());
    }

    public static PartitionMemoryPolicy forResources(long memoryBytes, int cpuCount)
    {
        if (memoryBytes <= 0) { throw new IllegalArgumentException("worker memory must be positive"); }
        long usable = usableBytes(memoryBytes, Long.MAX_VALUE);
        // Separate byte pools retain writer progress space. 15% of physical
        // memory remains outside this working-set model for native/SDK overhead.
        long pool = Math.max(1, usable * 35 / 100);
        int readers = Math.max(1, cpuCount);
        return new PartitionMemoryPolicy(memoryBytes, pool, pool, readers, 0);
    }

    public static long usableBytes(long memoryBytes, long heapBytes)
    {
        return Math.max(1, Math.min(memoryBytes - memoryBytes * 15 / 100, heapBytes));
    }

    public void validate()
    {
        if (workerBytes <= 0 || inputBytes <= 0 || outputBytes <= 0 || readerThreads <= 0 || windowRowGroups < 0 ||
                inputBytes > workerBytes * 3 / 4 || outputBytes > workerBytes * 3 / 4 - inputBytes)
        {
            throw new IllegalArgumentException("invalid partition memory policy: reserve runtime, decoded-batch and native headroom");
        }
    }

    public long getWorkerBytes() { return workerBytes; }
    public void setWorkerBytes(long value) { workerBytes = value; }
    public long getInputBytes() { return inputBytes; }
    public void setInputBytes(long value) { inputBytes = value; }
    public long getOutputBytes() { return outputBytes; }
    public void setOutputBytes(long value) { outputBytes = value; }
    public int getReaderThreads() { return readerThreads; }
    public void setReaderThreads(int value) { readerThreads = value; }
    public int getWindowRowGroups() { return windowRowGroups; }
    public void setWindowRowGroups(int value) { windowRowGroups = value; }
}
