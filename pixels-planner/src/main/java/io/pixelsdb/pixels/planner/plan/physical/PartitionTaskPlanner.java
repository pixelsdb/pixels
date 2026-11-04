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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import java.util.*;

/** Coarse resource-aware task packing. No file readers or object-storage requests belong here. */
public final class PartitionTaskPlanner
{
    private PartitionTaskPlanner() {}

    public static List<List<InputSplit>> original(List<InputSplit> splits, int parallelism)
    {
        if (parallelism <= 0) { throw new IllegalArgumentException("parallelism must be positive"); }
        List<List<InputSplit>> result = new ArrayList<>();
        for (int start = 0; start < splits.size(); start += parallelism)
        {
            result.add(new ArrayList<>(splits.subList(start, Math.min(splits.size(), start + parallelism))));
        }
        return result;
    }

    /** Statistics are averages, never a promise of the worker's actual memory use. */
    public static List<List<InputSplit>> plan(List<InputSplit> splits, List<Column> statistics, long rowCount,
            String[] columns, boolean[] projection, int batchSize, PartitionMemoryPolicy policy,
            boolean streaming, int partitions, long fallbackRowGroupBytes)
    {
        policy.validate();
        if (batchSize <= 0 || partitions <= 0 || columns.length != projection.length)
        {
            throw new IllegalArgumentException("invalid partition task shape");
        }
        Map<String, Column> byName = new HashMap<>();
        double totalBytes = 0, totalChunkBytes = 0;
        for (Column column : statistics)
        {
            byName.put(column.getName(), column);
            totalBytes += Math.max(0, column.getSize());
            totalChunkBytes += Math.max(0, column.getChunkSize());
        }
        double groups = totalChunkBytes > 0 ? Math.ceil(totalBytes / totalChunkBytes) : 0;
        double rows = groups > 0 && rowCount > 0 ? rowCount / groups : 0;
        double encoded = 0, variable = 0, inputVariable = 0;
        double expansion = PartitionInputSizing.expansionFactor();
        TypeDescription schema = TypeDescription.createStruct(), inputSchema = TypeDescription.createStruct();
        for (int i = 0; i < columns.length; i++)
        {
            Column column = byName.get(columns[i]);
            double bytes = column == null ? 0 : Math.max(0, column.getChunkSize());
            encoded += bytes;
            TypeDescription type = column == null ? TypeDescription.createLong() : TypeDescription.fromString(column.getType());
            inputSchema.addField(columns[i], type.clone());
            double payload = 0;
            if (PartitionInputSizing.isVariable(type))
            {
                long length = PartitionInputSizing.variableBytes(column);
                payload = length >= 0 && groups > 0 ? length / groups : bytes * expansion;
                inputVariable += payload;
            }
            if (projection[i])
            {
                schema.addField(columns[i], type);
                variable += payload;
            }
        }
        if (!(encoded > 0) || !Double.isFinite(encoded)) { encoded = Math.max(1, fallbackRowGroupBytes); }
        long batch = PartitionInputSizing.decodedVectorBytes(schema, batchSize) + 128;
        boolean knownRows = rows > 0;
        if (!knownRows) { rows = encoded / Math.max(1, batch / (double) batchSize); }
        double expanded = knownRows ? PartitionInputSizing.decodedVectorBytes(inputSchema, (long) Math.ceil(rows)) + inputVariable : encoded * expansion;
        double retained = 1.5 * (Math.ceil(Math.max(1, rows) / batchSize) * batch + variable);
        long perReaderTails = Math.multiplyExact(batch + 4L * batchSize, partitions);
        long tailSpace = PartitionMemoryPolicy.usableBytes(policy.getWorkerBytes(), Long.MAX_VALUE) * 15 / 100;
        int memoryReaders = (int) Math.min(Integer.MAX_VALUE, tailSpace / perReaderTails);
        if (memoryReaders < 1) { throw new IllegalArgumentException("one reader's partition tails exceed backend memory"); }
        policy.setReaderThreads(Math.min(policy.getReaderThreads(), memoryReaders));
        long tails = Math.multiplyExact(Math.multiplyExact(batch, partitions), policy.getReaderThreads());
        // About five buffer turnovers per streaming task; this is work sizing,
        // not a claim that all decoded task data is simultaneously resident.
        long limit = streaming ? Math.multiplyExact(5, Math.min(policy.getInputBytes(), policy.getOutputBytes())) :
                policy.getOutputBytes() * 9 / 10 - tails;
        if (limit <= 0) { throw new IllegalArgumentException("partition tail capacity exceeds backend memory; lower batch size or partition count"); }
        double cost = streaming ? Math.max(encoded, expanded) : Math.max(1, retained);
        // Keep at least one RG: coarse statistics must not reject an otherwise readable RG.
        long groupsPerTask = Math.max(1, Math.min(Integer.MAX_VALUE, (long) Math.floor(limit / cost)));
        return pack(splits, groupsPerTask, policy.getReaderThreads());
    }

    static List<List<InputSplit>> pack(List<InputSplit> inputs, long groupsPerTask, int readers)
    {
        List<List<InputSplit>> result = new ArrayList<>();
        List<InputInfo> task = new ArrayList<>();
        long groups = 0;
        for (InputSplit split : inputs)
        {
            for (InputInfo input : split.getInputInfos())
            {
                if (input.getRgStart() < 0) { throw new IllegalArgumentException("negative row-group start"); }
                if (input.getRgLength() <= 0)
                {
                    if (!task.isEmpty()) { result.add(scanSplits(task, groups, readers)); task = new ArrayList<>(); groups = 0; }
                    // Unknown intermediate range: do not invent row counts or inspect its file.
                    result.add(Collections.singletonList(new InputSplit(Collections.singletonList(
                            new InputInfo(input.getPath(), input.getRgStart(), input.getRgLength())))));
                    continue;
                }
                long start = input.getRgStart(), remaining = input.getRgLength();
                while (remaining > 0)
                {
                    int take = (int) Math.min(remaining, groupsPerTask - groups);
                    if (start > Integer.MAX_VALUE) { throw new IllegalArgumentException("row-group range overflow"); }
                    append(task, input.getPath(), (int) start, take);
                    start += take; remaining -= take; groups += take;
                    if (groups == groupsPerTask)
                    {
                        result.add(scanSplits(task, groups, readers)); task = new ArrayList<>(); groups = 0;
                    }
                }
            }
        }
        if (!task.isEmpty()) { result.add(scanSplits(task, groups, readers)); }
        return result;
    }

    private static void append(List<InputInfo> ranges, String path, int start, int length)
    {
        if (!ranges.isEmpty())
        {
            InputInfo last = ranges.get(ranges.size() - 1);
            if (last.getPath().equals(path) && (long) last.getRgStart() + last.getRgLength() == start &&
                    (long) last.getRgLength() + length <= Integer.MAX_VALUE)
            {
                last.setRgLength(last.getRgLength() + length);
                return;
            }
        }
        ranges.add(new InputInfo(path, start, length));
    }

    /** Give each reader multiple independent slices; no fixed eight-split task cap. */
    private static List<InputSplit> scanSplits(List<InputInfo> ranges, long groups, int readers)
    {
        long size = Math.max(1, (groups + 2L * readers - 1) / (2L * readers));
        List<InputSplit> result = new ArrayList<>();
        List<InputInfo> split = new ArrayList<>();
        long filled = 0;
        for (InputInfo range : ranges)
        {
            long start = range.getRgStart(), remaining = range.getRgLength();
            while (remaining > 0)
            {
                int take = (int) Math.min(remaining, size - filled);
                append(split, range.getPath(), (int) start, take);
                filled += take; start += take; remaining -= take;
                if (filled == size) { result.add(new InputSplit(split)); split = new ArrayList<>(); filled = 0; }
            }
        }
        if (!split.isEmpty()) { result.add(new InputSplit(split)); }
        return result;
    }
}
