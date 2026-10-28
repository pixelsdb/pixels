/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.planner.plan.physical.PartitionInputSizing;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import java.io.IOException;

/** Consecutive RGs within one existing logical input; no task/producer identity changes. */
final class PartitionReadWindow
{
    final InputInfo input;
    final long bytes;
    final boolean exactSizing;

    private PartitionReadWindow(InputInfo input, long bytes, boolean exactSizing)
    {
        this.input = input; this.bytes = bytes; this.exactSizing = exactSizing;
    }

    static PartitionReadWindow next(PixelsReader reader, InputInfo original, int start, int end,
                                    String[] columns, PartitionMemoryPolicy policy, long totalBudget) throws IOException
    {
        long target = Math.max(1, totalBudget / policy.getReaderThreads());
        long bytes = 0;
        boolean exactSizing = false;
        int next = start;
        while (next < end && (policy.getWindowRowGroups() == 0 || next - start < policy.getWindowRowGroups()))
        {
            // Normal sizing is entirely in-memory: file-tail lengths include
            // all columns and alignment, so also bound a selected-column read.
            long encoded = PartitionInputSizing.encodedUpperBound(reader, next);
            boolean exact = encoded < 0 || encoded > totalBudget / 2;
            if (exact && next > start) { break; }
            if (exact)
            {
                // Inspect only this current RG, never the remaining task. The
                // scanner will reuse the fetched footer through its cache.
                encoded = PartitionInputSizing.selectedEncodedBytes(reader, next, columns);
                exactSizing = true;
            }
            // Reserve for the chunks and a possible merged/read response copy.
            long cost = Math.multiplyExact(2, encoded);
            if (cost > totalBudget) { throw new IOException("single row group exceeds input budget: " + original.getPath() + " rg=" + next); }
            if (next > start && (cost > target || bytes > target - cost)) { break; }
            bytes = Math.addExact(bytes, cost);
            next++;
            if (exact) { break; }
        }
        if (next <= start) { throw new IllegalArgumentException("empty read window"); }
        return new PartitionReadWindow(new InputInfo(original.getPath(), start, next - start), Math.max(1, bytes), exactSizing);
    }
}
