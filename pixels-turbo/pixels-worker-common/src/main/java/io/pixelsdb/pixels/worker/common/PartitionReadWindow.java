/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.planner.plan.physical.PartitionInputSizing;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import java.io.IOException;

/** Consecutive RGs within one existing logical input; no task/producer identity changes. */
final class PartitionReadWindow
{
    final InputInfo input;
    final long bytes, encodedBytes, decodedBytes;
    final boolean exactSizing;

    private PartitionReadWindow(InputInfo input, long encodedBytes, long decodedBytes, boolean exactSizing)
    {
        this.input = input;
        this.encodedBytes = encodedBytes;
        this.decodedBytes = decodedBytes;
        this.bytes = Math.addExact(Math.multiplyExact(2, encodedBytes), decodedBytes);
        this.exactSizing = exactSizing;
    }

    static PartitionReadWindow next(PixelsReader reader, InputInfo original, int start, int end,
                                    String[] columns, PartitionMemoryPolicy policy, long totalBudget) throws IOException
    {
        TypeDescription schema = reader.getFileSchema(), selected = TypeDescription.createStruct();
        int[] ids = new int[columns.length];
        for (int i = 0; i < columns.length; i++)
        {
            ids[i] = schema.getFieldNames().indexOf(columns[i]);
            selected.addField(columns[i], ids[i] < 0 ? TypeDescription.createLong() : schema.getChildren().get(ids[i]).clone());
        }
        long batch = PartitionInputSizing.decodedVectorBytes(selected, WorkerCommon.rowBatchSize);
        // Hidden timestamp plus bounded RLE literal arrays; variable payload is
        // backed by encoded chunks and detached output is charged separately.
        batch = Math.addExact(batch, 64 + 9L * WorkerCommon.rowBatchSize +
                2L * Long.BYTES * io.pixelsdb.pixels.common.utils.Constants.MAX_SCOPE * (columns.length + 1));
        long target = Math.max(1, totalBudget / policy.getReaderThreads());
        long encodedBytes = 0, decodedBytes = 0;
        boolean exactSizing = false;
        int next = start;
        while (next < end && (policy.getWindowRowGroups() == 0 || next - start < policy.getWindowRowGroups()))
        {
            // Normal sizing is entirely in-memory: file-tail lengths include
            // all columns and alignment, so also bound a selected-column read.
            long decoded = batch;
            for (int i = 0; i < ids.length; i++)
            {
                if (ids[i] >= 0 && PartitionInputSizing.isVariable(selected.getChildren().get(i)))
                {
                    decoded = Math.addExact(decoded, dictionaryBytes(reader, next, ids[i]));
                }
            }
            if (decoded > totalBudget)
            {
                if (next > start) { break; }
                throw new IOException("decoded batch and dictionary exceed input budget: " + original.getPath() + " rg=" + next);
            }
            long encoded = PartitionInputSizing.encodedUpperBound(reader, next);
            boolean exact = encoded < 0 || encoded > (totalBudget - decoded) / 2;
            if (exact && next > start) { break; }
            if (exact)
            {
                // Inspect only this current RG, never the remaining task. The
                // scanner will reuse the fetched footer through its cache.
                encoded = PartitionInputSizing.selectedEncodedBytes(reader, next, columns);
                exactSizing = true;
            }
            // Reserve for the chunks and a possible merged/read response copy.
            long candidateEncoded = Math.addExact(encodedBytes, encoded);
            long candidateDecoded = Math.max(decodedBytes, decoded);
            long cost = Math.addExact(Math.multiplyExact(2, candidateEncoded), candidateDecoded);
            if (next > start && cost > target) { break; }
            if (cost > totalBudget)
            {
                throw new IOException("single row group and decoded state exceed input budget: " + original.getPath() + " rg=" + next);
            }
            encodedBytes = candidateEncoded;
            decodedBytes = candidateDecoded;
            next++;
            if (exact) { break; }
        }
        if (next <= start) { throw new IllegalArgumentException("empty read window"); }
        return new PartitionReadWindow(new InputInfo(original.getPath(), start, next - start), encodedBytes, decodedBytes, exactSizing);
    }
    private static long dictionaryBytes(PixelsReader reader, int group, int column) throws IOException
    {
        PixelsProto.RowGroupFooter footer = reader.getRowGroupFooter(group);
        if (column >= footer.getRowGroupEncoding().getColumnChunkEncodingsCount())
        {
            throw new IOException("missing column encoding for dictionary sizing");
        }
        PixelsProto.ColumnEncoding encoding = footer.getRowGroupEncoding().getColumnChunkEncodings(column);
        if (encoding.getKind() != PixelsProto.ColumnEncoding.Kind.DICTIONARY) { return 0; }
        if (encoding.hasDictionarySize())
        {
            return Math.addExact(16, 4 * (Integer.toUnsignedLong(encoding.getDictionarySize()) + 1));
        }
        if (encoding.hasCascadeEncoding() && encoding.getCascadeEncoding().getKind() == PixelsProto.ColumnEncoding.Kind.RUNLENGTH)
        {
            PixelsProto.RowGroupInformation info = reader.getRowGroupInfo(group);
            if (!info.hasNumberOfRows()) { throw new IOException("missing row count for dictionary sizing"); }
            // Older dictionaries retain a growable offsets array and its copy.
            return 16384 + 8 * (Integer.toUnsignedLong(info.getNumberOfRows()) + 1);
        }
        return 16 + Integer.toUnsignedLong(footer.getRowGroupIndexEntry().getColumnChunkIndexEntries(column).getChunkLength());
    }

}
