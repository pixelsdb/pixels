/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import java.io.IOException;
import java.util.*;

/** File-tail estimates and worker-only exact fallback, never reduced by filter selectivity. */
public final class PartitionInputSizing
{
    private PartitionInputSizing() {}

    /** Fixed vector layout without allocating row-sized arrays for estimation. */
    public static long decodedVectorBytes(TypeDescription schema, long rows)
    {
        if (rows < 0) { throw new IllegalArgumentException("negative row count"); }
        long base = schema.createRowBatch(0, TypeDescription.Mode.NONE).getMemoryUsage();
        long perRow = schema.createRowBatch(1, TypeDescription.Mode.NONE).getMemoryUsage() - base;
        for (TypeDescription type : schema.getChildren())
        {
            // BinaryColumnVector's historical counter omits byte[][] references.
            if (isVariable(type)) { perRow = Math.addExact(perRow, Long.BYTES); }
        }
        return Math.addExact(base, Math.multiplyExact(rows, perRow));
    }

    public static boolean isVariable(TypeDescription type)
    {
        switch (type.getCategory())
        {
            case STRING: case BINARY: case VARBINARY: case VARCHAR: case CHAR: return true;
            default: return false;
        }
    }

    /** Existing metadata length sums estimate uncompressed variable payload. */
    public static long variableBytes(io.pixelsdb.pixels.common.metadata.domain.Column column)
    {
        if (column == null || column.getRecordStats() == null || !column.getRecordStats().hasRemaining()) { return -1; }
        try
        {
            PixelsProto.ColumnStatistic stat = PixelsProto.ColumnStatistic.parseFrom(column.getRecordStats().duplicate());
            if (stat.hasStringStatistics() && stat.getStringStatistics().hasSum())
            {
                long length = stat.getStringStatistics().getSum();
                // Stored Java String.length() is UTF-16 units; UTF-8 needs <= 3 bytes per unit.
                return length < 0 ? -1 : Math.multiplyExact(3, length);
            }
            if (stat.hasBinaryStatistics() && stat.getBinaryStatistics().hasSum())
            {
                return Math.max(-1, stat.getBinaryStatistics().getSum());
            }
        }
        catch (com.google.protobuf.InvalidProtocolBufferException | ArithmeticException invalid) { /* Use the configurable fallback. */ }
        return -1;
    }

    /** A fallback for absent row/length statistics, not a fixed compression promise. */
    public static double expansionFactor()
    {
        String key = "executor.partition.memory.expansion.factor";
        String configured = io.pixelsdb.pixels.common.utils.ConfigFactory.Instance().getProperty(key);
        double factor;
        try { factor = configured == null ? 4 : Double.parseDouble(configured.trim()); }
        catch (NumberFormatException error) { throw new IllegalArgumentException(key + " must be finite and >= 1", error); }
        if (!Double.isFinite(factor) || factor < 1) { throw new IllegalArgumentException(key + " must be finite and >= 1"); }
        return factor;
    }

    public static int end(InputInfo input, int groups)
    {
        if (input.getRgStart() < 0) { throw new IllegalArgumentException("negative row-group start"); }
        return input.getRgLength() <= 0 ? groups :
                (int) Math.min(groups, (long) input.getRgStart() + input.getRgLength());
    }

    /** No IO: the whole RG bounds the selected chunks, including the hidden column and padding. */
    public static long encodedUpperBound(PixelsReader reader, int group)
    {
        PixelsProto.RowGroupInformation info = reader.getRowGroupInfo(group);
        return info.hasDataLength() ? Integer.toUnsignedLong(info.getDataLength()) : -1;
    }

    /** Worker-only fallback for missing lengths or an oversized whole RG with a narrow projection. */
    public static long selectedEncodedBytes(PixelsReader reader, int group, String[] columns) throws IOException
    {
        PixelsProto.RowGroupIndex index = reader.getRowGroupFooter(group).getRowGroupIndexEntry();
        List<String> names = reader.getFileSchema().getFieldNames();
        long bytes = 0;
        for (String column : columns)
        {
            int id = names.indexOf(column);
            // Missing schema-evolved columns are synthesized, rather than fetched.
            if (id >= 0) { bytes = Math.addExact(bytes, Integer.toUnsignedLong(index.getColumnChunkIndexEntries(id).getChunkLength())); }
        }
        if (index.hasHiddenColumnChunkIndexEntry())
        {
            bytes = Math.addExact(bytes, Integer.toUnsignedLong(index.getHiddenColumnChunkIndexEntry().getChunkLength()));
        }
        return bytes;
    }

}
