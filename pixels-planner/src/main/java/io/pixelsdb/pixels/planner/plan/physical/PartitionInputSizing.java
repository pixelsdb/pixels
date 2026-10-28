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
