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
package io.pixelsdb.pixels.executor.scan;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 10/07/2022
 */
public class Scanner
{
    private final TypeDescription outputSchema;
    private final boolean[] projection;
    private final int projectionSize;
    private final TableScanFilter filter;
    private final Bitmap filtered;
    private final Bitmap tmp;

    public Scanner(int rowBatchSize, TypeDescription inputSchema, String[] columnNames,
                   boolean[] projection, TableScanFilter filter)
    {
        checkArgument(rowBatchSize > 0, "rowBatchSize must be positive");
        this.projection = requireNonNull(projection, "projection is null");
        this.outputSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        List<TypeDescription> inputFields = requireNonNull(inputSchema, "inputSchema is null").getChildren();
        requireNonNull(inputFields, "fields in the inputSchema is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(inputFields.size() == projection.length && projection.length == columnNames.length,
                "columnNames.length, projection.length, and the number of fields in the inputSchema are not consistent");

        int projSize = 0;
        for (int i = 0; i < inputFields.size(); ++i)
        {
            if (this.projection[i])
            {
                this.outputSchema.addField(columnNames[i], inputFields.get(i));
                projSize++;
            }
        }
        this.projectionSize = projSize;

        this.filter = requireNonNull(filter, "filter is null");
        this.filtered = new Bitmap(rowBatchSize, true);
        this.tmp = new Bitmap(rowBatchSize, false);
    }

    public TypeDescription getOutputSchema()
    {
        return outputSchema;
    }

    /**
     * Apply filter and projection on the input row batch.
     * @param inputRowBatch the input row batch, should not be reused outsize this method
     * @return the input row batch after filter and projection
     */
    public VectorizedRowBatch filterAndProject(VectorizedRowBatch inputRowBatch)
    {
        if (!inputRowBatch.isEmpty())
        {
            this.filter.doFilter(inputRowBatch, filtered, tmp);
            inputRowBatch.applyFilter(filtered);
        }
        inputRowBatch.applyProjection(projection, projectionSize);
        return inputRowBatch;
    }
}
