/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina.util;

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.retina.util.flat.*;

import java.nio.ByteBuffer;

public class RowBatchSerializationUtil
{
    /**
     * Serialize VectorizedRowBatch to byte array
     * @param batch
     * @return
     */
    public static byte[] serialize(VectorizedRowBatch batch)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        int[] columnVectorOffsets = new int[batch.cols.length];
        for (int i = 0; i < batch.cols.length; ++i)
        {
            columnVectorOffsets[i] = serializeColumnVector(builder, batch.cols[i]);
        }

        int colsOffset = VectorizedRowBatchFlat.createColsVector(builder, columnVectorOffsets);
        VectorizedRowBatchFlat.startVectorizedRowBatchFlat(builder);
        VectorizedRowBatchFlat.addNumCols(builder, batch.numCols);
        VectorizedRowBatchFlat.addCols(builder, colsOffset);
        VectorizedRowBatchFlat.addSize(builder, batch.size);
        VectorizedRowBatchFlat.addProjectionSize(builder, batch.projectionSize);
        VectorizedRowBatchFlat.addMaxSize(builder, batch.maxSize);
        VectorizedRowBatchFlat.addMemoryUsage(builder, batch.getMemoryUsage());
        VectorizedRowBatchFlat.addEndOfFile(builder, batch.endOfFile);
        int batchOffset = VectorizedRowBatchFlat.endVectorizedRowBatchFlat(builder);

        builder.finish(batchOffset);
        return builder.sizedByteArray();
    }

    /**
     * Deserialize byte arrays into VectorizedRowBatch
     * @param data
     * @return
     */
    public static VectorizedRowBatch deserialize(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        VectorizedRowBatchFlat batchFlat = VectorizedRowBatchFlat.getRootAsVectorizedRowBatchFlat(buffer);

        VectorizedRowBatch batch = new VectorizedRowBatch(batchFlat.numCols());
        batch.size = batchFlat.size();
        batch.projectionSize = batchFlat.projectionSize();
        batch.maxSize = batchFlat.maxSize();
        batch.endOfFile = batchFlat.endOfFile();

        // deserialize column vector
        ColumnVector[] cols = new ColumnVector[batchFlat.colsLength()];
        for (int i = 0; i < cols.length; ++i)
        {
            cols[i] = deserializeColumnVector(batchFlat.cols(i));
        }
        batch.cols = cols;
        return batch;
    }

    private static int serializeColumnVector(FlatBufferBuilder builder, ColumnVector vector)
    {
        ColumnVectorBaseFlat.startColumnVectorBaseFlat(builder);
        ColumnVectorBaseFlat.addLength(builder, vector.getLength());
        ColumnVectorBaseFlat.addWriteIndex(builder, vector.getWriteIndex());
        ColumnVectorBaseFlat.addMemoryUsage(builder, vector.getMemoryUsage());
        ColumnVectorBaseFlat.addIsRepeating(builder, vector.isRepeating());
        ColumnVectorBaseFlat.addDuplicated(builder, vector.duplicated);
        ColumnVectorBaseFlat.addOriginVecId(builder, vector.originVecId);
        ColumnVectorBaseFlat.addIsNull(builder, ColumnVectorBaseFlat.createIsNullVector(builder, vector.isNull));
        ColumnVectorBaseFlat.addNoNulls(builder, vector.noNulls);
        ColumnVectorBaseFlat.addPreFlattenIsRepeating(builder, vector.getPreFlattenIsRepeating());
        ColumnVectorBaseFlat.addPreFlattenNoNulls(builder, vector.getPreFlattenNoNulls());
        int baseOffset = ColumnVectorBaseFlat.endColumnVectorBaseFlat(builder);

        if (vector instanceof BinaryColumnVector)
        {
            return serializeBinaryColumnVector(builder, (BinaryColumnVector) vector, baseOffset);
        } else if (vector instanceof ByteColumnVector)
        {
            return serializeByteColumnVector(builder, (ByteColumnVector) vector, baseOffset);
        } else if (vector instanceof DateColumnVector)
        {
            return serializeDateColumnVector(builder, (DateColumnVector) vector, baseOffset);
        } else if (vector instanceof DecimalColumnVector)
        {
            return serializeDecimalColumnVector(builder, (DecimalColumnVector) vector, baseOffset);
        } else if (vector instanceof DictionaryColumnVector)
        {
            return serializeDictionaryColumnVector(builder, (DictionaryColumnVector) vector, baseOffset);
        } else if (vector instanceof DoubleColumnVector)
        {
            return serializeDoubleColumnVector(builder, (DoubleColumnVector) vector, baseOffset);
        } else if (vector instanceof FloatColumnVector)
        {
            return serializeFloatColumnVector(builder, (FloatColumnVector) vector, baseOffset);
        } else if (vector instanceof IntColumnVector)
        {
            return serializeIntColumnVector(builder, (IntColumnVector) vector, baseOffset);
        } else if (vector instanceof LongColumnVector)
        {
            return serializeLongColumnVector(builder, (LongColumnVector) vector, baseOffset);
        } else if (vector instanceof LongDecimalColumnVector)
        {
            return serializeLongDecimalColumnVector(builder, (LongDecimalColumnVector) vector, baseOffset);
        } else if (vector instanceof StructColumnVector)
        {
            return serializeStructColumnVector(builder, (StructColumnVector) vector, baseOffset);
        } else if (vector instanceof TimeColumnVector)
        {
            return serializeTimeColumnVector(builder, (TimeColumnVector) vector, baseOffset);
        } else if (vector instanceof TimestampColumnVector)
        {
            return serializeTimestampColumnVector(builder, (TimestampColumnVector) vector, baseOffset);
        }

        throw new UnsupportedOperationException("Unsupported column vector type: " + vector.getClass());
    }

    private static ColumnVector deserializeColumnVector(ColumnVectorFlat vectorFlat)
    {
        ColumnVectorBaseFlat base = vectorFlat.base();

        switch (vectorFlat.columnVectorType())
        {
        }
    }

    private static int serializeBinaryColumnVector(FlatBufferBuilder builder, BinaryColumnVector vector, int baseOffset)
    {
        int[] byteArrayOffsets = new int[vector.vector.length];
        for (int i = 0; i < vector.vector.length; ++i)
        {
            int bytesOffset = ByteArray.createBytesVector(builder, vector.vector[i]);
            ByteArray.startByteArray(builder);
            ByteArray.addBytes(builder, bytesOffset);
            byteArrayOffsets[i] = ByteArray.endByteArray(builder);
        }

        BinaryColumnVectorFlat.startBinaryColumnVectorFlat(builder);
        BinaryColumnVectorFlat.addBase(builder, baseOffset);
        BinaryColumnVectorFlat.addVector(builder, BinaryColumnVectorFlat.createVectorVector(builder, vector.vector));

    }

    private static int serializeByteColumnVector(FlatBufferBuilder builder, ByteColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeDateColumnVector(FlatBufferBuilder builder, DateColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeDecimalColumnVector(FlatBufferBuilder builder, DecimalColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeDictionaryColumnVector(FlatBufferBuilder builder, DictionaryColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeDoubleColumnVector(FlatBufferBuilder builder, DoubleColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeFloatColumnVector(FlatBufferBuilder builder, FloatColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeIntColumnVector(FlatBufferBuilder builder, IntColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeLongColumnVector(FlatBufferBuilder builder, LongColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeLongDecimalColumnVector(FlatBufferBuilder builder, LongDecimalColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeStructColumnVector(FlatBufferBuilder builder, StructColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeTimeColumnVector(FlatBufferBuilder builder, TimeColumnVector vector, int baseOffset)
    {
        return 0;
    }

    private static int serializeTimestampColumnVector(FlatBufferBuilder builder, TimestampColumnVector vector, int baseOffset)
    {
        return 0;
    }
}
