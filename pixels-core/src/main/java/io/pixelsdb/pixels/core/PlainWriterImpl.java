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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.PhysicalWriterUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsProto.RowGroupInformation;
import io.pixelsdb.pixels.core.PixelsProto.RowGroupStatistic;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The plain writer is to write a file in columnar format without encoding.
 * All the column chunks are written as the plain values in memory.
 * <p/>
 * This is used to reduce decoding overhead when the data writing is not bounded
 * by the bandwidth of the underlying storage.
 *
 * Created at: 04/04/2022
 * Author: hank
 */
public class PlainWriterImpl implements PixelsWriter
{
    private static final Logger LOGGER = LogManager.getLogger(PlainWriterImpl.class);

    private final TypeDescription schema;
    private final int rowGroupSize;
    private final TimeZone timeZone;

    private final List<VectorizedRowBatch> bufferedRowBatches;
    private final StatsRecorder[] fileColStatRecorders;
    private long fileContentLength;
    private int fileRowNum;

    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;

    private final List<RowGroupInformation> rowGroupInfoList;    // row group information in footer
    private final List<RowGroupStatistic> rowGroupStatisticList; // row group statistic in footer

    private final PhysicalWriter physicalWriter;

    private PlainWriterImpl(
            TypeDescription schema,
            int rowGroupSize,
            TimeZone timeZone,
            PhysicalWriter physicalWriter)
    {
        this.schema = requireNonNull(schema, "schema is null");
        checkArgument(rowGroupSize > 0, "row group size is not positive");
        this.rowGroupSize = rowGroupSize;
        this.timeZone = requireNonNull(timeZone);

        List<TypeDescription> children = schema.getChildren();
        checkArgument(!requireNonNull(children, "schema is null").isEmpty(), "schema is empty");
        fileColStatRecorders = new StatsRecorder[children.size()];
        for (int i = 0; i < children.size(); ++i)
        {
            fileColStatRecorders[i] = StatsRecorder.create(children.get(i));
        }

        this.rowGroupInfoList = new LinkedList<>();
        this.rowGroupStatisticList = new LinkedList<>();
        this.bufferedRowBatches = new LinkedList<>();

        this.physicalWriter = physicalWriter;
    }

    public static class Builder
    {
        private TypeDescription builderSchema;
        private int builderRowGroupSize;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private Storage builderStorage;
        private String builderFilePath;
        private long builderBlockSize;
        private short builderReplication = 1;
        private boolean builderBlockPadding = true;

        private Builder()
        {
        }

        public Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = requireNonNull(schema);

            return this;
        }

        public Builder setRowGroupSize(int rowGroupSize)
        {
            this.builderRowGroupSize = rowGroupSize;

            return this;
        }

        public Builder setTimeZone(TimeZone timeZone)
        {
            this.builderTimeZone = requireNonNull(timeZone);

            return this;
        }

        public Builder setStorage(Storage storage)
        {
            this.builderStorage = requireNonNull(storage);

            return this;
        }

        public Builder setFilePath(String filePath)
        {
            this.builderFilePath = requireNonNull(filePath);

            return this;
        }

        public Builder setBlockSize(long blockSize)
        {
            checkArgument(blockSize > 0, "block size should be positive");
            this.builderBlockSize = blockSize;

            return this;
        }

        public Builder setReplication(short replication)
        {
            checkArgument(replication > 0, "num of replicas should be positive");
            this.builderReplication = replication;

            return this;
        }

        public Builder setBlockPadding(boolean blockPadding)
        {
            this.builderBlockPadding = blockPadding;

            return this;
        }

        public PixelsWriter build()
                throws PixelsWriterException
        {
            PhysicalWriter fsWriter = null;
            try
            {
                fsWriter = PhysicalWriterUtil.newPhysicalWriter(
                        this.builderStorage, this.builderFilePath, this.builderBlockSize, this.builderReplication,
                        this.builderBlockPadding);
            } catch (IOException e)
            {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException(
                        "Failed to create PixelsWriter due to error of creating PhysicalWriter", e);
            }
            checkArgument(!requireNonNull(builderSchema.getChildren(), "schema is null").isEmpty(),
                    "schema is empty");

            if (fsWriter == null)
            {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException(
                        "Failed to create PixelsWriter due to error of creating PhysicalWriter");
            }

            return new PlainWriterImpl(
                    builderSchema,
                    builderRowGroupSize,
                    builderTimeZone,
                    fsWriter);
        }
    }

    /**
     * Add row batch into the file.
     * The row batch will be buffered in this writer, thus the row batch should not be
     * reused or modified after returning from addRowBatch().
     *
     * @param rowBatch the row batch.
     * @return if the file adds a new row group, return false. Else, return true.
     */
    @Override
    public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException
    {
        if (curRowGroupDataLength + rowBatch.getMemoryUsage() > rowGroupSize)
        {
            // write row group and reset.
            curRowGroupDataLength = 0;
            curRowGroupNumOfRows = 0;
        }
        bufferedRowBatches.add(rowBatch);
        curRowGroupDataLength += rowBatch.getMemoryUsage();
        curRowGroupNumOfRows += rowBatch.size;
        return true;
    }

    /**
     * Get schema of this file.
     *
     * @return schema
     */
    @Override
    public TypeDescription getSchema()
    {
        return schema;
    }

    /**
     * Close PlainWriterImpl, indicating the end of file.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException
    {

    }
}
