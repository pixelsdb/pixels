/*
 * Copyright 2017-2019 PixelsDB.
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
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto.CompressionKind;
import io.pixelsdb.pixels.core.PixelsProto.RowGroupInformation;
import io.pixelsdb.pixels.core.PixelsProto.RowGroupStatistic;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.core.writer.ColumnWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.Constants.DEFAULT_HDFS_BLOCK_SIZE;
import static io.pixelsdb.pixels.core.TypeDescription.writeTypes;
import static io.pixelsdb.pixels.core.writer.ColumnWriter.newColumnWriter;
import static java.util.Objects.requireNonNull;

/**
 * Pixels file writer default implementation
 * <p>
 * This writer is NOT thread safe!
 *
 * @author guodong
 * @author hank
 */
@NotThreadSafe
public class PixelsWriterImpl implements PixelsWriter
{
    private static final Logger LOGGER = LogManager.getLogger(PixelsWriterImpl.class);

    private final TypeDescription schema;
    private final int pixelStride;
    private final int rowGroupSize;
    private final CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    private final boolean encoding;
    private final boolean partitioned;
    private final Optional<List<Integer>> partKeyColumnIds;

    private final ColumnWriter[] columnWriters;
    private final StatsRecorder[] fileColStatRecorders;
    private long fileContentLength;
    private int fileRowNum;

    private long writtenBytes = 0L;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;
    /**
     * Whether any current hash value has been set.
     */
    private boolean hashValueIsSet = false;
    private int currHashValue = 0;

    private final List<RowGroupInformation> rowGroupInfoList;    // row group information in footer
    private final List<RowGroupStatistic> rowGroupStatisticList; // row group statistic in footer

    private final PhysicalWriter physicalWriter;
    private final List<TypeDescription> children;

    private final ExecutorService columnWriterService = Executors.newCachedThreadPool();

    private PixelsWriterImpl(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            PhysicalWriter physicalWriter,
            boolean encoding,
            boolean partitioned,
            Optional<List<Integer>> partKeyColumnIds)
    {
        this.schema = requireNonNull(schema, "schema is null");
        checkArgument(pixelStride > 0, "pixel stripe is not positive");
        this.pixelStride = pixelStride;
        checkArgument(rowGroupSize > 0, "row group size is not positive");
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        checkArgument(compressionBlockSize > 0, "compression block size is not positive");
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone);
        this.encoding = encoding;
        this.partitioned = partitioned;
        this.partKeyColumnIds = requireNonNull(partKeyColumnIds, "partKeyColumnIds is null");

        children = schema.getChildren();
        checkArgument(!requireNonNull(children, "schema is null").isEmpty(), "schema is empty");
        this.columnWriters = new ColumnWriter[children.size()];
        fileColStatRecorders = new StatsRecorder[children.size()];
        for (int i = 0; i < children.size(); ++i)
        {
            columnWriters[i] = newColumnWriter(children.get(i), pixelStride, encoding);
            fileColStatRecorders[i] = StatsRecorder.create(children.get(i));
        }

        this.rowGroupInfoList = new LinkedList<>();
        this.rowGroupStatisticList = new LinkedList<>();

        this.physicalWriter = physicalWriter;
    }

    public static class Builder
    {
        private TypeDescription builderSchema = null;
        private int builderPixelStride = 0;
        private int builderRowGroupSize = 0;
        private CompressionKind builderCompressionKind = CompressionKind.NONE;
        private int builderCompressionBlockSize = 1;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private Storage builderStorage = null;
        private String builderFilePath = null;
        private long builderBlockSize = DEFAULT_HDFS_BLOCK_SIZE;
        private short builderReplication = 3;
        private boolean builderBlockPadding = true;
        private boolean builderOverwrite = false;
        private boolean builderEncoding = true;
        private boolean builderPartitioned = false;
        private Optional<List<Integer>> builderPartKeyColumnIds = Optional.empty();

        private Builder()
        {
        }

        public Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = requireNonNull(schema);

            return this;
        }

        public Builder setPixelStride(int stride)
        {
            this.builderPixelStride = stride;

            return this;
        }

        public Builder setRowGroupSize(int rowGroupSize)
        {
            this.builderRowGroupSize = rowGroupSize;

            return this;
        }

        public Builder setCompressionKind(CompressionKind compressionKind)
        {
            this.builderCompressionKind = requireNonNull(compressionKind);

            return this;
        }

        public Builder setCompressionBlockSize(int compressionBlockSize)
        {
            this.builderCompressionBlockSize = compressionBlockSize;

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

        public Builder setPath(String filePath)
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

        public Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;

            return this;
        }

        public Builder setEncoding(boolean encoding)
        {
            this.builderEncoding = encoding;

            return this;
        }

        public Builder setPartitioned(boolean partitioned)
        {
            this.builderPartitioned = partitioned;

            return this;
        }

        public Builder setPartKeyColumnIds(List<Integer> partitionColumnIds)
        {
            this.builderPartKeyColumnIds = Optional.ofNullable(partitionColumnIds);

            return this;
        }

        public PixelsWriter build()
                throws PixelsWriterException
        {
            requireNonNull(this.builderStorage, "storage is not set");
            requireNonNull(this.builderFilePath, "file path is not set");
            requireNonNull(this.builderSchema, "schema is not set");
            checkArgument(!requireNonNull(builderSchema.getChildren(),
                            "schema's children is null").isEmpty(), "schema is empty");
            checkArgument(this.builderPixelStride > 0, "pixels stride size is not set");
            checkArgument(this.builderRowGroupSize > 0, "row group size is not set");
            checkArgument(this.builderPartitioned ==
                            (this.builderPartKeyColumnIds.isPresent() && !this.builderPartKeyColumnIds.get().isEmpty()),
                    "partition column ids are present while partitioned is false, or vice versa");

            PhysicalWriter fsWriter = null;
            try
            {

                fsWriter = PhysicalWriterUtil.newPhysicalWriter(
                        this.builderStorage, this.builderFilePath, this.builderBlockSize, this.builderReplication,
                        this.builderBlockPadding, this.builderOverwrite);
            } catch (IOException e)
            {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException(
                        "Failed to create PixelsWriter due to error of creating PhysicalWriter", e);
            }

            if (fsWriter == null)
            {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException(
                        "Failed to create PixelsWriter due to error of creating PhysicalWriter");
            }

            return new PixelsWriterImpl(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    fsWriter,
                    builderEncoding,
                    builderPartitioned,
                    builderPartKeyColumnIds);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public TypeDescription getSchema()
    {
        return schema;
    }

    @Override
    public int getNumRowGroup()
    {
        return this.rowGroupInfoList.size();
    }

    @Override
    public int getNumWriteRequests()
    {
        if (physicalWriter == null)
        {
            return 0;
        }
        return (int) Math.ceil(writtenBytes / (double) physicalWriter.getBufferSize());
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    public int getPixelStride()
    {
        return pixelStride;
    }

    public int getRowGroupSize()
    {
        return rowGroupSize;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public int getCompressionBlockSize()
    {
        return compressionBlockSize;
    }

    public TimeZone getTimeZone()
    {
        return timeZone;
    }

    public boolean isEncoding()
    {
        return encoding;
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

    @Override
    public boolean addRowBatch(VectorizedRowBatch rowBatch)
            throws IOException
    {
        checkArgument(!partitioned, "this file is hash partitioned, " +
                "use addRowBatch(rowBatch, hashValue) instead");
        /**
         * Issue #170:
         * ColumnWriter.write() returns the total size of the current column chunk,
         * thus we should set curRowGroupDataLength = 0 here at the beginning.
         */
        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        writeColumnVectors(rowBatch.cols, rowBatch.size);
        // If the current row group size has exceeded the row group size, write current row group.
        if (curRowGroupDataLength >= rowGroupSize)
        {
            writeRowGroup();
            curRowGroupNumOfRows = 0L;
            return false;
        }
        return true;
    }

    @Override
    public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue) throws IOException
    {
        checkArgument(partitioned, "this file is not hash partitioned, " +
                "use addRowBatch(rowBatch) instead");
        if (hashValueIsSet)
        {
            // As the current hash value is set, at lease one row batch has been added.
            if (currHashValue != hashValue)
            {
                // Write the current partition (row group) and add the row batch to a new partition.
                writeRowGroup();
                curRowGroupNumOfRows = 0L;
            }
        }
        currHashValue = hashValue;
        hashValueIsSet = true;
        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        writeColumnVectors(rowBatch.cols, rowBatch.size);
    }

    private void writeColumnVectors(ColumnVector[] columnVectors, int rowBatchSize)
    {
        CompletableFuture<?>[] futures = new CompletableFuture[columnVectors.length];
        AtomicInteger dataLength = new AtomicInteger(0);
        for (int i = 0; i < columnVectors.length; ++i)
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ColumnWriter writer = columnWriters[i];
            ColumnVector columnVector = columnVectors[i];
            columnWriterService.execute(() ->
            {
                try
                {
                    dataLength.addAndGet(writer.write(columnVector, rowBatchSize));
                    future.complete(null);
                } catch (IOException e)
                {
                    throw new CompletionException("failed to write column vector", e);
                }
            });
            futures[i] = future;
        }
        CompletableFuture.allOf(futures).join();
        curRowGroupDataLength += dataLength.get();
    }

    /**
     * Close PixelsWriterImpl, indicating the end of file.
     */
    @Override
    public void close()
    {
        try
        {
            if (curRowGroupNumOfRows != 0)
            {
                writeRowGroup();
            }
            writeFileTail();
            physicalWriter.close();
            for (ColumnWriter cw : columnWriters)
            {
                cw.close();
            }
            columnWriterService.shutdown();
            columnWriterService.shutdownNow();
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void writeRowGroup()
            throws IOException
    {
        int rowGroupDataLength = 0;

        PixelsProto.RowGroupStatistic.Builder curRowGroupStatistic =
                PixelsProto.RowGroupStatistic.newBuilder();
        PixelsProto.RowGroupInformation.Builder curRowGroupInfo =
                PixelsProto.RowGroupInformation.newBuilder();
        PixelsProto.RowGroupIndex.Builder curRowGroupIndex =
                PixelsProto.RowGroupIndex.newBuilder();
        PixelsProto.RowGroupEncoding.Builder curRowGroupEncoding =
                PixelsProto.RowGroupEncoding.newBuilder();

        // reset each column writer and get current row group content size in bytes
        for (ColumnWriter writer : columnWriters)
        {
            // flush writes the isNull bit map into the internal output stream.
            writer.flush();
            rowGroupDataLength += writer.getColumnChunkSize();
        }

        // write and flush row group content
        try
        {
            curRowGroupOffset = physicalWriter.prepare(rowGroupDataLength);
            if (curRowGroupOffset != -1)
            {
                for (ColumnWriter writer : columnWriters)
                {
                    byte[] rowGroupBuffer = writer.getColumnChunkContent();
                    physicalWriter.append(rowGroupBuffer, 0, rowGroupBuffer.length);
                    writtenBytes += rowGroupBuffer.length;
                }
                physicalWriter.flush();
            }
            else
            {
                LOGGER.warn("Write row group prepare failed");
                throw new IOException("Write row group prepare failed");
            }
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            throw e;
        }

        // update index and stats
        rowGroupDataLength = 0;
        for (int i = 0; i < columnWriters.length; i++)
        {
            ColumnWriter writer = columnWriters[i];
            PixelsProto.ColumnChunkIndex.Builder chunkIndexBuilder = writer.getColumnChunkIndex();
            chunkIndexBuilder.setChunkOffset(curRowGroupOffset + rowGroupDataLength);
            chunkIndexBuilder.setChunkLength(writer.getColumnChunkSize());
            rowGroupDataLength += writer.getColumnChunkSize();
            // collect columnChunkIndex from every column chunk into curRowGroupIndex
            curRowGroupIndex.addColumnChunkIndexEntries(chunkIndexBuilder.build());
            // collect columnChunkStatistic into rowGroupStatistic
            curRowGroupStatistic.addColumnChunkStats(writer.getColumnChunkStat().build());
            // collect columnChunkEncoding
            curRowGroupEncoding.addColumnChunkEncodings(writer.getColumnChunkEncoding().build());
            // update file column statistic
            fileColStatRecorders[i].merge(writer.getColumnChunkStatRecorder());
            /* TODO: writer.reset() does not work for partitioned file writing, fix it later.
             * The possible reason is that: when the file is partitioned, the last stride of a row group
             * (a.k.a., partition) is likely not full (length < pixelsStride), thus if the writer is not
             * reset correctly, the strides of the next row group will not be written correctly.
             * We temporarily fix this problem by creating a new column writer for each row group.
             */
            // writer.reset();
            columnWriters[i] = newColumnWriter(children.get(i), pixelStride, encoding);
        }

        // put curRowGroupIndex into rowGroupFooter
        PixelsProto.RowGroupFooter rowGroupFooter =
                PixelsProto.RowGroupFooter.newBuilder()
                        .setRowGroupIndexEntry(curRowGroupIndex.build())
                        .setRowGroupEncoding(curRowGroupEncoding.build())
                        .build();

        // write and flush row group footer
        try
        {
            byte[] footerBuffer = rowGroupFooter.toByteArray();
            physicalWriter.prepare(footerBuffer.length);
            curRowGroupFooterOffset = physicalWriter.append(footerBuffer, 0, footerBuffer.length);
            writtenBytes += footerBuffer.length;
            physicalWriter.flush();
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            throw e;
        }

        // update RowGroupInformation, and put it into rowGroupInfoList
        curRowGroupInfo.setFooterOffset(curRowGroupFooterOffset);
        curRowGroupInfo.setDataLength(rowGroupDataLength);
        curRowGroupInfo.setFooterLength(rowGroupFooter.getSerializedSize());
        curRowGroupInfo.setNumberOfRows(curRowGroupNumOfRows);
        if (partitioned)
        {
            PixelsProto.PartitionInformation.Builder partitionInfo =
                    PixelsProto.PartitionInformation.newBuilder();
            // partitionColumnIds has been checked to be present in the builder.
            partitionInfo.addAllColumnIds(partKeyColumnIds.orElse(null));
            partitionInfo.setHashValue(currHashValue);
            curRowGroupInfo.setPartitionInfo(partitionInfo.build());
        }
        rowGroupInfoList.add(curRowGroupInfo.build());
        // put curRowGroupStatistic into rowGroupStatisticList
        rowGroupStatisticList.add(curRowGroupStatistic.build());

        this.fileRowNum += curRowGroupNumOfRows;
        this.fileContentLength += rowGroupDataLength;
    }

    private void writeFileTail()
            throws IOException
    {
        PixelsProto.Footer footer;
        PixelsProto.PostScript postScript;

        // build Footer
        PixelsProto.Footer.Builder footerBuilder =
                PixelsProto.Footer.newBuilder();
        writeTypes(footerBuilder, schema);
        for (StatsRecorder recorder : fileColStatRecorders)
        {
            footerBuilder.addColumnStats(recorder.serialize().build());
        }
        for (RowGroupInformation rowGroupInformation : rowGroupInfoList)
        {
            footerBuilder.addRowGroupInfos(rowGroupInformation);
        }
        for (RowGroupStatistic rowGroupStatistic : rowGroupStatisticList)
        {
            footerBuilder.addRowGroupStats(rowGroupStatistic);
        }
        footerBuilder.setPartitioned(partitioned);
        footer = footerBuilder.build();

        // build PostScript
        postScript = PixelsProto.PostScript.newBuilder()
                .setVersion(Constants.VERSION)
                .setContentLength(fileContentLength)
                .setNumberOfRows(fileRowNum)
                .setCompression(compressionKind)
                .setCompressionBlockSize(compressionBlockSize)
                .setPixelStride(pixelStride)
                .setWriterTimezone(timeZone.getDisplayName())
                .setMagic(Constants.MAGIC)
                .build();

        // build FileTail
        PixelsProto.FileTail fileTail =
                PixelsProto.FileTail.newBuilder()
                        .setFooter(footer)
                        .setPostscript(postScript)
                        .setFooterLength(footer.getSerializedSize())
                        .setPostscriptLength(postScript.getSerializedSize())
                        .build();

        // write and flush FileTail plus FileTail physical offset at the end of the file
        int fileTailLen = fileTail.getSerializedSize() + Long.BYTES;
        physicalWriter.prepare(fileTailLen);
        long tailOffset = physicalWriter.append(fileTail.toByteArray(), 0, fileTail.getSerializedSize());
        ByteBuffer tailOffsetBuffer = ByteBuffer.allocate(Long.BYTES);
        tailOffsetBuffer.putLong(tailOffset);
        physicalWriter.append(tailOffsetBuffer);
        writtenBytes += fileTailLen;
        physicalWriter.flush();
    }
}
