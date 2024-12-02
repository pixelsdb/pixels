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
package io.pixelsdb.pixels.core.compactor;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsVersion;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.exception.PixelsFileMagicInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsFileVersionInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsReaderException;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.Constants.DEFAULT_HDFS_BLOCK_SIZE;
import static java.util.Objects.requireNonNull;

/**
 * Pixels file compactor
 *
 * @author hank
 */
public class PixelsCompactor
{
    private static final Logger LOGGER = LogManager.getLogger(PixelsCompactor.class);

    private final TypeDescription schema;
    private final CompactLayout compactLayout;
    private final int pixelStride;
    private final PixelsProto.CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    private final long fileContentLength;
    private final int fileRowNum;
    /**
     * The number of bytes that each column chunk is aligned to.
     */
    private final int chunkAlignment;
    /**
     * The byte buffer padded to each column chunk for alignment.
     */
    private final byte[] chunkPaddingBuffer;

    private final Storage inputStorage;
    private final PhysicalWriter fsWriter;
    private final StatsRecorder[] fileColStatRecorders;
    private final StatsRecorder hiddenFileColStatRecorder; // hidden column stats recorder

    private final List<PixelsProto.RowGroupInformation.Builder> rowGroupInfoBuilderList;   // row group information in footer
    private final List<PixelsProto.RowGroupStatistic.Builder> rowGroupStatBuilderList;     // row group statistic in footer
    private final List<PixelsProto.RowGroupFooter.Builder> rowGroupFooterBuilderList;      // row group footers
    private final List<String> rowGroupPaths;

    private final boolean hasHiddenColumn;

    private PixelsCompactor(
            TypeDescription schema,
            boolean hasHiddenColumn,
            CompactLayout compactLayout,
            int pixelStride,
            PixelsProto.CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            long fileContentLength,
            int fileRowNum,
            Storage inputStorage,
            PhysicalWriter fsWriter,
            StatsRecorder[] fileColStatRecorders,
            StatsRecorder hiddenFileColStatRecorder,
            List<PixelsProto.RowGroupInformation.Builder> rowGroupInfoBuilderList,
            List<PixelsProto.RowGroupStatistic.Builder> rowGroupStatBuilderList,
            List<PixelsProto.RowGroupFooter.Builder> rowGroupFooterBuilderList,
            List<String> rowGroupPaths)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.hasHiddenColumn = hasHiddenColumn;
        this.compactLayout = requireNonNull(compactLayout, "compactLayout is null");
        checkArgument(pixelStride > 0, "pixelStride is not positive");
        this.pixelStride = pixelStride;
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        checkArgument(compressionBlockSize > 0, "compression block size is not positive");
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.chunkAlignment = Integer.parseInt(ConfigFactory.Instance().getProperty("column.chunk.alignment"));
        checkArgument(this.chunkAlignment >= 0, "column.chunk.alignment must >= 0");
        this.chunkPaddingBuffer = new byte[this.chunkAlignment];
        checkArgument(fileContentLength > 0, "file content length is not positive");
        this.fileContentLength = fileContentLength;
        checkArgument(fileRowNum > 0, "file row number is not positive");
        this.fileRowNum = fileRowNum;

        this.inputStorage = requireNonNull(inputStorage, "input storage is null");
        this.fsWriter = requireNonNull(fsWriter, "fsWriter is null");

        this.fileColStatRecorders = requireNonNull(fileColStatRecorders, "file column stat reader is null");

        checkArgument(!requireNonNull(rowGroupInfoBuilderList, "rowGroupInfoBuilderList is null").isEmpty(),
                "rowGroupInfoBuilderList is empty");
        checkArgument(!requireNonNull(rowGroupStatBuilderList, "rowGroupStatBuilderList is null").isEmpty(),
                "rowGroupStatBuilderList is empty");
        checkArgument(!requireNonNull(rowGroupFooterBuilderList, "rowGroupFooterBuilderList is null").isEmpty(),
                "rowGroupFooterBuilderList is empty");
        checkArgument(!requireNonNull(rowGroupPaths, "rowGroupPaths is null").isEmpty(),
                "rowGroupPaths is empty");
        checkArgument(rowGroupInfoBuilderList.size() == rowGroupStatBuilderList.size() &&
                rowGroupStatBuilderList.size() == rowGroupFooterBuilderList.size() &&
                rowGroupFooterBuilderList.size() == rowGroupPaths.size(),
                "lengths of the row group lists are not consistent");
        this.rowGroupInfoBuilderList = ImmutableList.copyOf(rowGroupInfoBuilderList);
        this.rowGroupStatBuilderList = ImmutableList.copyOf(rowGroupStatBuilderList);
        if (hasHiddenColumn)
        {
            this.hiddenFileColStatRecorder = requireNonNull(hiddenFileColStatRecorder,
                    "no file level hidden timestamp column statistics");
        }
        else
        {
            this.hiddenFileColStatRecorder = null;
        }
        this.rowGroupFooterBuilderList = ImmutableList.copyOf(rowGroupFooterBuilderList);
        this.rowGroupPaths = ImmutableList.copyOf(rowGroupPaths);
    }

    /**
     * @return the number of row groups to be compact in this compactor
     */
    public int getNumRowGroup()
    {
        return this.rowGroupInfoBuilderList.size();
    }

    public static class Builder
    {
        private TypeDescription builderSchema = null;
        private boolean builderHasHiddenColumn = false; // whether the compact file should have hidden timestamp column
        private List<String> builderSourcePaths = null;
        private CompactLayout builderCompactLayout = null;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private Storage builderInputStorage = null;
        private Storage builderOutputStorage = null;
        private String builderFilePath = null;
        private StatsRecorder[] builderFileColStatRecorders;
        private StatsRecorder builderHiddenFileColStatRecorder = null; // hidden column stats recorder
        private long builderBlockSize = DEFAULT_HDFS_BLOCK_SIZE;
        private short builderReplication = 3;
        private boolean builderBlockPadding = true;
        private boolean builderOverwrite = false;
        private PixelsProto.CompressionKind builderCompressionKind = null;
        private int builderCompressionBlockSize = 1;
        private int builderPixelStride = 0;
        private long builderFileContentLength = 0L;
        private int builderFileRowNum = 0;
        private PhysicalWriter builderFsWriter = null;
        private final List<PixelsProto.RowGroupInformation.Builder> builderRowGroupInfoBuilderList = new LinkedList<>();
        private final List<PixelsProto.RowGroupStatistic.Builder> builderRowGroupStatBuilderList = new LinkedList<>();
        //private List<PixelsProto.ColumnStatistic.Builder> builderHiddenRowGroupStatBuilderList = null; // hidden column statistic if hasHiddenColumn
        private final List<PixelsProto.RowGroupFooter.Builder> builderRowGroupFooterBuilderList = new LinkedList<>();
        private final List<String> builderRowGroupPaths = new LinkedList<>();

        private Builder() { }

        /**
         * set schema is optional, if schema is not set, the schema read from the first source file will be used as this.schema.
         * and this.schema will be used as the schema of the compacted file.
         * @param schema the optional schema
         * @return the compactor itself
         */
        public PixelsCompactor.Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = schema;
            return this;
        }

        public PixelsCompactor.Builder setHasHiddenColumn(boolean hasHiddenColumn)
        {
            this.builderHasHiddenColumn = hasHiddenColumn;
            return this;
        }

        public PixelsCompactor.Builder setSourcePaths(List<String> sourcePaths)
        {
            this.builderSourcePaths = ImmutableList.copyOf(requireNonNull(sourcePaths));
            return this;
        }

        public PixelsCompactor.Builder setCompactLayout(CompactLayout compactLayout)
        {
            this.builderCompactLayout = requireNonNull(compactLayout);
            return this;
        }

        public PixelsCompactor.Builder setInputStorage(Storage inputStorage)
        {
            this.builderInputStorage = requireNonNull(inputStorage);
            return this;
        }

        public PixelsCompactor.Builder setOutputStorage(Storage outputStorage)
        {
            this.builderOutputStorage = requireNonNull(outputStorage);
            return this;
        }

        public PixelsCompactor.Builder setPath(String filePath)
        {
            this.builderFilePath = requireNonNull(filePath);
            return this;
        }

        public PixelsCompactor.Builder setTimeZone(TimeZone timeZone)
        {
            this.builderTimeZone = requireNonNull(timeZone);
            return this;
        }

        public PixelsCompactor.Builder setBlockSize(long blockSize)
        {
            this.builderBlockSize = blockSize;
            return this;
        }

        public PixelsCompactor.Builder setReplication(short replication)
        {
            this.builderReplication = replication;
            return this;
        }

        public PixelsCompactor.Builder setBlockPadding(boolean blockPadding)
        {
            this.builderBlockPadding = blockPadding;
            return this;
        }

        public Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;
            return this;
        }

        public PixelsCompactor build() throws IOException
        {
            // check arguments
            if (builderSourcePaths == null || builderCompactLayout == null || builderInputStorage == null ||
                    builderOutputStorage == null || builderFilePath == null)
            {
                throw new IllegalArgumentException("Missing argument(s) to build PixelsCompactor");
            }

            // read each source file footer
            for (int i = 0; i < builderSourcePaths.size(); i++)
            {
                String path = builderSourcePaths.get(i);
                PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(builderInputStorage, path);
                if (fsReader == null)
                {
                    throw new IOException("Read file failed");
                }

                // get FileTail
                long fileLen = fsReader.getFileLength();
                fsReader.seek(fileLen - Long.BYTES);
                long fileTailOffset = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
                fsReader.seek(fileTailOffset);
                byte[] fileTailBuffer = new byte[fileTailLength];
                fsReader.readFully(fileTailBuffer);
                PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);

                if (fileTail == null)
                {
                    fsReader.close();
                    throw new IOException("Read file tail failed");
                }

                // check file MAGIC and file version
                PixelsProto.PostScript postScript = fileTail.getPostscript();
                int fileVersion = postScript.getVersion();
                String fileMagic = postScript.getMagic();
                boolean hasHiddenColumn = postScript.getHasHiddenColumn();
                if (!PixelsVersion.matchVersion(fileVersion))
                {
                    fsReader.close();
                    throw new PixelsFileVersionInvalidException(fileVersion);
                }
                if (!fileMagic.contentEquals(Constants.FILE_MAGIC))
                {
                    fsReader.close();
                    throw new PixelsFileMagicInvalidException(fileMagic);
                }
                if (builderHasHiddenColumn && !hasHiddenColumn)
                {
                    fsReader.close();
                    throw new PixelsReaderException("File has no readable hidden timestamp columns to compact");
                }

                if (i == 0)
                {
                    builderCompressionKind = fileTail.getPostscript().getCompression();
                    builderCompressionBlockSize = fileTail.getPostscript().getCompressionBlockSize();
                    builderPixelStride = fileTail.getPostscript().getPixelStride();

                    if (builderSchema == null)
                    {
                        builderSchema = TypeDescription.createSchema(fileTail.getFooter().getTypesList());
                    }

                    List<TypeDescription> childrenSchema = builderSchema.getChildren();
                    checkArgument(!requireNonNull(childrenSchema).isEmpty(), "schema is empty");
                    builderFileColStatRecorders = new StatsRecorder[childrenSchema.size()];
                    for (int j = 0; j < childrenSchema.size(); ++j)
                    {
                        this.builderFileColStatRecorders[j] = StatsRecorder
                                .create(childrenSchema.get(j)); // to be updated when compacting
                    }

                    if (builderHasHiddenColumn)
                    {
                        builderHiddenFileColStatRecorder = StatsRecorder.create(TypeDescription.HIDDEN_COLUMN_TYPE);
                    }
                }

                builderFileContentLength += postScript.getContentLength(); // init fileContentLength
                builderFileRowNum += postScript.getNumberOfRows(); // init fileRowNum

                PixelsProto.Footer footer = fileTail.getFooter();
                // init rowGroupStatisticList
                for (PixelsProto.RowGroupStatistic stat : footer.getRowGroupStatsList())
                {

                    PixelsProto.RowGroupStatistic.Builder statBuilder = stat.toBuilder();
                    if (builderHasHiddenColumn)
                    {
                        checkArgument(statBuilder.hasHiddenColumnChunkStats(),
                                "row group does not have statistics for hidden column chunk");
                    }
                    else if (statBuilder.hasHiddenColumnChunkStats())
                    {
                        statBuilder.clearHiddenColumnChunkStats();
                    }
                    builderRowGroupStatBuilderList.add(statBuilder);
                }

                for (PixelsProto.RowGroupInformation info : footer.getRowGroupInfosList())
                {
                    builderRowGroupInfoBuilderList.add(info.toBuilder()); // footerOffset to be updated when compacting
                    long footerOffset = info.getFooterOffset();
                    long footerLength = info.getFooterLength();
                    fsReader.seek(footerOffset);
                    byte[] footerBuffer = new byte[(int) footerLength];
                    fsReader.readFully(footerBuffer);
                    PixelsProto.RowGroupFooter rowGroupFooter =
                            PixelsProto.RowGroupFooter.parseFrom(footerBuffer);
                    builderRowGroupFooterBuilderList
                            .add(rowGroupFooter.toBuilder()); // chunkOffset to be updated when compacting
                    builderRowGroupPaths.add(path);
                }
                fsReader.close();
            }

            builderFsWriter = PhysicalWriterUtil.newPhysicalWriter(builderOutputStorage, builderFilePath,
                    builderBlockSize, builderReplication, builderBlockPadding, builderOverwrite);

            return new PixelsCompactor(
                    builderSchema,
                    builderHasHiddenColumn,
                    builderCompactLayout,
                    builderPixelStride,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    builderFileContentLength,
                    builderFileRowNum,
                    builderInputStorage,
                    builderFsWriter,
                    builderFileColStatRecorders,
                    builderHiddenFileColStatRecorder,
                    builderRowGroupInfoBuilderList,
                    builderRowGroupStatBuilderList,
                    builderRowGroupFooterBuilderList,
                    builderRowGroupPaths);
        }
    }

    public static PixelsCompactor.Builder newBuilder()
    {
        return new PixelsCompactor.Builder();
    }

    public void compact()
    {
        this.writeColumnChunks();
        this.writeRowGroupFooters();
        this.writeFileTail();
    }

    private void writeColumnChunks()
    {
        // write common column chunks
        for (int i = 0; i < this.compactLayout.size(); ++i)
        {
            ColumnChunkIndex index = this.compactLayout.get(i);
            int rowGroupId = index.getRowGroupId();
            int columnId = index.getColumnId();
            PixelsProto.ColumnChunkIndex.Builder columnChunkIndexBuilder =
                    this.rowGroupFooterBuilderList.get(rowGroupId).getRowGroupIndexEntryBuilder()
                            .getColumnChunkIndexEntriesBuilder(columnId);
            long columnChunkOffset = columnChunkIndexBuilder.getChunkOffset();
            int columnChunkLength = columnChunkIndexBuilder.getChunkLength();
            String path = this.rowGroupPaths.get(rowGroupId);
            try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(inputStorage, path))
            {
                if (fsReader == null)
                {
                    throw new IOException("read file failed.");
                }
                fsReader.seek(columnChunkOffset);
                byte[] chunkBuffer = new byte[columnChunkLength];
                fsReader.readFully(chunkBuffer);

                // Issue #521: prepare for writing the column chunk, and make sure the start offset is aligned.
                long chunkStartOffset = fsWriter.prepare(columnChunkLength);
                int tryAlign = 0;
                while (chunkAlignment != 0 && chunkStartOffset % chunkAlignment != 0 && tryAlign++ < 2)
                {
                    int alignBytes = (int) (chunkAlignment - chunkStartOffset % chunkAlignment);
                    this.fsWriter.append(chunkPaddingBuffer, 0, alignBytes);
                    chunkStartOffset = this.fsWriter.prepare(columnChunkLength);
                }
                if (tryAlign > 2)
                {
                    LOGGER.warn("failed to align the start offset of the column chunk");
                    throw new IOException("failed to align the start offset of the column chunk");
                }

                this.fsWriter.append(chunkBuffer, 0, columnChunkLength);
                /*
                 * Issue #521:
                 * It is not necessary pad the column chunk here, as additional bytes are already padded before
                 * writing this column chunk to ensure chunkStartOffset is aligned. For the last column chunk,
                 * there is no need to ensure its length is aligned. We only need aligned start offsets.
                 *
                 * Also, there is no need to update the column chunk length, pixels reader needs the real length
                 * of the column chunk.
                 */
                columnChunkIndexBuilder.setChunkOffset(chunkStartOffset);
                // Issue #192: no need to flush fsWriter as writing has not finished.
            }
            catch (IOException e)
            {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
            }
        }
        // write hidden column chunks
        if (hasHiddenColumn)
        {
            /**
             * Warning: It's hard-coded here.
             * The chunks of the hidden column from different row groups are appended to the end of other column chunks.
             */
            for (int rowGroupId = 0; rowGroupId < this.rowGroupFooterBuilderList.size(); ++rowGroupId)
            {
                PixelsProto.ColumnChunkIndex.Builder hiddenColumnChunkIndexBuilder =
                        this.rowGroupFooterBuilderList.get(rowGroupId).getRowGroupIndexEntryBuilder()
                                .getHiddenColumnChunkIndexEntryBuilder();
                long hiddenColumnChunkOffset = hiddenColumnChunkIndexBuilder.getChunkOffset();
                int hiddenColumnChunkLength = hiddenColumnChunkIndexBuilder.getChunkLength();
                String path = this.rowGroupPaths.get(rowGroupId);
                try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(inputStorage, path))
                {
                    if (fsReader == null)
                    {
                        throw new IOException("read file failed.");
                    }
                    fsReader.seek(hiddenColumnChunkOffset);
                    byte[] hiddenChunkBuffer = new byte[hiddenColumnChunkLength];
                    fsReader.readFully(hiddenChunkBuffer);

                    long hiddenChunkStartOffset = fsWriter.prepare(hiddenColumnChunkLength);
                    int tryAlign = 0;
                    while (chunkAlignment != 0 && hiddenChunkStartOffset % chunkAlignment != 0 && tryAlign++ < 2)
                    {
                        int alignBytes = (int) (chunkAlignment - hiddenChunkStartOffset % chunkAlignment);
                        this.fsWriter.append(chunkPaddingBuffer, 0, alignBytes);
                        hiddenChunkStartOffset = this.fsWriter.prepare(hiddenColumnChunkLength);
                    }
                    if (tryAlign > 2)
                    {
                        LOGGER.warn("failed to align the start offset of the hidden column chunk");
                        throw new IOException("failed to align the start offset of the hidden column chunk");
                    }

                    this.fsWriter.append(hiddenChunkBuffer, 0, hiddenColumnChunkLength);
                    hiddenColumnChunkIndexBuilder.setChunkOffset(hiddenChunkStartOffset);
                }
                catch (IOException e)
                {
                    LOGGER.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private void writeRowGroupFooters()
    {
        for (int i = 0; i < this.rowGroupFooterBuilderList.size(); ++i)
        {
            PixelsProto.RowGroupFooter rowGroupFooter = this.rowGroupFooterBuilderList.get(i).build();
            ByteBuffer rowGroupFooterBuffer = ByteBuffer.allocate(rowGroupFooter.getSerializedSize());
            rowGroupFooterBuffer.put(rowGroupFooter.toByteArray());
            try
            {
                fsWriter.prepare(rowGroupFooter.getSerializedSize());
                long rowGroupFooterOffset = fsWriter.append(rowGroupFooterBuffer);
                // fsWriter.flush(); // Issue #192: no need to flush as writing has not finished.
                this.rowGroupInfoBuilderList.get(i).setFooterOffset(rowGroupFooterOffset);
                this.rowGroupInfoBuilderList.get(i).setFooterLength(rowGroupFooter.getSerializedSize());
            }
            catch (IOException e)
            {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
                return;
            }

            List<PixelsProto.ColumnStatistic> columnChunkStats =
                    this.rowGroupStatBuilderList.get(i).getColumnChunkStatsList();
            List<TypeDescription> children = this.schema.getChildren();
            checkArgument(!requireNonNull(children).isEmpty());
            for (int j = 0; j < children.size(); ++j)
            {
                fileColStatRecorders[j].merge(StatsRecorder.create(children.get(j), columnChunkStats.get(j)));
            }
            if (this.hasHiddenColumn)
            {
                PixelsProto.ColumnStatistic hiddenColumnChunkStat =
                        this.rowGroupStatBuilderList.get(i).getHiddenColumnChunkStats();
                hiddenFileColStatRecorder.merge(StatsRecorder.create(
                        TypeDescription.HIDDEN_COLUMN_TYPE, hiddenColumnChunkStat));
            }
        }
    }

    private void writeFileTail()
    {
        PixelsProto.Footer footer = buildFileFooter();
        PixelsProto.PostScript postScript = buildPostScript();

        PixelsProto.FileTail fileTail =
                PixelsProto.FileTail.newBuilder()
                        .setFooter(footer)
                        .setPostscript(postScript)
                        .setFooterLength(footer.getSerializedSize())
                        .setPostscriptLength(postScript.getSerializedSize())
                        .build();

        try
        {
            // write and flush FileTail plus FileTail physical offset at the end of the file
            int fileTailLen = fileTail.getSerializedSize() + Long.BYTES;
            fsWriter.prepare(fileTailLen);
            long tailOffset = fsWriter.append(fileTail.toByteArray(), 0, fileTail.getSerializedSize());
            ByteBuffer tailOffsetBuffer = ByteBuffer.allocate(Long.BYTES);
            tailOffsetBuffer.putLong(tailOffset);
            fsWriter.append(tailOffsetBuffer);
            fsWriter.flush();
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            System.out.println("Error writing file tail out.");
            e.printStackTrace();
        }
    }

    private PixelsProto.Footer buildFileFooter()
    {
        PixelsProto.Footer.Builder footerBuilder =
                PixelsProto.Footer.newBuilder();
        TypeDescription.writeTypes(footerBuilder, schema);
        for (StatsRecorder recorder : fileColStatRecorders)
        {
            footerBuilder.addColumnStats(recorder.serialize().build());
        }
        for (PixelsProto.RowGroupInformation.Builder rowGroupInformationBuilder : rowGroupInfoBuilderList)
        {
            footerBuilder.addRowGroupInfos(rowGroupInformationBuilder.build());
        }
        for (PixelsProto.RowGroupStatistic.Builder rowGroupStatisticBuilder : rowGroupStatBuilderList)
        {
            footerBuilder.addRowGroupStats(rowGroupStatisticBuilder.build());
        }
        if (hasHiddenColumn)
        {
            footerBuilder.setHiddenType(PixelsProto.Type.newBuilder()
                    .setName(TypeDescription.HIDDEN_COLUMN_NAME)
                    .setKind(PixelsProto.Type.Kind.LONG));
            footerBuilder.setHiddenColumnStats(hiddenFileColStatRecorder.serialize().build());
        }

        return footerBuilder.build();
    }

    private PixelsProto.PostScript buildPostScript()
    {
        return PixelsProto.PostScript.newBuilder()
                .setVersion(PixelsVersion.currentVersion().getVersion())
                .setContentLength(fileContentLength)
                .setNumberOfRows(fileRowNum)
                .setCompression(compressionKind)
                .setCompressionBlockSize(compressionBlockSize)
                .setPixelStride(pixelStride)
                .setWriterTimezone(timeZone.getDisplayName())
                .setPartitioned(false) // Issue #521: we do not compact partitioned files.
                .setColumnChunkAlignment(chunkAlignment)
                .setHasHiddenColumn(hasHiddenColumn)
                .setMagic(Constants.FILE_MAGIC)
                .build();
    }

    /**
     * Close PixelsCompactor, indicating the end of file
     */
    public void close()
    {
        try
        {
            fsWriter.close();
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            System.out.println("Error writing file tail out.");
            e.printStackTrace();
        }
    }
}
