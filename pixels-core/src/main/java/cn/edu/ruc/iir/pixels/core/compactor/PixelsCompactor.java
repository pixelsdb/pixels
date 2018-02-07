package cn.edu.ruc.iir.pixels.core.compactor;

import cn.edu.ruc.iir.pixels.core.Constants;
import cn.edu.ruc.iir.pixels.core.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.core.PhysicalFSWriter;
import cn.edu.ruc.iir.pixels.core.PhysicalReaderUtil;
import cn.edu.ruc.iir.pixels.core.PhysicalWriterUtil;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Pixels file compactor
 *
 * @author haoqiong
 */
public class PixelsCompactor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsCompactor.class);

    private final TypeDescription schema;
    private final CompactLayout compactLayout;
    private final int pixelStride;
    private final PixelsProto.CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    private final long fileContentLength;
    private final long fileRowNum;

    private final FileSystem fs;
    private final PhysicalFSWriter fsWriter;
    private final StatsRecorder[] fileColStatRecorders;

    private final List<PixelsProto.RowGroupInformation.Builder> rowGroupInfoBuilderList;    // row group information in footer
    private final List<PixelsProto.RowGroupStatistic.Builder> rowGroupStatBuilderList; // row group metric in footer
    private final List<PixelsProto.RowGroupFooter.Builder> rowGroupFooterBuilderList; // row group fotters
    private final List<Path> rowGroupPaths;

    private PixelsCompactor(
            TypeDescription schema,
            CompactLayout compactLayout,
            int pixelStride,
            PixelsProto.CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            long fileContentLength,
            long fileRowNum,
            FileSystem fs,
            PhysicalFSWriter fsWriter,
            StatsRecorder[] fileColStatRecorders,
            List<PixelsProto.RowGroupInformation.Builder> rowGroupInfoBuilderList,
            List<PixelsProto.RowGroupStatistic.Builder> rowGroupStatBuilderList,
            List<PixelsProto.RowGroupFooter.Builder> rowGroupFooterBuilderList,
            List<Path> rowGroupPaths)
    {
        this.schema = requireNonNull(schema);
        this.compactLayout = requireNonNull(compactLayout);
        checkArgument(pixelStride > 0);
        this.pixelStride = pixelStride;
        this.compressionKind = requireNonNull(compressionKind);
        checkArgument(compressionBlockSize > 0);
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone);
        checkArgument(fileContentLength > 0);
        this.fileContentLength = fileContentLength;
        checkArgument(fileRowNum > 0);
        this.fileRowNum = fileRowNum;

        this.fs = requireNonNull(fs);
        this.fsWriter = requireNonNull(fsWriter);

        this.fileColStatRecorders = fileColStatRecorders;

        checkArgument(!requireNonNull(rowGroupFooterBuilderList).isEmpty());
        checkArgument(!requireNonNull(rowGroupStatBuilderList).isEmpty());
        checkArgument(!requireNonNull(rowGroupFooterBuilderList).isEmpty());
        checkArgument(!requireNonNull(rowGroupPaths).isEmpty());
        this.rowGroupInfoBuilderList = ImmutableList.copyOf(rowGroupInfoBuilderList);
        this.rowGroupStatBuilderList = ImmutableList.copyOf(rowGroupStatBuilderList);
        this.rowGroupFooterBuilderList = ImmutableList.copyOf(rowGroupFooterBuilderList);
        this.rowGroupPaths = ImmutableList.copyOf(rowGroupPaths);
    }

    public static class Builder
    {
        private TypeDescription schema = null;
        private List<Path> sourcePaths = null;
        private CompactLayout compactLayout = null;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private FileSystem builderFS = null;
        private Path builderFilePath = null;
        private StatsRecorder[] fileColStatRecorders;
        private long builderBlockSize = Constants.DEFAULT_HDFS_BLOCK_SIZE;
        private short builderReplication = 3;
        private boolean builderBlockPadding = true;
        private PixelsProto.CompressionKind compressionKind = null;
        private int compressionBlockSize = 0;
        private int pixelStride = 0;
        private long fileContentLength = 0L;
        private long fileRowNum = 0;
        private PhysicalFSWriter fsWriter = null;
        private List<PixelsProto.RowGroupInformation.Builder> rowGroupInfoBuilderList = new LinkedList<>();
        private List<PixelsProto.RowGroupStatistic.Builder> rowGroupStatBuilderList = new LinkedList<>();
        private List<PixelsProto.RowGroupFooter.Builder> rowGroupFooterBuilderList = new LinkedList<>();
        private List<Path> rowGroupPaths = new LinkedList<>();

        private Builder()
        {}

        public PixelsCompactor.Builder setSchema(TypeDescription schema)
        {
            this.schema = requireNonNull(schema);

            return this;
        }

        public PixelsCompactor.Builder setSourcePaths(List<Path> sourcePaths)
        {
            this.sourcePaths = ImmutableList.copyOf(requireNonNull(sourcePaths));

            return this;
        }

        public PixelsCompactor.Builder setCompactLayout(CompactLayout compactLayout)
        {
            this.compactLayout = requireNonNull(compactLayout);

            return this;
        }

        public PixelsCompactor.Builder setFS(FileSystem fs)
        {
            this.builderFS = requireNonNull(fs);

            return this;
        }

        public PixelsCompactor.Builder setFilePath(Path filePath)
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

        public PixelsCompactor build() throws IOException
        {
            // check arguments
            if (schema == null || sourcePaths == null || compactLayout == null || builderTimeZone == null
                    || builderFS == null || builderFilePath == null)
            {
                throw new IllegalArgumentException("Missing argument to build PixelsCompactor");
            }

            List<TypeDescription> childrenSchema = schema.getChildren();
            checkArgument(!requireNonNull(childrenSchema).isEmpty());
            fileColStatRecorders = new StatsRecorder[childrenSchema.size()];
            for (int i = 0; i < childrenSchema.size(); ++i)
            {
                this.fileColStatRecorders[i] = StatsRecorder.create(childrenSchema.get(i)); // to be updated when compacting
            }

            // read each source file footer
            for (int i = 0; i < sourcePaths.size(); i++)
            {
                Path path = sourcePaths.get(i);
                PhysicalFSReader fsReader = PhysicalReaderUtil.newPhysicalFSReader(builderFS, path);
                if (fsReader == null)
                {
                    throw new IOException("Read file failed.");
                }
                long fileLength = fsReader.getFileLength();
                fsReader.seek(fileLength - 8);
                long pos = fsReader.readLong();
                fsReader.seek(pos - 4);
                int tailLength = fsReader.readInt();
                long tailOffset = fileLength - 8 - 4 - tailLength;
                fsReader.seek(tailOffset);
                byte[] tailBuffer = new byte[tailLength];
                fsReader.readFully(tailBuffer);

                PixelsProto.FileTail fileTail =
                        PixelsProto.FileTail.parseFrom(tailBuffer);

                if (fileTail == null) {
                    throw new IOException("read file tail failed.");
                }

                if (i == 0) {
                    compressionKind = fileTail.getPostscript().getCompression();
                    compressionBlockSize = fileTail.getPostscript().getCompressionBlockSize();
                    pixelStride = fileTail.getPostscript().getPixelStride();
                }

                PixelsProto.PostScript postScript = fileTail.getPostscript();
                fileContentLength += postScript.getContentLength(); // init fileContentLength
                fileRowNum += postScript.getNumberOfRows(); // init fileRowNum

                PixelsProto.Footer footer = fileTail.getFooter();
                // init rowGroupStatisticList
                for (PixelsProto.RowGroupStatistic stat : footer.getRowGroupStatsList()) {
                    rowGroupStatBuilderList.add(stat.toBuilder());
                }
                for (PixelsProto.RowGroupInformation info : footer.getRowGroupInfosList())
                {
                    rowGroupInfoBuilderList.add(info.toBuilder()); // footerOffset to be updated when compacting
                    long footerOffset = info.getFooterOffset();
                    long footerLength = info.getFooterLength();
                    fsReader.seek(footerOffset);
                    byte[] footerBuffer = new byte[(int) footerLength];
                    fsReader.readFully(footerBuffer);
                    PixelsProto.RowGroupFooter rowGroupFooter =
                            PixelsProto.RowGroupFooter.parseFrom(footerBuffer);
                    rowGroupFooterBuilderList.add(rowGroupFooter.toBuilder()); // chunkOffset to be updated when compacting
                    rowGroupPaths.add(path);
                }
            }

            fsWriter = PhysicalWriterUtil.newPhysicalFSWriter(builderFS, builderFilePath, builderBlockSize,
                    builderReplication, builderBlockPadding);

            return new PixelsCompactor(
                    schema,
                    compactLayout,
                    pixelStride,
                    compressionKind,
                    compressionBlockSize,
                    builderTimeZone,
                    fileContentLength,
                    fileRowNum,
                    builderFS,
                    fsWriter,
                    fileColStatRecorders,
                    rowGroupInfoBuilderList,
                    rowGroupStatBuilderList,
                    rowGroupFooterBuilderList,
                    rowGroupPaths);
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
    }

    private void writeColumnChunks()
    {
        for (int i = 0; i < this.compactLayout.size(); ++i)
        {
            ColumnChunkInfo info = this.compactLayout.get(i);
            int rowGroupId = info.getRowGroupId();
            int columnId = info.getColumnId();
            PixelsProto.ColumnChunkIndex.Builder columnChunkIndexBuilder =
                    this.rowGroupFooterBuilderList.get(rowGroupId).getRowGroupIndexEntryBuilder()
                            .getColumnChunkIndexEntriesBuilder(columnId);
            long columnChunkOffset = columnChunkIndexBuilder.getChunkOffset();
            long columnChunkLength = columnChunkIndexBuilder.getChunkLength();
            Path path = this.rowGroupPaths.get(rowGroupId);
            try (PhysicalFSReader fsReader = PhysicalReaderUtil.newPhysicalFSReader(fs, path))
            {
                if (fsReader == null) {
                    throw new IOException("read file failed.");
                }
                fsReader.seek(columnChunkOffset);
                byte[] chunkBuffer = new byte[(int) columnChunkLength];
                int readLength = 0;
                while (readLength < columnChunkLength)
                {
                    readLength += fsReader.read(chunkBuffer);
                }
                fsWriter.prepare((int) columnChunkLength);
                long offset = this.fsWriter.append(chunkBuffer, 0, (int) columnChunkLength);
                columnChunkIndexBuilder.setChunkOffset(offset);
                this.fsWriter.flush();
            } catch (IOException e)
            {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
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
                long rowGroupFooterOffset = fsWriter.append(rowGroupFooterBuffer);
                fsWriter.flush();
                this.rowGroupInfoBuilderList.get(i).setFooterOffset(rowGroupFooterOffset);
                this.rowGroupInfoBuilderList.get(i).setFooterLength(rowGroupFooter.getSerializedSize());
            } catch (IOException e)
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
        }
    }

    /**
     * Close PixelsCompactor, indicating the end of file
     * */
    public void close()
    {
        try
        {
            writeFileTail();
            fsWriter.close();
        } catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            System.out.println("Error writing file tail out.");
            e.printStackTrace();
        }
    }

    private void writeFileTail() throws IOException
    {
        PixelsProto.Footer footer = writeFooter();
        PixelsProto.PostScript postScript = writePostScript();

        PixelsProto.FileTail fileTail =
                PixelsProto.FileTail.newBuilder()
                        .setFooter(footer)
                        .setPostscript(postScript)
                        .setFooterLength(footer.getSerializedSize())
                        .setPostscriptLength(postScript.getSerializedSize())
                        .build();
        int fileTailLen = fileTail.getSerializedSize() + Long.BYTES;
        fsWriter.prepare(fileTailLen);
        long tailOffset = fsWriter.append(fileTail.toByteArray(), 0, fileTail.getSerializedSize());
        ByteBuf tailOffsetBuf = Unpooled.buffer(Long.BYTES);
        tailOffsetBuf.writeLong(tailOffset);
        fsWriter.append(tailOffsetBuf.array(), 0, Long.BYTES);
        tailOffsetBuf.release();
        fsWriter.flush();
    }

    private PixelsProto.Footer writeFooter()
    {
        PixelsProto.Footer.Builder footerBuilder =
                PixelsProto.Footer.newBuilder();
        PixelsWriterImpl.writeTypes(footerBuilder, schema);
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

        return footerBuilder.build();
    }

    private PixelsProto.PostScript writePostScript()
    {
        return PixelsProto.PostScript.newBuilder()
                .setVersion(Constants.VERSION)
                .setContentLength(fileContentLength)
                .setNumberOfRows(fileRowNum)
                .setCompression(compressionKind)
                .setCompressionBlockSize(compressionBlockSize)
                .setPixelStride(pixelStride)
                .setWriterTimezone(timeZone.getDisplayName())
                .setMagic(Constants.MAGIC)
                .build();
    }
}
