package cn.edu.ruc.iir.pixels.core.compactor;

import cn.edu.ruc.iir.pixels.core.*;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * Pixels file compactor
 *
 * @author haoqiong
 */
public class PixelsCompactor
{
    // todo applying refactor in PixelsCompactor

<<<<<<< HEAD
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsWriterImpl.class);
=======
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsWriter.class);
>>>>>>> a98e0d3c466b228a9347cb0fa2fc3f1b03cce52e

    private final TypeDescription schema;
    private final int pixelStride;
    private final PixelsProto.CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;

    private final List<Path> sourcePaths;
    private final CompactLayout compactLayout;
    private final FileSystem fs;
    private final Path filePath;
    private final long blockSize;
    private final short replication;
    private final boolean blockPadding;

    private final StatsRecorder[] fileColStatRecorders;
    private long fileContentLength;
    private long fileRowNum;

    private final List<PixelsProto.RowGroupInformation.Builder> rowGroupInfoBuilderList;    // row group information in footer
    private final List<PixelsProto.RowGroupStatistic.Builder> rowGroupStatBuilderList; // row group statistic in footer
    private final List<PixelsProto.RowGroupFooter.Builder> rowGroupFooterBuilderList; // row group fotters
    private final List<Path> rowGroupFilePathList;

    private PhysicalWriter physicalWriter;

    private PixelsCompactor(
            TypeDescription schema,
            TimeZone timeZone,
            List<Path> sourcePaths,
            CompactLayout compactLayout,
            FileSystem fs,
            Path filePath,
            long blockSize,
            short replication,
            boolean blockPadding)
    {
        this.schema = schema;
        this.sourcePaths = sourcePaths;
        this.compactLayout = compactLayout;
        this.fs = fs;
        this.filePath = filePath;
        this.blockSize = blockSize;
        this.replication = replication;
        this.blockPadding = blockPadding;

        List<TypeDescription> children = schema.getChildren();
        assert children != null;
        this.fileColStatRecorders = new StatsRecorder[children.size()];
        for (int i = 0; i < children.size(); ++i)
        {
            this.fileColStatRecorders[i] = StatsRecorder.create(children.get(i)); // to be updated when compacting
        }

        this.rowGroupInfoBuilderList = new LinkedList<>();
        this.rowGroupStatBuilderList = new LinkedList<>();

        this.timeZone = timeZone;

        // init compressionKind, compressionBlockSize and pixelStride
        Path path0 = this.sourcePaths.get(0);
        PixelsProto.FileTail fileTail0 = null;

        try (FSDataInputStream in0 = fs.open(path0))
        {
            long fileLength = fs.getFileStatus(path0).getLen();
            in0.seek(fileLength - 8);
            long pos = in0.readLong();
            in0.seek(pos - 4);
            int tailLength = in0.readInt();
            long tailOffset = fileLength - 8 - 4 - tailLength;
            in0.seek(tailOffset);
            byte[] tailBuffer = new byte[tailLength];
            in0.readFully(tailBuffer);

            fileTail0 =
                    PixelsProto.FileTail.parseFrom(tailBuffer);

            if (fileTail0 == null)
            {
                throw new IOException("read file tail failed.");
            }
        } catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        this.compressionKind = fileTail0.getPostscript().getCompression();
        this.compressionBlockSize = (int)fileTail0.getPostscript().getCompressionBlockSize();
        this.pixelStride = fileTail0.getPostscript().getPixelStride();

        this.fileContentLength = 0;
        this.fileRowNum = 0;
        this.rowGroupFooterBuilderList = new LinkedList<>();
        this.rowGroupFilePathList = new LinkedList<>();
        for (Path path : this.sourcePaths)
        {
            try (FSDataInputStream in = fs.open(path))
            {
                long fileLength = fs.getFileStatus(path).getLen();
                in.seek(fileLength - 8);
                long pos = in.readLong();
                in.seek(pos - 4);
                int tailLength = in.readInt();
                long tailOffset = fileLength - 8 - 4 - tailLength;
                in.seek(tailOffset);
                byte[] tailBuffer = new byte[tailLength];
                in.readFully(tailBuffer);

                PixelsProto.FileTail fileTail =
                        PixelsProto.FileTail.parseFrom(tailBuffer);

                PixelsProto.PostScript postScript = fileTail.getPostscript();
                this.fileContentLength += postScript.getContentLength(); // init fileContentLength
                this.fileRowNum += postScript.getNumberOfRows(); // init fileRowNum

                PixelsProto.Footer footer = fileTail.getFooter();
                for (PixelsProto.RowGroupStatistic stat : footer.getRowGroupStatsList())
                {
                    this.rowGroupStatBuilderList.add(stat.toBuilder());// init rowGroupStatisticList
                }
                for (PixelsProto.RowGroupInformation info : footer.getRowGroupInfosList())
                {
                    this.rowGroupInfoBuilderList.add(info.toBuilder()); // footerOffset to be updated when compacting
                    long footerOffset = info.getFooterOffset();
                    long footerLength = info.getFooterLength();
                    in.seek(footerOffset);
                    byte[] footerBuffer = new byte[(int)footerLength];
                    in.readFully(footerBuffer);
                    PixelsProto.RowGroupFooter rowGroupFooter =
                            PixelsProto.RowGroupFooter.parseFrom(footerBuffer);
                    this.rowGroupFooterBuilderList.add(rowGroupFooter.toBuilder()); // chunkOffset to be updated when compacting
                    this.rowGroupFilePathList.add(path);
                }

            } catch (IOException e)
            {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
                System.exit(-1);
            }
        }

        try
        {
            this.physicalWriter = new PhysicalFSWriter(fs, filePath, blockSize, replication, blockPadding);
        } catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static class Builder
    {
        private TypeDescription schema;
        private List<Path> sourcePaths;
        private CompactLayout compactLayout;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private FileSystem builderFS;
        private Path builderFilePath;
        private long builderBlockSize;
        private short builderReplication = 3;
        private boolean builderBlockPadding = true;

        public PixelsCompactor.Builder setSchema (TypeDescription schema)
        {
            this.schema = schema;

            return this;
        }

        public PixelsCompactor.Builder setSourcePaths (List<Path> sourcePaths)
        {
            this.sourcePaths = sourcePaths;

            return this;
        }

        public PixelsCompactor.Builder setCompactLayout (CompactLayout compactLayout)
        {
            this.compactLayout = compactLayout;

            return this;
        }

        public PixelsCompactor.Builder setFS(FileSystem fs)
        {
            this.builderFS = fs;

            return this;
        }

        public PixelsCompactor.Builder setFilePath(Path filePath)
        {
            this.builderFilePath = filePath;

            return this;
        }

        public PixelsCompactor.Builder setTimeZone(TimeZone timeZone)
        {
            this.builderTimeZone = timeZone;

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

        public PixelsCompactor build()
        {
            return new PixelsCompactor(
                    schema,
                    builderTimeZone,
                    sourcePaths,
                    compactLayout,
                    builderFS,
                    builderFilePath,
                    builderBlockSize,
                    builderReplication,
                    builderBlockPadding);
        }
    }

    public static PixelsCompactor.Builder newBuilder()
    {
        return new PixelsCompactor.Builder();
    }

    public FileSystem getFs()
    {
        return fs;
    }

    public Path getFilePath()
    {
        return filePath;
    }

    public long getBlockSize()
    {
        return blockSize;
    }

    public short getReplication()
    {
        return replication;
    }

    public boolean isBlockPadding()
    {
        return blockPadding;
    }

    public void compact ()
    {
        this.writeColumnChunks();
        this.writeRowGroupFooters();
    }

    private void writeColumnChunks ()
    {
        for (int i = 0; i < this.compactLayout.size(); ++i)
        {
            ColumnChunkInfo info = this.compactLayout.get(i);
            int rowGroupId = info.getRowGroupId();
            int columnId = info.getColumnId();
            PixelsProto.ColumnChunkIndex.Builder columnChunkIndexBuilder = this.rowGroupFooterBuilderList.get(rowGroupId).
                    getRowGroupIndexEntryBuilder().getColumnChunkIndexEntriesBuilder(columnId);
            long columnChunkOffset = columnChunkIndexBuilder.getChunkOffset();
            long columnChunkLength = columnChunkIndexBuilder.getChunkLength();
            Path path = this.rowGroupFilePathList.get(rowGroupId);
            try (FSDataInputStream in = fs.open(path))
            {
                in.seek(columnChunkOffset);
                ByteBuffer chunkBuffer = ByteBuffer.allocate((int)columnChunkLength);
                int readLength = 0;
                while (readLength < columnChunkLength)
                {
                    readLength += in.read(chunkBuffer);
                }
                long offset = this.physicalWriter.append(chunkBuffer);
                columnChunkIndexBuilder.setChunkOffset(offset);
                this.physicalWriter.flush();
            } catch (IOException e)
            {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    private void writeRowGroupFooters ()
    {
        for (int i = 0; i < this.rowGroupFooterBuilderList.size(); ++i)
        {
            PixelsProto.RowGroupFooter rowGroupFooter = this.rowGroupFooterBuilderList.get(i).build();
            ByteBuffer rowGroupFooterBuffer = ByteBuffer.allocate(rowGroupFooter.getSerializedSize());
            rowGroupFooterBuffer.put(rowGroupFooter.toByteArray());
            try
            {
                long rowGroupFooterOffset = physicalWriter.append(rowGroupFooterBuffer);
                physicalWriter.flush();
                this.rowGroupInfoBuilderList.get(i).setFooterOffset(rowGroupFooterOffset);
                this.rowGroupInfoBuilderList.get(i).setFooterLength(rowGroupFooter.getSerializedSize());
            } catch (IOException e)
            {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
                System.exit(-1);
            }

            List<PixelsProto.ColumnStatistic> columnChunkStats =
                    this.rowGroupStatBuilderList.get(i).getColumnChunkStatsList();
            List<TypeDescription> children = this.schema.getChildren();
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
            physicalWriter.close();
        } catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            System.out.println("Error writing file tail out.");
            e.printStackTrace();
            System.exit(-1);
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
//        physicalWriter.writeFileTail(fileTail);
    }

    private PixelsProto.Footer writeFooter()
    {
        PixelsProto.Footer.Builder footerBuilder =
                PixelsProto.Footer.newBuilder();
        writeTypes(footerBuilder, schema);
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

    private void writeTypes(PixelsProto.Footer.Builder builder, TypeDescription schema)
    {
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();
        assert children != null;
        for (int i = 0; i < children.size(); i++)
        {
            PixelsProto.Type.Builder tmpType = PixelsProto.Type.newBuilder();
            tmpType.setName(names.get(i));
            switch (children.get(i).getCategory())
            {
                case BOOLEAN:
                    tmpType.setKind(PixelsProto.Type.Kind.BOOLEAN);
                    break;
                case BYTE:
                    tmpType.setKind(PixelsProto.Type.Kind.BYTE);
                    break;
                case SHORT:
                    tmpType.setKind(PixelsProto.Type.Kind.SHORT);
                    break;
                case INT:
                    tmpType.setKind(PixelsProto.Type.Kind.INT);
                    break;
                case LONG:
                    tmpType.setKind(PixelsProto.Type.Kind.LONG);
                    break;
                case FLOAT:
                    tmpType.setKind(PixelsProto.Type.Kind.FLOAT);
                    break;
                case DOUBLE:
                    tmpType.setKind(PixelsProto.Type.Kind.DOUBLE);
                    break;
                case STRING:
                    tmpType.setKind(PixelsProto.Type.Kind.STRING);
                    tmpType.setMaximumLength(schema.getMaxLength());
                    break;
                case CHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.CHAR);
                    tmpType.setMaximumLength(schema.getMaxLength());
                    break;
                case VARCHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.VARCHAR);
                    tmpType.setMaximumLength(schema.getMaxLength());
                    break;
                case BINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.BINARY);
                    break;
                case TIMESTAMP:
                    tmpType.setKind(PixelsProto.Type.Kind.TIMESTAMP);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown category: " +
                            schema.getCategory());
            }
            builder.addTypes(tmpType.build());
        }
    }
}
