package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.PixelsProto.CompressionKind;
import cn.edu.ruc.iir.pixels.core.PixelsProto.RowGroupInformation;
import cn.edu.ruc.iir.pixels.core.PixelsProto.RowGroupStatistic;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.core.writer.BinaryColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.BooleanColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.ByteColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.CharColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.ColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.DoubleColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.FloatColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.IntegerColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.StringColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.TimestampColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.VarcharColumnWriter;
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
 * Pixels file writer
 *
 * This writer is NOT thread safe!
 *
 * @author guodong
 */
public class PixelsWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsWriter.class);

    private final TypeDescription schema;
    private final int pixelStride;
    private final int rowGroupSize;
    private final CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    private final FileSystem fs;
    private final Path filePath;
    private final long blockSize;
    private final short replication;
    private final boolean blockPadding;
    private final boolean encoding;

    private final ColumnWriter[] columnWriters;
    private final StatsRecorder[] fileColStatRecorders;
    private long fileContentLength;
    private long fileRowNum;

    private boolean isNewRowGroup = true;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;

    private final List<RowGroupInformation> rowGroupInfoList;    // row group information in footer
    private final List<RowGroupStatistic> rowGroupStatisticList; // row group statistic in footer

    private PhysicalWriter physicalWriter;

    private PixelsWriter(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            FileSystem fs,
            Path filePath,
            long blockSize,
            short replication,
            boolean blockPadding,
            boolean encoding)
    {
        this.schema = schema;
        this.pixelStride = pixelStride;
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = compressionKind;
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = timeZone;
        this.fs = fs;
        this.filePath = filePath;
        this.blockSize = blockSize;
        this.replication = replication;
        this.blockPadding = blockPadding;
        this.encoding = encoding;

        List<TypeDescription> children = schema.getChildren();
        assert children != null;
        this.columnWriters = new ColumnWriter[children.size()];
        fileColStatRecorders = new StatsRecorder[children.size()];
        for (int i = 0; i < children.size(); ++i)
        {
            columnWriters[i] = createColumnWriter(children.get(i), encoding);
            fileColStatRecorders[i] = StatsRecorder.create(children.get(i));
        }

        this.rowGroupInfoList = new LinkedList<>();
        this.rowGroupStatisticList = new LinkedList<>();

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
        private TypeDescription builderSchema;
        private int builderPixelStride;
        private int builderRowGroupSize;  // group size in MB
        private CompressionKind builderCompressionKind = CompressionKind.NONE;
        private int builderCompressionBlockSize = 0;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private FileSystem builderFS;
        private Path builderFilePath;
        private long builderBlockSize;
        private short builderReplication = 3;
        private boolean builderBlockPadding = true;
        private boolean encoding = true;

        public Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = schema;

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
            this.builderCompressionKind = compressionKind;

            return this;
        }

        public Builder setCompressionBlockSize(int compressionBlockSize)
        {
            this.builderCompressionBlockSize = compressionBlockSize;

            return this;
        }

        public Builder setTimeZone(TimeZone timeZone)
        {
            this.builderTimeZone = timeZone;

            return this;
        }

        public Builder setFS(FileSystem fs)
        {
            this.builderFS = fs;

            return this;
        }

        public Builder setFilePath(Path filePath)
        {
            this.builderFilePath = filePath;

            return this;
        }

        public Builder setBlockSize(long blockSize)
        {
            this.builderBlockSize = blockSize;

            return this;
        }

        public Builder setReplication(short replication)
        {
            this.builderReplication = replication;

            return this;
        }

        public Builder setBlockPadding(boolean blockPadding)
        {
            this.builderBlockPadding = blockPadding;

            return this;
        }

        public Builder setEncoding(boolean encoding)
        {
            this.encoding = encoding;

            return this;
        }

        public PixelsWriter build()
        {
            return new PixelsWriter(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    builderFS,
                    builderFilePath,
                    builderBlockSize,
                    builderReplication,
                    builderBlockPadding,
                    encoding);
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

    public boolean isEncoding()
    {
        return encoding;
    }

    public ColumnWriter[] getColumnWriters()
    {
        return columnWriters;
    }

    public void addRowBatch(VectorizedRowBatch rowBatch) throws IOException
    {
        if (isNewRowGroup) {
            this.isNewRowGroup = false;
            this.curRowGroupNumOfRows = 0L;
        }
        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        ColumnVector[] cvs = rowBatch.cols;
        for (int i = 0; i < cvs.length; i++)
        {
            ColumnWriter writer = columnWriters[i];
            curRowGroupDataLength += writer.writeBatch(cvs[i], rowBatch.size);
        }
        // see if current size has exceeded the row group size. if so, write out current row group
        if (curRowGroupDataLength >= rowGroupSize) {
            writeRowGroup();
            curRowGroupDataLength = 0;
        }
    }

    /**
     * Close PixelsWriter, indicating the end of file
     * */
    public void close()
    {
        try
        {
            if (curRowGroupDataLength != 0) {
                writeRowGroup();
            }
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

    private void writeRowGroup()
    {
        this.isNewRowGroup = true;
        int rowGroupDataLength = 0;

        PixelsProto.RowGroupStatistic.Builder curRowGroupStatistic =
                PixelsProto.RowGroupStatistic.newBuilder();
        PixelsProto.RowGroupInformation.Builder curRowGroupInfo =
                PixelsProto.RowGroupInformation.newBuilder();
        PixelsProto.RowGroupIndex.Builder curRowGroupIndex =
                PixelsProto.RowGroupIndex.newBuilder();

        // reset each column writer and get current row group content size in bytes
        for (ColumnWriter writer : columnWriters)
        {
            // new chunk for each writer
            writer.newChunk();
            rowGroupDataLength += writer.getColumnChunkSize();
        }

        // write and flush row group content
        try {
            curRowGroupOffset = physicalWriter.prepare(rowGroupDataLength);
            if (curRowGroupOffset != -1) {
                for (ColumnWriter writer : columnWriters)
                {
                    byte[] rowGroupBuffer = writer.getColumnChunkContent();
                    physicalWriter.append(rowGroupBuffer, 0, rowGroupBuffer.length);
                }
                physicalWriter.flush();
            }
            else {
                LOGGER.warn("Write row group prepare failed");
            }
        }
        catch (IOException e) {
            LOGGER.error(e.getMessage());
            System.exit(-1);
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
            // update file column statistic
            fileColStatRecorders[i].merge(writer.getColumnChunkStatRecorder());
            // call children writer reset()
            writer.reset();
        }

        // put curRowGroupIndex into rowGroupFooter
        PixelsProto.RowGroupFooter rowGroupFooter =
                PixelsProto.RowGroupFooter.newBuilder()
                        .setRowGroupIndexEntry(curRowGroupIndex.build())
                        .build();

        // write and flush row group footer
        try {
            byte[] footerBuffer = rowGroupFooter.toByteArray();
            physicalWriter.prepare(footerBuffer.length);
            curRowGroupFooterOffset = physicalWriter.append(footerBuffer, 0, footerBuffer.length);
            physicalWriter.flush();
        }
        catch (IOException e) {
            LOGGER.error(e.getMessage());
            System.exit(-1);
        }

        // update RowGroupInformation, and put it into rowGroupInfoList
        curRowGroupInfo.setFooterOffset(curRowGroupFooterOffset);
        curRowGroupInfo.setDataLength(rowGroupDataLength);
        curRowGroupInfo.setFooterLength(rowGroupFooter.getSerializedSize());
        curRowGroupInfo.setNumberOfRows(curRowGroupNumOfRows);
        rowGroupInfoList.add(curRowGroupInfo.build());
        // put curRowGroupStatistic into rowGroupStatisticList
        rowGroupStatisticList.add(curRowGroupStatistic.build());

        this.fileRowNum += curRowGroupNumOfRows;
        this.fileContentLength += rowGroupDataLength;
    }

    private void writeFileTail() throws IOException
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
        physicalWriter.flush();
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

    private ColumnWriter createColumnWriter(TypeDescription schema, boolean isEncoding)
    {
        switch (schema.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnWriter(schema, pixelStride);
            case BYTE:
                return new ByteColumnWriter(schema, pixelStride);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnWriter(schema, pixelStride, isEncoding);
            case FLOAT:
                return new FloatColumnWriter(schema, pixelStride);
            case DOUBLE:
                return new DoubleColumnWriter(schema, pixelStride);
            case STRING:
                return new StringColumnWriter(schema, pixelStride);
            case CHAR:
                return new CharColumnWriter(schema, pixelStride);
            case VARCHAR:
                return new VarcharColumnWriter(schema, pixelStride);
            case BINARY:
                return new BinaryColumnWriter(schema, pixelStride);
            case TIMESTAMP:
                return new TimestampColumnWriter(schema, pixelStride);
            default:
                throw new IllegalArgumentException("Bad schema type: " + schema.getCategory());
        }
    }
}
