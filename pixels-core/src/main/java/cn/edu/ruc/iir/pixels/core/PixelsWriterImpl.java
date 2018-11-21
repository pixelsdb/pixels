package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.common.physical.PhysicalWriter;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalWriterUtil;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.core.PixelsProto.CompressionKind;
import cn.edu.ruc.iir.pixels.core.PixelsProto.RowGroupInformation;
import cn.edu.ruc.iir.pixels.core.PixelsProto.RowGroupStatistic;
import cn.edu.ruc.iir.pixels.core.exception.PixelsWriterException;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Pixels file writer default implementation
 *
 * This writer is NOT thread safe!
 *
 * @author guodong
 */
@NotThreadSafe
public class PixelsWriterImpl
        extends PixelsWriter
{
    private static final Logger LOGGER = LogManager.getLogger(PixelsWriterImpl.class);

    private final TypeDescription schema;
    private final int pixelStride;
    private final int rowGroupSize;
    private final CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    private final boolean encoding;

    private final ColumnWriter[] columnWriters;
    private final StatsRecorder[] fileColStatRecorders;
    private long fileContentLength;
    private int fileRowNum;

    private boolean isNewRowGroup = true;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;

    private final List<RowGroupInformation> rowGroupInfoList;    // row group information in footer
    private final List<RowGroupStatistic> rowGroupStatisticList; // row group statistic in footer

    private final PhysicalWriter physicalWriter;

    private PixelsWriterImpl(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            PhysicalWriter physicalWriter,
            boolean encoding)
    {
        this.schema = requireNonNull(schema, "schema is null");
        checkArgument(pixelStride > 0, "pixel stripe is not positive");
        this.pixelStride = pixelStride;
        checkArgument(rowGroupSize > 0, "row group size is not positive");
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = requireNonNull(compressionKind);
        checkArgument(compressionBlockSize > 0, "compression block size is not positive");
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone);
        this.encoding = encoding;

        List<TypeDescription> children = schema.getChildren();
        checkArgument(!requireNonNull(children, "schema is null").isEmpty(), "schema is empty");
        this.columnWriters = new ColumnWriter[children.size()];
        fileColStatRecorders = new StatsRecorder[children.size()];
        for (int i = 0; i < children.size(); ++i)
        {
            columnWriters[i] = createColumnWriter(children.get(i), encoding);
            fileColStatRecorders[i] = StatsRecorder.create(children.get(i));
        }

        this.rowGroupInfoList = new LinkedList<>();
        this.rowGroupStatisticList = new LinkedList<>();

        this.physicalWriter = physicalWriter;
    }

    public static class Builder
    {
        private TypeDescription builderSchema;
        private int builderPixelStride;
        private int builderRowGroupSize;
        private CompressionKind builderCompressionKind = CompressionKind.NONE;
        private int builderCompressionBlockSize = 0;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private FileSystem builderFS;
        private Path builderFilePath;
        private long builderBlockSize;
        private short builderReplication = 3;
        private boolean builderBlockPadding = true;
        private boolean encoding = true;

        private Builder()
        {}

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

        public Builder setFS(FileSystem fs)
        {
            this.builderFS = requireNonNull(fs);

            return this;
        }

        public Builder setFilePath(Path filePath)
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

        public Builder setEncoding(boolean encoding)
        {
            this.encoding = encoding;

            return this;
        }

        public PixelsWriter build() throws PixelsWriterException
        {
            PhysicalWriter fsWriter = PhysicalWriterUtil.newPhysicalFSWriter(
                    this.builderFS, this.builderFilePath, this.builderBlockSize, this.builderReplication, this.builderBlockPadding);
            checkArgument(!requireNonNull(builderSchema.getChildren(), "schema is null").isEmpty(),
                    "schema is empty");

            if (fsWriter == null) {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException("Failed to create PixelsWriter due to error of creating PhysicalWriter");
            }

            return new PixelsWriterImpl(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    fsWriter,
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

    public boolean isEncoding()
    {
        return encoding;
    }

    /**
     * Add a row batch
     * Repeating is not supported currently in ColumnVector
     * */
    @Override
    public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException
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
            curRowGroupDataLength += writer.write(cvs[i], rowBatch.size);
        }
        // see if current size has exceeded the row group size. if so, write out current row group
        if (curRowGroupDataLength >= rowGroupSize) {
            writeRowGroup();
            curRowGroupDataLength = 0;
            return false;
        }
        return true;
    }

    /**
     * Close PixelsWriterImpl, indicating the end of file
     * */
    @Override
    public void close()
    {
        try
        {
            if (curRowGroupNumOfRows != 0) {
                writeRowGroup();
            }
            writeFileTail();
            physicalWriter.close();
            for (ColumnWriter cw : columnWriters)
            {
                cw.close();
            }
        } catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void writeRowGroup() throws IOException
    {
        this.isNewRowGroup = true;
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
            // new chunk for each writer
            writer.flush();
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
            return;
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
            // call children writer reset()
            writer.reset();
        }

        // put curRowGroupIndex into rowGroupFooter
        PixelsProto.RowGroupFooter rowGroupFooter =
                PixelsProto.RowGroupFooter.newBuilder()
                        .setRowGroupIndexEntry(curRowGroupIndex.build())
                        .setRowGroupEncoding(curRowGroupEncoding.build())
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
            return;
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

    public static void writeTypes(PixelsProto.Footer.Builder builder, TypeDescription schema)
    {
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();
        if (children == null || children.isEmpty()) {
            return;
        }
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
                return new BooleanColumnWriter(schema, pixelStride, isEncoding);
            case BYTE:
                return new ByteColumnWriter(schema, pixelStride, isEncoding);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnWriter(schema, pixelStride, isEncoding);
            case FLOAT:
                return new FloatColumnWriter(schema, pixelStride, isEncoding);
            case DOUBLE:
                return new DoubleColumnWriter(schema, pixelStride, isEncoding);
            case STRING:
                return new StringColumnWriter(schema, pixelStride, isEncoding);
            case CHAR:
                return new CharColumnWriter(schema, pixelStride, isEncoding);
            case VARCHAR:
                return new VarcharColumnWriter(schema, pixelStride, isEncoding);
            case BINARY:
                return new BinaryColumnWriter(schema, pixelStride, isEncoding);
            case TIMESTAMP:
                return new TimestampColumnWriter(schema, pixelStride, isEncoding);
            default:
                throw new IllegalArgumentException("Bad schema type: " + schema.getCategory());
        }
    }
}
