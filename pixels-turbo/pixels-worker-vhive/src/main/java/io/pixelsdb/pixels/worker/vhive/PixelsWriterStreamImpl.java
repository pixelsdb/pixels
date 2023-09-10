package io.pixelsdb.pixels.worker.vhive;

import io.netty.buffer.ByteBuf;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.core.writer.ColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.Constants.MAGIC;
import static io.pixelsdb.pixels.core.writer.ColumnWriter.newColumnWriter;
import static java.util.Objects.requireNonNull;

/**
 * PixelsWriterStreamImpl is an implementation of {@link PixelsWriter} that writes
 * ColumnChunks to a stream, for operator pipelining over HTTP.
 */
@NotThreadSafe
public class PixelsWriterStreamImpl implements PixelsWriter {
    private static final Logger LOGGER = LogManager.getLogger(io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.class);

    private static final ByteOrder WRITER_ENDIAN;
    /**
     * The number of bytes that the start offset of each column chunk is aligned to.
     */
    private static final int CHUNK_ALIGNMENT;
    /**
     * The byte buffer padded to each column chunk for alignment.
     */
    private static final byte[] CHUNK_PADDING_BUFFER;

    static {
        boolean littleEndian = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("column.chunk.little.endian"));
        if (littleEndian) {
            WRITER_ENDIAN = ByteOrder.LITTLE_ENDIAN;
        } else {
            WRITER_ENDIAN = ByteOrder.BIG_ENDIAN;
        }
        CHUNK_ALIGNMENT = Integer.parseInt(ConfigFactory.Instance().getProperty("column.chunk.alignment"));
        checkArgument(CHUNK_ALIGNMENT >= 0, "column.chunk.alignment must >= 0");
        CHUNK_PADDING_BUFFER = new byte[CHUNK_ALIGNMENT];
    }

    private final TypeDescription schema;
    private final int rowGroupSize;
    private final PixelsProto.CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    /**
     * The writer option for the column writers.
     */
    private final PixelsWriterOption columnWriterOption;
    private final boolean partitioned;
    private final Optional<List<Integer>> partKeyColumnIds;

    private final ColumnWriter[] columnWriters;
    private long fileContentLength;
    private int fileRowNum;

    private long writtenBytes = 0L;
    private boolean isFirstRowGroup = true;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;  // curPacketDataLength
    /**
     * Whether any current hash value has been set.
     */
    private boolean hashValueIsSet = false;
    private int currHashValue = 0;

    private final ByteBuf bufWriter;  // file structure (NOT stream structure): [rowGroup + rowGroupFooter]* + fileTail + tailOffset
    // private int bufWriterIdx;
    // private final ByteBuf[] bufWriters;
    private final List<TypeDescription> children;

    private final ExecutorService columnWriterService = Executors.newCachedThreadPool();

    private PixelsWriterStreamImpl(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            PixelsProto.CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            ByteBuf bufWriter,
            EncodingLevel encodingLevel,
            boolean nullsPadding,
            boolean partitioned,
            Optional<List<Integer>> partKeyColumnIds) {
        this.schema = requireNonNull(schema, "schema is null");
        checkArgument(pixelStride > 0, "pixel stripe is not positive");
        checkArgument(rowGroupSize > 0, "row group size is not positive");
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        checkArgument(compressionBlockSize > 0, "compression block size is not positive");
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone);
        this.partitioned = partitioned;
        this.partKeyColumnIds = requireNonNull(partKeyColumnIds, "partKeyColumnIds is null");
        this.children = schema.getChildren();
        checkArgument(!requireNonNull(children, "schema is null").isEmpty(), "schema is empty");
        this.columnWriters = new ColumnWriter[children.size()];
        this.columnWriterOption = new PixelsWriterOption()
                .pixelStride(pixelStride)
                .encodingLevel(requireNonNull(encodingLevel, "encodingLevel is null"))
                .byteOrder(WRITER_ENDIAN)
                .nullsPadding(nullsPadding);
        for (int i = 0; i < children.size(); ++i) {
            columnWriters[i] = newColumnWriter(children.get(i), columnWriterOption);
        }

        this.bufWriter = bufWriter;
        // this.bufWriterIdx = 0;
    }

    public static class Builder {
        private TypeDescription builderSchema = null;
        private int builderPixelStride = 0;
        private int builderRowGroupSize = 0;
        private PixelsProto.CompressionKind builderCompressionKind = PixelsProto.CompressionKind.NONE;
        private int builderCompressionBlockSize = 1;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private EncodingLevel builderEncodingLevel = EncodingLevel.EL0;
        private boolean builderPartitioned = false;
        private boolean builderNullsPadding = false;
        private Optional<List<Integer>> builderPartKeyColumnIds = Optional.empty();
        private ByteBuf builderBufWriter = null;

        private Builder() {
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setSchema(TypeDescription schema) {
            this.builderSchema = requireNonNull(schema);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setPixelStride(int stride) {
            this.builderPixelStride = stride;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setRowGroupSize(int rowGroupSize) {
            this.builderRowGroupSize = rowGroupSize;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setCompressionKind(PixelsProto.CompressionKind compressionKind) {
            this.builderCompressionKind = requireNonNull(compressionKind);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setCompressionBlockSize(int compressionBlockSize) {
            this.builderCompressionBlockSize = compressionBlockSize;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setTimeZone(TimeZone timeZone) {
            this.builderTimeZone = requireNonNull(timeZone);
            return this;
        }

        public Builder setNullsPadding(boolean nullsPadding)
        {
            this.builderNullsPadding = nullsPadding;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setEncodingLevel(EncodingLevel encodingLevel) {
            this.builderEncodingLevel = encodingLevel;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setPartitioned(boolean partitioned) {
            this.builderPartitioned = partitioned;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setPartKeyColumnIds(List<Integer> partitionColumnIds) {
            this.builderPartKeyColumnIds = Optional.ofNullable(partitionColumnIds);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setBufWriter(ByteBuf bufWriter) {
            this.builderBufWriter = requireNonNull(bufWriter);
            return this;
        }

        public PixelsWriter build() throws PixelsWriterException {
            requireNonNull(this.builderSchema, "schema is not set");
            checkArgument(!requireNonNull(builderSchema.getChildren(),
                    "schema's children is null").isEmpty(), "schema is empty");
            checkArgument(this.builderPixelStride > 0, "pixels stride size is not set");
            checkArgument(this.builderRowGroupSize > 0, "row group size is not set");
            checkArgument(this.builderPartitioned ==
                            (this.builderPartKeyColumnIds.isPresent() && !this.builderPartKeyColumnIds.get().isEmpty()),
                    "partition column ids are present while partitioned is false, or vice versa");



            return new io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    builderBufWriter,
                    builderEncodingLevel,
                    builderNullsPadding,
                    builderPartitioned,
                    builderPartKeyColumnIds);
        }
    }

    public static io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder newBuilder() {
        return new io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder();
    }

    public TypeDescription getSchema() {
        return schema;
    }

    @Override
    public int getNumRowGroup() {
        throw new UnsupportedOperationException("getNumRowGroup is not supported in a stream");  // can modify it to display num of already written row groups
    }

    @Override
    public int getNumWriteRequests() {
        return 0;
//        if (physicalWriter == null) {
//            return 0;
//        }
//        return (int) Math.ceil(writtenBytes / (double) physicalWriter.getBufferSize());
    }

    @Override
    public long getCompletedBytes() {
        return writtenBytes;
    }

    public int getPixelStride() {
        return columnWriterOption.getPixelStride();
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public PixelsProto.CompressionKind getCompressionKind() {
        return compressionKind;
    }

    public int getCompressionBlockSize() {
        return compressionBlockSize;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public EncodingLevel getEncodingLevel() {
        return columnWriterOption.getEncodingLevel();
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    @Override
    public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException {
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
        if (curRowGroupDataLength >= rowGroupSize) {
            writeRowGroup();
            curRowGroupNumOfRows = 0L;
            return false;
        }
        writeRowGroup();  // todo: currently, in streaming mode, we have to flush on every row batch
        curRowGroupNumOfRows = 0L;
        return true;
    }

    @Override
    public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue) throws IOException {
        checkArgument(partitioned, "this file is not hash partitioned, " +
                "use addRowBatch(rowBatch) instead");
        if (hashValueIsSet) {
            // As the current hash value is set, at lease one row batch has been added.
            if (currHashValue != hashValue) {
                // Write the current partition (row group) and add the row batch to a new partition.
//                bufWriterIdx++;
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

    private void writeColumnVectors(ColumnVector[] columnVectors, int rowBatchSize) {
        CompletableFuture<?>[] futures = new CompletableFuture[columnVectors.length];
        AtomicInteger dataLength = new AtomicInteger(0);
        for (int i = 0; i < columnVectors.length; ++i) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ColumnWriter writer = columnWriters[i];
            ColumnVector columnVector = columnVectors[i];
            columnWriterService.execute(() ->
            {
                try {
                    dataLength.addAndGet(writer.write(columnVector, rowBatchSize));
                    future.complete(null);
                } catch (IOException e) {
                    throw new CompletionException("failed to write column vector", e);
                }
            });
            futures[i] = future;
        }
        CompletableFuture.allOf(futures).join();
        curRowGroupDataLength += dataLength.get();
    }

    /**
     * Close PixelsWriterStreamImpl, indicating the end of file.
     */
    @Override
    public void close() {
        try {
            if (curRowGroupNumOfRows != 0) {
                writeRowGroup();
            }
            // physicalWriter.close();
            for (ColumnWriter cw : columnWriters) {
                cw.close();
            }
            columnWriterService.shutdown();
            columnWriterService.shutdownNow();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void writeRowGroup() throws IOException {
        if (isFirstRowGroup) {writeStreamHeader(); isFirstRowGroup = false;}  // xxx: If we never call addRowBatch(), we will never write stream head

        int rowGroupDataLength = 0;

        PixelsProto.RowGroupInformation.Builder curRowGroupInfo =
                PixelsProto.RowGroupInformation.newBuilder();
        PixelsProto.RowGroupIndex.Builder curRowGroupIndex =
                PixelsProto.RowGroupIndex.newBuilder();
        PixelsProto.RowGroupEncoding.Builder curRowGroupEncoding =
                PixelsProto.RowGroupEncoding.newBuilder();

        // reset each column writer and get current row group content size in bytes
        for (ColumnWriter writer : columnWriters) {
            // flush writes the isNull bit map into the internal output stream.
            writer.flush();
            rowGroupDataLength += writer.getColumnChunkSize();
            if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0) {
                /*
                 * Issue #519:
                 * This is necessary as the prepare() method of some storage (e.g., hdfs)
                 * has to determine whether to start a new block, if the current block
                 * is not large enough.
                 */
                rowGroupDataLength += CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
            }
        }

        // write and flush row group content
        try {
            curRowGroupOffset = bufWriter.writerIndex();  // physicalWriter.prepare(rowGroupDataLength);
            if (curRowGroupOffset != -1) {
                // Issue #519: make sure to start writing the column chunks in the row group from an aligned offset.
                int tryAlign = 0;
                long writtenBytesBefore = writtenBytes;
                int rowGroupDataLenPos = bufWriter.writerIndex();
//                System.out.println("rowGroupDataLenPos: " + rowGroupDataLenPos);
                bufWriter.writeInt(0); // write a placeholder for row group data length
                writtenBytes += Integer.BYTES;
                curRowGroupOffset = bufWriter.writerIndex();

                while (CHUNK_ALIGNMENT != 0 && curRowGroupOffset % CHUNK_ALIGNMENT != 0 && tryAlign++ < 2) {
                    int alignBytes = (int) (CHUNK_ALIGNMENT - curRowGroupOffset % CHUNK_ALIGNMENT);
                    bufWriter.writeBytes(CHUNK_PADDING_BUFFER, 0, alignBytes);  // physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
                    writtenBytes += alignBytes;
                    curRowGroupOffset = bufWriter.writerIndex();  // physicalWriter.prepare(rowGroupDataLength);
                }
                if (tryAlign > 2) {
                    LOGGER.warn("failed to align the start offset of the column chunks in the row group");
                    throw new IOException("failed to align the start offset of the column chunks in the row group");
                }

                for (ColumnWriter writer : columnWriters) {
                    byte[] columnChunkBuffer = writer.getColumnChunkContent();
                    // System.out.println("Column offset: " + bufWriter.writerIndex() + ", size: " + columnChunkBuffer.length);
//                    System.out.println("Column position in bufWriter: " + bufWriter.writerIndex());
                    bufWriter.writeBytes(
                            columnChunkBuffer,
                            0, columnChunkBuffer.length);  // physicalWriter.append(columnChunkBuffer, 0, columnChunkBuffer.length);
                    writtenBytes += columnChunkBuffer.length;
                    // add align bytes to make sure the column size is the multiple of fsBlockSize
                    if (CHUNK_ALIGNMENT != 0 && columnChunkBuffer.length % CHUNK_ALIGNMENT != 0) {
                        int alignBytes = CHUNK_ALIGNMENT - columnChunkBuffer.length % CHUNK_ALIGNMENT;
                        bufWriter.writeBytes(CHUNK_PADDING_BUFFER, 0, alignBytes);  // physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
                        writtenBytes += alignBytes;
                    }
                }

                // write row group data len
                int rowGroupDataLen = bufWriter.writerIndex() - rowGroupDataLenPos - Integer.BYTES;
                if (rowGroupDataLen == writtenBytes - writtenBytesBefore) {
                    throw new IOException("Calculated rowGroupDataLen is not equal to recorded writtenBytes");
                }
                bufWriter.setInt(rowGroupDataLenPos, rowGroupDataLen);

                // physicalWriter.flush();
            } else {
                LOGGER.warn("write row group prepare failed");
                throw new IOException("write row group prepare failed");
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw e;
        }

        // update index and stats
        rowGroupDataLength = 0;
        for (int i = 0; i < columnWriters.length; i++) {
            ColumnWriter writer = columnWriters[i];
            PixelsProto.ColumnChunkIndex.Builder chunkIndexBuilder = writer.getColumnChunkIndex();
            chunkIndexBuilder.setChunkOffset(curRowGroupOffset + rowGroupDataLength);
            chunkIndexBuilder.setChunkLength(writer.getColumnChunkSize());
            // Design: Because ColumnChunkIndex does not change after the last write() or flush(), we can move it from rowGroup footer to header
            rowGroupDataLength += writer.getColumnChunkSize();
            if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0) {
                /*
                 * Issue #519:
                 * This line must be consistent with how column chunks are padded above.
                 * If we only pad after each column chunk when writing it into the physical writer
                 * without checking the alignment of the current position of the physical writer,
                 * then we should not consider curRowGroupOffset when calculating the alignment here.
                 */
                rowGroupDataLength += CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
            }
            // collect columnChunkIndex from every column chunk into curRowGroupIndex
            curRowGroupIndex.addColumnChunkIndexEntries(chunkIndexBuilder.build());
            // collect columnChunkEncoding
            curRowGroupEncoding.addColumnChunkEncodings(writer.getColumnChunkEncoding().build());
            // Design: ColumnChunkEncoding is final. We can move it from rowGroup footer to header
            /* TODO: writer.reset() does not work for partitioned file writing, fix it later.
             * The possible reason is that: when the file is partitioned, the last stride of a row group
             * (a.k.a., partition) is likely not full (length < pixelsStride), thus if the writer is not
             * reset correctly, the strides of the next row group will not be written correctly.
             * We temporarily fix this problem by creating a new column writer for each row group.
             */
            // writer.reset();
            columnWriters[i] = newColumnWriter(children.get(i), columnWriterOption);
        }

        // put curRowGroupIndex into rowGroupFooter
        PixelsProto.StreamRowGroupFooter rowGroupFooter =
                PixelsProto.StreamRowGroupFooter.newBuilder()
                        .setRowGroupIndexEntry(curRowGroupIndex.build())
                        .setRowGroupEncoding(curRowGroupEncoding.build())
                        .setNumberOfRows(curRowGroupNumOfRows)
                        .build();
        // XXX: rowGroupIndex and rowGroupEncoding are the same for all row groups in the same file? If so, we can
        //  send them only once per file

        // write and flush row group footer
        byte[] footerBuffer = rowGroupFooter.toByteArray();
        // physicalWriter.prepare(footerBuffer.length);
        curRowGroupFooterOffset = bufWriter.writerIndex();
//        System.out.println("position before writing rowGroupFooter: " + bufWriter.writerIndex());
        bufWriter.writeInt(footerBuffer.length);
        bufWriter.writeBytes(footerBuffer, 0, footerBuffer.length);
//        System.out.println("position after writing rowGroupFooter: " + bufWriter.writerIndex());
        // System.out.println("curRowGroupFooterOffset: " + curRowGroupFooterOffset);
        // curRowGroupFooterOffset = physicalWriter.append(footerBuffer, 0, footerBuffer.length);
        writtenBytes += footerBuffer.length;
        // physicalWriter.flush();

        // update RowGroupInformation, and put it into rowGroupInfoList
        curRowGroupInfo.setFooterOffset(curRowGroupFooterOffset);
        curRowGroupInfo.setDataLength(rowGroupDataLength);
        curRowGroupInfo.setFooterLength(rowGroupFooter.getSerializedSize());
        curRowGroupInfo.setNumberOfRows(curRowGroupNumOfRows);
        if (partitioned) {
            PixelsProto.PartitionInformation.Builder partitionInfo =
                    PixelsProto.PartitionInformation.newBuilder();
            // partitionColumnIds has been checked to be present in the builder.
            partitionInfo.addAllColumnIds(partKeyColumnIds.orElse(null));
            partitionInfo.setHashValue(currHashValue);
            curRowGroupInfo.setPartitionInfo(partitionInfo.build());
        }

        this.fileRowNum += curRowGroupNumOfRows;
        this.fileContentLength += rowGroupDataLength;
    }

    static void writeTypes(PixelsProto.StreamHeader.Builder builder, TypeDescription schema)
    {
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();
        if (children == null || children.isEmpty())
        {
            return;
        }
        for (int i = 0; i < children.size(); i++)
        {
            TypeDescription child = children.get(i);
            PixelsProto.Type.Builder tmpType = PixelsProto.Type.newBuilder();
            tmpType.setName(names.get(i));
            switch (child.getCategory())
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
                case DECIMAL:
                    tmpType.setKind(PixelsProto.Type.Kind.DECIMAL);
                    tmpType.setPrecision(child.getPrecision());
                    tmpType.setScale(child.getScale());
                    break;
                case STRING:
                    tmpType.setKind(PixelsProto.Type.Kind.STRING);
                    break;
                case CHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.CHAR);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case VARCHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.VARCHAR);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case BINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.BINARY);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case VARBINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.VARBINARY);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case TIMESTAMP:
                    tmpType.setKind(PixelsProto.Type.Kind.TIMESTAMP);
                    tmpType.setPrecision(child.getPrecision());
                    break;
                case DATE:
                    tmpType.setKind(PixelsProto.Type.Kind.DATE);
                    break;
                case TIME:
                    tmpType.setKind(PixelsProto.Type.Kind.TIME);
                    tmpType.setPrecision(child.getPrecision());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown category: " +
                            schema.getCategory());
            }
            builder.addTypes(tmpType.build());
        }
    }

    private void writeStreamHeader() {
        // build streamHeader
        PixelsProto.StreamHeader.Builder streamHeaderBuilder =
                PixelsProto.StreamHeader.newBuilder();
        writeTypes(streamHeaderBuilder, schema);
        streamHeaderBuilder.setVersion(Constants.VERSION)
                .setPixelStride(columnWriterOption.getPixelStride())
                .setWriterTimezone(timeZone.getDisplayName())
                .setPartitioned(partitioned)
                .setColumnChunkAlignment(CHUNK_ALIGNMENT)
                .setMagic(Constants.MAGIC)
                .build();
        PixelsProto.StreamHeader streamHeader = streamHeaderBuilder.build();
        int streamHeaderLength = streamHeader.getSerializedSize();

        // write and flush streamHeader
        ByteBuffer metadataLengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        metadataLengthBuffer.putInt(streamHeaderLength);
        metadataLengthBuffer.flip(); // Flip the buffer to change its position to the beginning

        byte[] magicBytes = MAGIC.getBytes();
        bufWriter.writeBytes(magicBytes);
        bufWriter.writeBytes(metadataLengthBuffer);
        bufWriter.writeBytes(streamHeader.toByteArray());
        writtenBytes += magicBytes.length + streamHeaderLength + Integer.BYTES;

        int paddingLength = (8 - (magicBytes.length + Integer.BYTES + streamHeaderLength) % 8) % 8;  // Can use '&7'
        byte[] paddingBytes = new byte[paddingLength];
        bufWriter.writeBytes(paddingBytes);
//        System.out.println("position after writing streamHeader: " + bufWriter.writerIndex());
        // physicalWriter.flush();
    }
}
