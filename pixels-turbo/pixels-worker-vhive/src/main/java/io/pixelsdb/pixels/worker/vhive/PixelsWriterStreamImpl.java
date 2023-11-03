package io.pixelsdb.pixels.worker.vhive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import org.asynchttpclient.*;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.pixelsdb.pixels.common.utils.Constants.MAGIC;
import static io.pixelsdb.pixels.core.writer.ColumnWriter.newColumnWriter;
import static java.util.Objects.requireNonNull;

/**
 * PixelsWriterStreamImpl is an implementation of {@link PixelsWriter} that writes
 * ColumnChunks to a stream, for operator pipelining over HTTP.
 */
@NotThreadSafe
public class PixelsWriterStreamImpl implements PixelsWriter {
    private static final Logger LOGGER = LogManager.getLogger(PixelsWriterStreamImpl.class);

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
    private int rowGroupNum = 0;

    private long writtenBytes = 0L;
    private boolean isFirstRowGroup = true;  // private boolean streamHeaderSent = false;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;  // curPacketDataLength
    /**
     * Whether any current hash value has been set.
     */
    private boolean hashValueIsSet = false;
    private int currHashValue = 0;

    private final ByteBuf byteBuf;  // file structure (NOT stream structure): [rowGroup + rowGroupFooter]* + fileTail + tailOffset
    // private int bufWriterIdx;
    // private final ByteBuf[] bufWriters;
    private final String endpoint;  // IP:port
    private final AsyncHttpClient httpClient;
    private final Semaphore outstandingRequestSemaphore = new Semaphore(1); // Only allow 1 outstanding request at a time

    private final List<TypeDescription> children;
    private final ExecutorService columnWriterService = Executors.newCachedThreadPool();

    private PixelsWriterStreamImpl(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            PixelsProto.CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            EncodingLevel encodingLevel,
            boolean nullsPadding,
            boolean partitioned,
            Optional<List<Integer>> partKeyColumnIds,
            String endpoint) {
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

        this.byteBuf = Unpooled.buffer(); // ??? Unpooled.directBuffer();
        this.endpoint = endpoint;
        this.httpClient = Dsl.asyncHttpClient();
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
        private String builderEndpoint = null;

        private Builder() {
        }

        public Builder setSchema(TypeDescription schema) {
            this.builderSchema = requireNonNull(schema);
            return this;
        }

        public Builder setPixelStride(int stride) {
            this.builderPixelStride = stride;
            return this;
        }

        public Builder setRowGroupSize(int rowGroupSize) {
            this.builderRowGroupSize = rowGroupSize;
            return this;
        }

        public Builder setCompressionKind(PixelsProto.CompressionKind compressionKind) {
            this.builderCompressionKind = requireNonNull(compressionKind);
            return this;
        }

        public Builder setCompressionBlockSize(int compressionBlockSize) {
            this.builderCompressionBlockSize = compressionBlockSize;
            return this;
        }

        public Builder setTimeZone(TimeZone timeZone) {
            this.builderTimeZone = requireNonNull(timeZone);
            return this;
        }

        public Builder setNullsPadding(boolean nullsPadding)
        {
            this.builderNullsPadding = nullsPadding;
            return this;
        }

        public Builder setEncodingLevel(EncodingLevel encodingLevel) {
            this.builderEncodingLevel = encodingLevel;
            return this;
        }

        public Builder setPartitioned(boolean partitioned) {
            this.builderPartitioned = partitioned;
            return this;
        }

        public Builder setPartKeyColumnIds(List<Integer> partitionColumnIds) {
            this.builderPartKeyColumnIds = Optional.ofNullable(partitionColumnIds);
            return this;
        }

        public Builder setEndpoint(String endpoint) {
            this.builderEndpoint = requireNonNull(endpoint);
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
            // todo: check the other arguments


            return new PixelsWriterStreamImpl(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    builderEncodingLevel,
                    builderNullsPadding,
                    builderPartitioned,
                    builderPartKeyColumnIds,
                    builderEndpoint);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public TypeDescription getSchema() {
        return schema;
    }

    @Override
    public int getNumRowGroup() {
        // returns the num of already written row groups
        return rowGroupNum;
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
//        writeRowGroup();
//        curRowGroupNumOfRows = 0L;
//        // XXX: In Junit tests, we have to flush on every row batch rather than row group. Add a parameter to control this.
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
     * Close PixelsWriterStreamImpl, indicating the end of stream.
     */
    @Override
    public void close() {
        try {
            if (curRowGroupNumOfRows != 0) {
                writeRowGroup();
            }

            // close the HTTP connection
            Request req = httpClient.preparePost(endpoint)
                    .addHeader(CONTENT_TYPE, "application/x-protobuf")
                    .addHeader(CONTENT_LENGTH, 0)
                    .addHeader(CONNECTION, CLOSE)
                    .build();
//            LOGGER.debug("Sending close request to server");
            try {
                outstandingRequestSemaphore.acquire();
                Response response = httpClient.executeRequest(req).get();
                if (response.getStatusCode() != 200) {
                    throw new IOException("Failed to send close request to server. Is the server already closed? status code: " + response.getStatusCode());
                }
            } catch (Throwable e) {
                // warning that remote has already closed, but not throwing out the exception
                LOGGER.warn(e.getMessage());
                e.printStackTrace();
            }

            for (ColumnWriter cw : columnWriters) {
                cw.close();
            }
            columnWriterService.shutdown();
            columnWriterService.shutdownNow();

            if (byteBuf.refCnt() > 0) {
                byteBuf.release();
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    // XXX: In demo version, I have modified this method to be non-private, because StreamWorkerCommon::passSchemaToNextLevel() needs it.
    //  This is a bad practice. todo Change it back.
    void writeRowGroup() throws IOException {
        LOGGER.debug("writeRowGroup() called");
        // Maybe use Unpooled.wrappedBuffer() instead of ByteBuf.writeBytes()
        if (isFirstRowGroup) {writeStreamHeader(); isFirstRowGroup = false;}  // XXX: If we never call addRowBatch(), we will never write stream head
        LOGGER.debug("byteBuf writerIndex: " + byteBuf.writerIndex());
        // if (!streamHeaderSent) {writeStreamHeader(); streamHeaderSent = true;} // XXX: Should we put it here or in the AddRowBatch()?

        int rowGroupDataLength = 0;

//        PixelsProto.RowGroupInformation.Builder curRowGroupInfo =
//                PixelsProto.RowGroupInformation.newBuilder();
        PixelsProto.RowGroupIndex.Builder curRowGroupIndex =
                PixelsProto.RowGroupIndex.newBuilder();
        PixelsProto.RowGroupEncoding.Builder curRowGroupEncoding =
                PixelsProto.RowGroupEncoding.newBuilder();

        // reset each column writer and get current row group content size in bytes
        for (ColumnWriter writer : columnWriters) {
            // flush writes the isNull bit map into the internal output stream.
            writer.flush();
//            rowGroupDataLength += writer.getColumnChunkSize();
//            if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0) {
                /*
                 * Issue #519:
                 * This is necessary as the prepare() method of some storage (e.g., hdfs)
                 * has to determine whether to start a new block, if the current block
                 * is not large enough.
                 */
//                rowGroupDataLength += CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
//            }
        }

        // write and flush row group content
        int recordedRowGroupDataLen = 0;
        try {
            curRowGroupOffset = byteBuf.writerIndex();  // physicalWriter.prepare(rowGroupDataLength);
            if (curRowGroupOffset != -1) {  // Actually, it is always true because a ByteBuf's writerIndex is never -1. Just keep the code for compatibility.
                // Issue #519: make sure to start writing the column chunks in the row group from an aligned offset.
                int tryAlign = 0;
                long writtenBytesBefore = writtenBytes;
                int rowGroupDataLenPos = byteBuf.writerIndex();
                LOGGER.debug("rowGroupDataLenPos: " + rowGroupDataLenPos);
                byteBuf.writeInt(0); // write a placeholder for row group data length
                writtenBytes += Integer.BYTES;
                curRowGroupOffset = byteBuf.writerIndex();

                while (CHUNK_ALIGNMENT != 0 && curRowGroupOffset % CHUNK_ALIGNMENT != 0 && tryAlign++ < 2) {
                    int alignBytes = (int) (CHUNK_ALIGNMENT - curRowGroupOffset % CHUNK_ALIGNMENT);
                    byteBuf.writeBytes(CHUNK_PADDING_BUFFER, 0, alignBytes);  // physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
                    writtenBytes += alignBytes;
                    curRowGroupOffset = byteBuf.writerIndex();  // physicalWriter.prepare(rowGroupDataLength);
                }
                if (tryAlign > 2) {
                    LOGGER.warn("failed to align the start offset of the column chunks in the row group");
                    throw new IOException("failed to align the start offset of the column chunks in the row group");
                }

                for (ColumnWriter writer : columnWriters) {
                    byte[] columnChunkBuffer = writer.getColumnChunkContent();
                    LOGGER.debug("Written Column offset: " + byteBuf.writerIndex() + ", column size: " + columnChunkBuffer.length);

                    PixelsProto.ColumnChunkIndex.Builder chunkIndexBuilder = writer.getColumnChunkIndex();
                    chunkIndexBuilder.setChunkOffset(byteBuf.writerIndex());
                    chunkIndexBuilder.setChunkLength(writer.getColumnChunkSize());
                    curRowGroupIndex.addColumnChunkIndexEntries(chunkIndexBuilder.build());
                    curRowGroupEncoding.addColumnChunkEncodings(writer.getColumnChunkEncoding().build());

                    byteBuf.writeBytes(
                            columnChunkBuffer,
                            0, columnChunkBuffer.length);  // physicalWriter.append(columnChunkBuffer, 0, columnChunkBuffer.length);
                    writtenBytes += columnChunkBuffer.length;
                    // add align bytes to make sure the column size is the multiple of fsBlockSize
                    if (CHUNK_ALIGNMENT != 0 && columnChunkBuffer.length % CHUNK_ALIGNMENT != 0) {
                        int alignBytes = CHUNK_ALIGNMENT - columnChunkBuffer.length % CHUNK_ALIGNMENT;
                        byteBuf.writeBytes(CHUNK_PADDING_BUFFER, 0, alignBytes);  // physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
                        writtenBytes += alignBytes;
                    }
                }

                // write row group data len
                recordedRowGroupDataLen = byteBuf.writerIndex() - rowGroupDataLenPos;  // - Integer.BYTES;
                // LOGGER.debug("recordedRowGroupDataLength: " + recordedRowGroupDataLen + ", accumulatedRowGroupDataLen: " + (writtenBytes - writtenBytesBefore));
                if (recordedRowGroupDataLen != writtenBytes - writtenBytesBefore)
                    LOGGER.warn("Recorded rowGroupDataLen is not equal to accumulated writtenBytes");
                byteBuf.setInt(rowGroupDataLenPos, recordedRowGroupDataLen);

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
        LOGGER.debug("rowGroupDataLength: " + rowGroupDataLength + ", recordedRowGroupDataLen: " + recordedRowGroupDataLen);
        if (rowGroupDataLength + 32 != recordedRowGroupDataLen) LOGGER.warn("");  // throw new IOException

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
        curRowGroupFooterOffset = byteBuf.writerIndex();
        LOGGER.debug("writerIndex before writing rowGroupFooter: " + byteBuf.writerIndex());
        byteBuf.writeInt(footerBuffer.length);
        byteBuf.writeBytes(footerBuffer, 0, footerBuffer.length);
        LOGGER.debug("writerIndex after writing rowGroupFooter: " + byteBuf.writerIndex());
        // curRowGroupFooterOffset = physicalWriter.append(footerBuffer, 0, footerBuffer.length);
        writtenBytes += footerBuffer.length;
        // physicalWriter.flush();

        // update RowGroupInformation, and put it into rowGroupInfoList
//        curRowGroupInfo.setFooterOffset(curRowGroupFooterOffset);
//        curRowGroupInfo.setDataLength(rowGroupDataLength);
//        curRowGroupInfo.setFooterLength(rowGroupFooter.getSerializedSize());
//        curRowGroupInfo.setNumberOfRows(curRowGroupNumOfRows);
        if (partitioned) {
            PixelsProto.PartitionInformation.Builder partitionInfo =
                    PixelsProto.PartitionInformation.newBuilder();
            // partitionColumnIds has been checked to be present in the builder.
            partitionInfo.addAllColumnIds(partKeyColumnIds.orElse(null));
            partitionInfo.setHashValue(currHashValue);
//            curRowGroupInfo.setPartitionInfo(partitionInfo.build());
        }

        this.fileRowNum += curRowGroupNumOfRows;
        this.rowGroupNum++;
//        this.fileContentLength += rowGroupDataLength;

        // Added: Send row group to server
        ByteBuf byteBufCopy = byteBuf.copy();
        Request req = httpClient.preparePost(endpoint)
                .addFormParam("param1", "value1")  // can use this to record the hashValue
                .setBody(byteBufCopy.nioBuffer())
                .addHeader(CONTENT_TYPE, "application/x-protobuf")
                .addHeader(CONTENT_LENGTH, byteBuf.readableBytes())
                .addHeader(CONNECTION, "keep-alive")
                .build();
        try {
            outstandingRequestSemaphore.acquire();
            httpClient.executeRequest(req, new AsyncCompletionHandler<Response>() {

                @Override
                public Response onCompleted(Response response) throws Exception {
                    byteBufCopy.release();
                    if (response.getStatusCode() != 200) {
                        throw new IOException("Failed to send row group to server, status code: " + response.getStatusCode());
                    }
                    outstandingRequestSemaphore.release();
                    return response;
                }

                @Override
                public void onThrowable(Throwable t) {
                    byteBufCopy.release();
                    LOGGER.error(t.getMessage());
                    t.printStackTrace();
                    outstandingRequestSemaphore.release();
                }
            });  // todo: Does this API keep the connection open after returning?
            // Currently, only 1 outstanding request is allowed, for the sake of simplicity.
            //  i.e., the writer will block if there is already an outstanding request, and only sends the next row group after the previous request returns.
        } catch (Throwable e) {
//                if (e instanceof ConnectException) {
//                    LOGGER.warn("Connection refused, retrying...");
//                    try {
//                        TimeUnit.MILLISECONDS.sleep(20);
//                    } catch (InterruptedException interruptedException) {
//                        Thread.currentThread().interrupt();
//                    }
//                }
//                else {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
//                }
        }
        byteBuf.clear();
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
        byteBuf.writeBytes(magicBytes);
        byteBuf.writeBytes(metadataLengthBuffer);
        byteBuf.writeBytes(streamHeader.toByteArray());

        int paddingLength = (8 - (magicBytes.length + Integer.BYTES + streamHeaderLength) % 8) % 8;  // Can use '&7'
        byte[] paddingBytes = new byte[paddingLength];
        byteBuf.writeBytes(paddingBytes);
        writtenBytes += magicBytes.length + streamHeaderLength + Integer.BYTES + paddingLength;
//        System.out.println("position after writing streamHeader: " + bufWriter.writerIndex());
        // physicalWriter.flush();
    }
}
