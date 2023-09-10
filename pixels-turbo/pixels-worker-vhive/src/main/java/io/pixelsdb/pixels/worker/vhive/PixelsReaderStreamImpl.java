package io.pixelsdb.pixels.worker.vhive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsVersion;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.exception.PixelsFileMagicInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsFileVersionInvalidException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static io.pixelsdb.pixels.common.utils.Constants.MAGIC;

@NotThreadSafe
public class PixelsReaderStreamImpl implements PixelsReader
{
    private static final Logger LOGGER = LogManager.getLogger(io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.class);

    private final TypeDescription fileSchema;
    private final ByteBuf bufReader;
    private final PixelsProto.StreamHeader streamHeader;
    private final Random random;

    private PixelsReaderStreamImpl(TypeDescription fileSchema,
                             ByteBuf bufReader,
                             PixelsProto.StreamHeader streamHeader)
    {
        this.fileSchema = fileSchema;
        this.bufReader = bufReader;
        this.streamHeader = streamHeader;
        this.random = new Random();
    }

    public static class Builder
    {
        private TypeDescription builderSchema = null;
        private ByteBuf builderBufReader = null;
        private int builderTotalBufLen = 0;

        private Builder()
        {
        }

        public Builder setBuilderBufReader(ByteBuf builderBufReader) {
            this.builderBufReader = builderBufReader;
            return this;
        }

        public Builder setBuilderTotalBufLen(int builderTotalBufLen) {
            this.builderTotalBufLen = builderTotalBufLen;
            return this;
        }

        static int calculateCeiling(int value, int multiple) {
            // to calculate padding length in HttpClient

            if (value <= 0 || multiple <= 0) {
                throw new IllegalArgumentException("Both value and multiple must be positive.");
            }

            int remainder = value % multiple;
            if (remainder == 0) {
                // No need to adjust, value is already a multiple of multiple
                return value;
            }

            int difference = multiple - remainder;
            return value + difference;
        }

        public PixelsReader build() throws IllegalArgumentException, IOException
        {
            // check MAGIC
            int magicLength = MAGIC.getBytes().length;
            byte[] magicBytes = new byte[magicLength];
            builderBufReader.getBytes(0, magicBytes);
            String magic = new String(magicBytes);
            if (!magic.contentEquals(Constants.MAGIC))
            {
                throw new PixelsFileMagicInvalidException(magic);
            }

            // parse streamHeader
            int metadataLength = builderBufReader.getInt(magicLength);  // getInt(int index)
//            System.out.println("Parsed metadataLength: " + metadataLength);
            ByteBuf metadataBuf = Unpooled.buffer(metadataLength);
            builderBufReader.getBytes(magicLength + Integer.BYTES, metadataBuf);
            PixelsProto.StreamHeader streamHeader = PixelsProto.StreamHeader.parseFrom(metadataBuf.nioBuffer());
//            System.out.println("Parsed streamHeader object: ");
//            System.out.println(streamHeader);

            // check file version
            int fileVersion = streamHeader.getVersion();
            if (!PixelsVersion.matchVersion(fileVersion))
            {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }

            // consume the padding bytes
            builderBufReader.readerIndex(calculateCeiling(magicLength + Integer.BYTES + metadataLength, 8));
//            System.out.println("streamHeader length incl padding: " + builderBufReader.readerIndex());

            // create a default PixelsReader
            // To this point, the readerIndex of bufReader is at the start of the actual rowGroups.
            builderSchema = TypeDescription.createSchema(streamHeader.getTypesList());
            return new io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl(builderSchema, builderBufReader, streamHeader);
        }
    }

    public static io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.Builder newBuilder()
    {
        return new io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.Builder();
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId) {
        throw new UnsupportedOperationException("getNumRowGroupFooter is not supported in a stream");
    }

    /**
     * Get a <code>PixelsRecordReader</code>
     *
     * @return record reader
     */
    @Override
    public PixelsRecordReader read(PixelsReaderOption option) throws IOException
    {
        float diceValue = random.nextFloat();
//        LOGGER.debug("create a recordReader with enableCache as " + enableCache);
        return new PixelsRecordReaderStreamImpl(bufReader, streamHeader, option);
        // Theoretically, it is still possible to append data to the bufReader while reading.
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion()
    {
        return PixelsVersion.from(this.streamHeader.getVersion());
    }

    /**
     * Get the number of rows of the file
     *
     * @return num of rows
     */
    // In streaming mode, the number of rows cannot be determined in advance.
    // 用到numberOfRows的有三种情况：数组大小；判断rgIdx是否越界；作为循环条件
    // 在之后要实现的streaming模式下，需要通过其他方式实现
    @Override
    public long getNumberOfRows()
    {
        throw new UnsupportedOperationException("getNumberOfRows is not supported in a stream");
    }

    /**
     * Get the compression codec used in this file. Currently unused and thus unsupported
     *
     * @return compression codec
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        throw new UnsupportedOperationException("getCompressionKind is currently not supported");
    }

    /**
     * Get the compression block size. Currently unused and thus unsupported
     *
     * @return compression block size
     */
    @Override
    public long getCompressionBlockSize()
    {
        throw new UnsupportedOperationException("getCompressionBlockSize is currently not supported");
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.streamHeader.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.streamHeader.getWriterTimezone();
    }

    /**
     * Get schema of this file
     *
     * @return schema
     */
    @Override
    public TypeDescription getFileSchema()
    {
        return this.fileSchema;
    }

    /**
     * Get the number of row groups in this file
     *
     * @return row group num
     */
    @Override
    public int getRowGroupNum()
    {
        throw new UnsupportedOperationException("getRowGroupNum is not supported in a stream");  // can modify it to display num of already read row groups
    }

    @Override
    public boolean isPartitioned()
    {
        return this.streamHeader.hasPartitioned() && this.streamHeader.getPartitioned();
    }

    /**
     * Get file level statistics of each column. Not required in streaming mode
     *
     * @return array of column stat
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats() {
        throw new UnsupportedOperationException("getColumnStats is not supported in a stream");
    }

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName) {
        throw new UnsupportedOperationException("getColumnStat is not supported in a stream");
    }

    /**
     * Get information of all row groups
     *
     * @return array of row group information
     */
    // todo: rowGroupInfo在WorkerCommon里读hashValue时需要用到。之后再考虑streaming模式下怎么实现
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        throw new UnsupportedOperationException("getRowGroupInfos is not supported in a stream");
    }

    /**
     * Get information of specified row group
     *
     * @param rowGroupId row group id
     * @return row group information
     */
    @Override
    public PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId)
    {
        throw new UnsupportedOperationException("getRowGroupInfo is not supported in a stream");
    }

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId) {
        throw new UnsupportedOperationException("getRowGroupStat is not supported in a stream");
    }

    /**
     * Get statistics of all row groups
     *
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats() {
        throw new UnsupportedOperationException("getRowGroupStats is not supported in a stream");
    }

    @Override
    public PixelsProto.PostScript getPostScript() {
        throw new UnsupportedOperationException("getPostScript is not supported in a stream");
    }

    @Override
    public PixelsProto.Footer getFooter() {
        throw new UnsupportedOperationException("getFooter is not supported in a stream");
    }

    public PixelsProto.StreamHeader getStreamHeader()
    {
        return streamHeader;
    }

    /**
     * Cleanup and release resources
     *
     * @throws IOException
     */
    @Override
    public void close()
            throws IOException
    {
        // this.physicalReader.close();
    }
}
