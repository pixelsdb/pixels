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
    private final PixelsProto.PipeliningMetadata pipeliningMetadata;
    private final PixelsProto.PipeliningFooter pipeliningFooter;
    private final Random random;

    private PixelsReaderStreamImpl(TypeDescription fileSchema,
                             ByteBuf bufReader,
                             PixelsProto.PipeliningMetadata pipeliningMetadata,
                                   PixelsProto.PipeliningFooter pipeliningFooter)
    {
        this.fileSchema = fileSchema;
        this.bufReader = bufReader;
        this.pipeliningMetadata = pipeliningMetadata;
        this.pipeliningFooter = pipeliningFooter;
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

        public PixelsReader build() throws IllegalArgumentException, IOException
        {
            // parse pipeliningFooter
            int totLen = builderBufReader.readableBytes();
            int streamFooterOffset = builderBufReader.getInt(totLen - Integer.BYTES);
            int streamFooterLength = totLen - streamFooterOffset - Integer.BYTES;
//            System.out.println("totLen: " + totLen + ", streamFooterOffset: " + streamFooterOffset + ", streamFooterLength: " + streamFooterLength);
            ByteBuf streamFooterBuf = Unpooled.buffer(streamFooterLength);
            builderBufReader.getBytes(streamFooterOffset, streamFooterBuf);
            PixelsProto.PipeliningFooter pipeliningFooter = PixelsProto.PipeliningFooter.parseFrom(streamFooterBuf.nioBuffer());
            // PixelsProto.PipeliningFooter pipeliningFooter = PixelsProto.PipeliningFooter.parseFrom(builderBufReader.array(), streamFooterOffset, streamFooterLength);
            //            this API seems unsupported in protobuf 2
//            System.out.println("Parsed pipeliningFooter: ");
//            System.out.println(pipeliningFooter);

            // check MAGIC
            int magicLength = MAGIC.getBytes().length;
            byte[] magicBytes = new byte[magicLength];
            builderBufReader.getBytes(0, magicBytes);
            String magic = new String(magicBytes);
            if (!magic.contentEquals(Constants.MAGIC))
            {
                throw new PixelsFileMagicInvalidException(magic);
            }

            // parse metadata
            int metadataLength = builderBufReader.getInt(magicLength);  // getInt(int index)
            // System.out.println("Parsed metadataLength: " + metadataLength);
            ByteBuf metadataBuf = Unpooled.buffer(metadataLength);
            builderBufReader.getBytes(magicLength + Integer.BYTES, metadataBuf);
            PixelsProto.PipeliningMetadata metadata = PixelsProto.PipeliningMetadata.parseFrom(metadataBuf.nioBuffer());
//            System.out.println("Parsed metadata object: ");
//            System.out.println(metadata);

            // check file version
            int fileVersion = metadata.getVersion();
            if (!PixelsVersion.matchVersion(fileVersion))
            {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }

            // consume the padding bytes?

            // create a default PixelsReader
            builderSchema = TypeDescription.createSchema(metadata.getTypesList());
            return new io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl(builderSchema, builderBufReader, metadata, pipeliningFooter);
        }
    }

    public static io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.Builder newBuilder()
    {
        return new io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.Builder();
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId)
            throws IOException
    {
        long footerOffset = pipeliningFooter.getRowGroupInfos(rowGroupId).getFooterOffset();
        int footerLength = pipeliningFooter.getRowGroupInfos(rowGroupId).getFooterLength();
        // physicalReader.seek(footerOffset);
        // ByteBuffer footer = physicalReader.readFully(footerLength);
        bufReader.readerIndex((int) footerOffset);
        ByteBuffer footer = bufReader.readBytes(footerLength).nioBuffer();
        return PixelsProto.RowGroupFooter.parseFrom(footer);
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
        return new PixelsRecordReaderStreamImpl(bufReader, pipeliningMetadata, pipeliningFooter, option);
        // Note that it is still possible to append data to the bufReader while reading.
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion()
    {
        return PixelsVersion.from(this.pipeliningMetadata.getVersion());
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
        return this.pipeliningFooter.getNumberOfRows();
    }

    /**
     * Get the compression codec used in this file. Currently unused and thus unsupported
     *
     * @return compression codec
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        return null;
    }

    /**
     * Get the compression block size. Currently unused and thus unsupported
     *
     * @return compression block size
     */
    @Override
    public long getCompressionBlockSize()
    {
        return 0;
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.pipeliningMetadata.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.pipeliningMetadata.getWriterTimezone();
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
        return this.pipeliningFooter.getRowGroupInfosCount();
    }

    @Override
    public boolean isPartitioned()
    {
        return this.pipeliningMetadata.hasPartitioned() && this.pipeliningMetadata.getPartitioned();
    }

    /**
     * Get file level statistics of each column. Not required in streaming mode
     *
     * @return array of column stat
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats() {
        return null;
    }

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName) {
        return null;
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
        return this.pipeliningFooter.getRowGroupInfosList();
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
        if (rowGroupId < 0)
        {
            return null;
        }
        return this.pipeliningFooter.getRowGroupInfos(rowGroupId);
    }

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId) {
        return null;
    }

    /**
     * Get statistics of all row groups
     *
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats() {
        return null;
    }

    @Override
    public PixelsProto.PostScript getPostScript() {
        return null;
    }

    @Override
    public PixelsProto.Footer getFooter() {
        return null;
    }

    public PixelsProto.PipeliningMetadata getPipeliningMetadata()
    {
        return pipeliningMetadata;
    }

    public PixelsProto.PipeliningFooter getPipeliningFooter()
    {
        return pipeliningFooter;
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
