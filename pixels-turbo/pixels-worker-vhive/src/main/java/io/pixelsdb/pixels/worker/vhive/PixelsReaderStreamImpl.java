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

@NotThreadSafe
public class PixelsReaderStreamImpl implements PixelsReader
{
    private static final Logger LOGGER = LogManager.getLogger(io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.class);

    private final TypeDescription fileSchema;
    private final ByteBuf bufReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final Random random;

    private PixelsReaderStreamImpl(TypeDescription fileSchema,
                             ByteBuf bufReader,
                             PixelsProto.FileTail fileTail)
    {
        this.fileSchema = fileSchema;
        this.bufReader = bufReader;
        this.postScript = fileTail.getPostscript();
        this.footer = fileTail.getFooter();
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
            // get FileTail (moved to the beginning)
            int fileLen = builderBufReader.readableBytes();
            long fileTailOffset = builderBufReader.getLong(fileLen - Long.BYTES);
            int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
            // System.out.println("fileLen: " + fileLen + ", fileTailLength: " + fileTailLength + ", fileTailOffset: " + fileTailOffset);
            ByteBuf fileTailBuf = Unpooled.buffer(fileTailLength);
            builderBufReader.getBytes((int) fileTailOffset, fileTailBuf, fileTailLength);
            PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuf.nioBuffer());

            // check file MAGIC and file version
            PixelsProto.PostScript postScript = fileTail.getPostscript();
            int fileVersion = postScript.getVersion();
            String fileMagic = postScript.getMagic();
            if (!PixelsVersion.matchVersion(fileVersion))
            {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }
            if (!fileMagic.contentEquals(Constants.MAGIC))
            {
                throw new PixelsFileMagicInvalidException(fileMagic);
            }

            builderSchema = TypeDescription.createSchema(fileTail.getFooter().getTypesList());

            // create a default PixelsReader
            return new io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl(builderSchema, builderBufReader, fileTail);
        }
    }

    public static io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.Builder newBuilder()
    {
        return new io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl.Builder();
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId)
            throws IOException
    {
        long footerOffset = footer.getRowGroupInfos(rowGroupId).getFooterOffset();
        int footerLength = footer.getRowGroupInfos(rowGroupId).getFooterLength();
//        physicalReader.seek(footerOffset);
//        ByteBuffer footer = physicalReader.readFully(footerLength);
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
        return new PixelsRecordReaderStreamImpl(bufReader, postScript, footer, option);
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion()
    {
        return PixelsVersion.from(this.postScript.getVersion());
    }

    /**
     * Get the number of rows of the file
     *
     * @return num of rows
     */
    @Override
    public long getNumberOfRows()
    {
        return this.postScript.getNumberOfRows();
    }

    /**
     * Get the compression codec used in this file
     *
     * @return compression codec
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        return this.postScript.getCompression();
    }

    /**
     * Get the compression block size
     *
     * @return compression block size
     */
    @Override
    public long getCompressionBlockSize()
    {
        return this.postScript.getCompressionBlockSize();
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.postScript.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.postScript.getWriterTimezone();
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
        return this.footer.getRowGroupInfosCount();
    }

    @Override
    public boolean isPartitioned()
    {
        return this.postScript.hasPartitioned() && this.postScript.getPartitioned();
    }

    /**
     * Get file level statistics of each column
     *
     * @return array of column stat
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats()
    {
        return this.footer.getColumnStatsList();
    }

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName)
    {
        List<String> fieldNames = fileSchema.getFieldNames();
        int fieldId = fieldNames.indexOf(columnName);
        if (fieldId == -1)
        {
            return null;
        }
        return this.footer.getColumnStats(fieldId);
    }

    /**
     * Get information of all row groups
     *
     * @return array of row group information
     */
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        return this.footer.getRowGroupInfosList();
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
        return this.footer.getRowGroupInfos(rowGroupId);
    }

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId)
    {
        if (rowGroupId < 0)
        {
            return null;
        }
        return this.footer.getRowGroupStats(rowGroupId);
    }

    /**
     * Get statistics of all row groups
     *
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        return this.footer.getRowGroupStatsList();
    }

    public PixelsProto.PostScript getPostScript()
    {
        return postScript;
    }

    public PixelsProto.Footer getFooter()
    {
        return footer;
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
//        this.physicalReader.close();
        /* no need to close the pixelsCacheReader, because it is usually maintained
           as a global singleton instance and shared across different PixelsReaders.
         */

    }
}
