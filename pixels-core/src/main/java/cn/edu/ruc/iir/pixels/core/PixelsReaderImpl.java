package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.exception.PixelsFileMagicInvalidException;
import cn.edu.ruc.iir.pixels.core.exception.PixelsFileVersionInvalidException;
import cn.edu.ruc.iir.pixels.core.exception.PixelsReaderException;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReaderImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Pixels file reader default implementation
 *
 * This writer is NOT thread safe!
 *
 * @author guodong
 */
@NotThreadSafe
public class PixelsReaderImpl
        implements PixelsReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsReaderImpl.class);

    private final TypeDescription fileSchema;
    private final PhysicalFSReader physicalFSReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final List<PixelsRecordReader> recordReaders;

    private PixelsReaderImpl(TypeDescription fileSchema,
                            PhysicalFSReader physicalFSReader,
                            PixelsProto.FileTail fileTail)
    {
        this.fileSchema = fileSchema;
        this.physicalFSReader = physicalFSReader;
        this.postScript = fileTail.getPostscript();
        this.footer = fileTail.getFooter();
        this.recordReaders = new LinkedList<>();
    }

    public static class Builder
    {
        private FileSystem builderFS = null;
        private Path builderPath = null;
        private TypeDescription builderSchema = null;

        private Builder()
        {}

        public Builder setFS(FileSystem fs)
        {
            this.builderFS = requireNonNull(fs);
            return this;
        }

        public Builder setPath(Path path)
        {
            this.builderPath = requireNonNull(path);
            return this;
        }

        public Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = requireNonNull(schema);
            return this;
        }

        public PixelsReader build() throws IllegalArgumentException, IOException
        {
            // check arguments
            if (builderFS == null || builderPath == null || builderSchema == null) {
                throw new IllegalArgumentException("Missing argument to build PixelsReader");
            }
            // get PhysicalFSReader
            PhysicalFSReader fsReader = PhysicalReaderUtil.newPhysicalFSReader(builderFS, builderPath);
            if (fsReader == null) {
                LOGGER.error("Failed to create PhysicalFSReader");
                throw new PixelsReaderException("Failed to create PixelsReader due to error of creating PhysicalFSReader");
            }
            // get FileTail
            long fileLen = fsReader.getFileLength();
            fsReader.seek(fileLen - Long.BYTES);
            long fileTailOffset = fsReader.readLong();
            int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
            fsReader.seek(fileTailOffset);
            byte[] fileTailBuffer = new byte[fileTailLength];
            fsReader.readFully(fileTailBuffer);
            PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);

            // check file MAGIC and file version
            PixelsProto.PostScript postScript = fileTail.getPostscript();
            int fileVersion = postScript.getVersion();
            String fileMagic = postScript.getMagic();
            if (!PixelsVersion.matchVersion(fileVersion)) {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }
            if (!fileMagic.contentEquals(Constants.MAGIC)) {
                throw new PixelsFileMagicInvalidException(fileMagic);
            }

            // todo check file schema

            // create a default PixelsReader
            return new PixelsReaderImpl(builderSchema, fsReader, fileTail);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId) throws IOException
    {
        long footerOffset = footer.getRowGroupInfos(rowGroupId).getFooterOffset();
        long footerLength = footer.getRowGroupInfos(rowGroupId).getFooterLength();
        byte[] footer = new byte[(int) footerLength];
        physicalFSReader.seek(footerOffset);
        physicalFSReader.readFully(footer);
        return PixelsProto.RowGroupFooter.parseFrom(footer);
    }

    /**
     * Get a <code>PixelsRecordReader</code>
     *
     * @return record reader
     */
    @Override
    public PixelsRecordReader read(PixelsReaderOption option)
    {
        return new PixelsRecordReaderImpl(physicalFSReader, postScript, footer, option);
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
     * Get file level metric of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName)
    {
        List<String> fieldNames = fileSchema.getFieldNames();
        int fieldId = fieldNames.indexOf(columnName);
        if (fieldId == -1) {
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
        if (rowGroupId < 0) {
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
        if (rowGroupId < 0) {
            return null;
        }
        return this.footer.getRowGroupStats(rowGroupId);
    }

    /**
     * Get statistics of all row groups
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
    public void close() throws IOException
    {
        for (PixelsRecordReader recordReader : recordReaders) {
            recordReader.close();
        }
        this.physicalFSReader.close();
    }
}
