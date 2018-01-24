package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.exception.PixelsFileMagicInvalidException;
import cn.edu.ruc.iir.pixels.core.exception.PixelsFileVersionInvalidException;
import cn.edu.ruc.iir.pixels.core.exception.PixelsReaderException;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReaderOption;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Pixels reader default implementation
 *
 * @author guodong
 */
public class PixelsReaderImpl
        implements PixelsReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsReaderImpl.class);

    private final FileSystem fs;
    private final Path path;
    private final TypeDescription fileSchema;
    private final PhysicalFSReader physicalFSReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;

    public PixelsReaderImpl(FileSystem fs, Path path,
                            TypeDescription fileSchema,
                            PhysicalFSReader physicalFSReader,
                            PixelsProto.FileTail fileTail)
    {
        this.fs = fs;
        this.path = path;
        this.fileSchema = fileSchema;
        this.physicalFSReader = physicalFSReader;
        this.postScript = fileTail.getPostscript();
        this.footer = fileTail.getFooter();
    }

    public static class Builder
    {
        private FileSystem builderFS = null;
        private Path builderPath = null;
        private TypeDescription builderSchema = null;
        // todo add layouts information

        public Builder setFS(FileSystem fs)
        {
            this.builderFS = fs;
            return this;
        }

        public Builder setPath(Path path)
        {
            this.builderPath = path;
            return this;
        }

        public Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = schema;
            return this;
        }

        public PixelsReader build() throws IllegalArgumentException, IOException
        {
            // check arguments
            if (builderFS == null || builderPath == null || builderSchema == null) {
                throw new IllegalArgumentException("Missing argument to build PixelsReader");
            }
            // get PhysicalFSReader
            PhysicalFSReader fsReader = PhysicalFSReaderUtil.newPhysicalFSReader(builderFS, builderPath);
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
            return new PixelsReaderImpl(builderFS, builderPath, builderSchema, fsReader, fileTail);
        }
    }

    /**
     * THIS SHOULD BE USELESS
     * */
    private TypeDescription readTypes()
    {
        TypeDescription schema = TypeDescription.createStruct();
        List<PixelsProto.Type> types = footer.getTypesList();
        for (PixelsProto.Type type : types) {
            String fieldName = type.getName();
            TypeDescription fieldType;
            switch (type.getKind())
            {
                case INT:
                    fieldType = TypeDescription.createInt();
                    break;
                case BYTE:
                    fieldType = TypeDescription.createByte();
                    break;
                case CHAR:
                    fieldType = TypeDescription.createChar();
                    break;
                case DATE:
                    fieldType = TypeDescription.createDate();
                    break;
                case LONG:
                    fieldType = TypeDescription.createLong();
                    break;
                case FLOAT:
                    fieldType = TypeDescription.createFloat();
                    break;
                case SHORT:
                    fieldType = TypeDescription.createShort();
                    break;
                case BINARY:
                    fieldType = TypeDescription.createBinary();
                    break;
                case DOUBLE:
                    fieldType = TypeDescription.createDouble();
                    break;
                case STRING:
                    fieldType = TypeDescription.createString();
                    break;
                case BOOLEAN:
                    fieldType = TypeDescription.createBoolean();
                    break;
                case VARCHAR:
                    fieldType = TypeDescription.createVarchar();
                    break;
                case TIMESTAMP:
                    fieldType = TypeDescription.createTimestamp();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " +
                            type.getKind());
            }
            schema.addField(fieldName, fieldType);
        }
        return schema;
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
     * @throws IOException
     */
    @Override
    public PixelsRecordReader read(PixelsRecordReaderOption option) throws IOException
    {
        return null;
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
        return footer.getColumnStatsList();
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
        if (fieldId == -1) {
            return null;
        }
        return footer.getColumnStats(fieldId);
    }

    /**
     * Get information of all row groups
     *
     * @return array of row group information
     */
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        return footer.getRowGroupInfosList();
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
        return footer.getRowGroupInfos(rowGroupId);
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
        return footer.getRowGroupStats(rowGroupId);
    }

    /**
     * Get statistics of all row groups
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        return footer.getRowGroupStatsList();
    }

    /**
     * Get file system
     * @return file system
     * */
    public FileSystem getFs()
    {
        return fs;
    }

    /**
     * Get file path
     * @return file path
     * */
    public Path getPath()
    {
        return path;
    }

    /**
     * Cleanup and release resources
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        physicalFSReader.close();
    }
}
