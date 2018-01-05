package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.exception.UnSupportedReaderException;
import cn.edu.ruc.iir.pixels.core.reader.VectorReader;
import cn.edu.ruc.iir.pixels.core.reader.VectorReaderImpl;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsReaderImpl implements PixelsReader
{
    private final int fileVersion;
    private final long rowNum;
    private final PixelsProto.CompressionKind compressionKind;
    private final long compressionBlockSize;
    private final long pixelStride;
    private final String writerTimeZone;
    private final TypeDescription fileSchema;
    private final int rowGroupNum;
    private final FSDataInputStream rawReader;

    private final PixelsProto.Footer footer;

    public PixelsReaderImpl(FileSystem fs, Path path) throws IOException, UnSupportedReaderException
    {
        this.rawReader = new PhysicalFSReader(fs, path).getRawReader();
        // get file length
        long fileLen = fs.getFileStatus(path).getLen();
        // seek to last Long which records FileTail offset
        rawReader.seek(fileLen - Long.BYTES);
        long tailOffset = rawReader.readLong();
        // calculate length of FileTail
        int tailLen = (int) (fileLen - tailOffset - Long.BYTES);
        // seek to FileTail offset and read FileTail content
        rawReader.seek(tailOffset);
        byte[] tailBuffer = new byte[tailLen];
        rawReader.readFully(tailBuffer);
        // close raw reader
        rawReader.close();

        PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(tailBuffer);
        
        PixelsProto.PostScript postScript = fileTail.getPostscript();
        this.fileVersion = postScript.getVersion();
        this.rowNum = postScript.getNumberOfRows();
        this.compressionKind = postScript.getCompression();
        this.compressionBlockSize = postScript.getCompressionBlockSize();
        this.pixelStride = postScript.getPixelStride();
        this.writerTimeZone = postScript.getWriterTimezone();

        if (!postScript.getMagic().equals(Constants.MAGIC) && (this.fileVersion > Constants.VERSION)) {
            throw new UnSupportedReaderException("Current reader is not compatible with this file");
        }
        
        this.footer = fileTail.getFooter();
        this.fileSchema = TypeDescription.createStruct();
        readTypes();
        this.rowGroupNum = footer.getRowGroupInfosCount();
    }
    
    private void readTypes()
    {
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
            fileSchema.addField(fieldName, fieldType);
        }
    }

    public PixelsProto.RowGroupFooter readRowGroupFooter(int rowGroupId) throws IOException
    {
        long footerOffset = footer.getRowGroupInfos(rowGroupId).getFooterOffset();
        long footerLength = footer.getRowGroupInfos(rowGroupId).getFooterLength();
        byte[] footer = new byte[(int) footerLength];
        rawReader.seek(footerOffset);
        rawReader.readFully(footer);
        PixelsProto.RowGroupFooter rowGroupFooter =
                PixelsProto.RowGroupFooter.parseFrom(footer);
        return rowGroupFooter;
    }

    /**
     * Get the iterator for reading rows inside the file
     *
     * @return {@code VectorReader}
     */
    @Override
    public VectorReader vectors(int[] selectedRowGroups, String[] selectedFieldNames) throws IOException
    {
        List<String> fieldNames = this.fileSchema.getFieldNames();
        int[] selectedFieldIds = new int[selectedFieldNames.length];
        for (int i = 0; i < selectedFieldNames.length; i++) {
            selectedFieldIds[i] = fieldNames.indexOf(selectedFieldNames[i]);
        }
        return new VectorReaderImpl(this, selectedRowGroups, selectedFieldIds);
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public int getFileVersion()
    {
        return this.fileVersion;
    }

    /**
     * Get the number of rows of the file
     *
     * @return num of rows
     */
    @Override
    public long getNumberOfRows()
    {
        return this.rowNum;
    }

    /**
     * Get the compression codec used in this file
     *
     * @return compression codec
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        return this.compressionKind;
    }

    /**
     * Get the compression block size
     *
     * @return compression block size
     */
    @Override
    public long getCompressionBlockSize()
    {
        return this.compressionBlockSize;
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.pixelStride;
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.writerTimeZone;
    }

    /**
     * Get schema of this file
     *
     * @return schema
     */
    @Override
    public TypeDescription getSchema()
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
        return this.rowGroupNum;
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
     *
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        return footer.getRowGroupStatsList();
    }
}
