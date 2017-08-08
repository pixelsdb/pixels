package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.PixelsProto.ColumnStatistic;
import cn.edu.ruc.iir.pixels.core.PixelsProto.CompressionKind;
import cn.edu.ruc.iir.pixels.core.PixelsProto.Footer;
import cn.edu.ruc.iir.pixels.core.PixelsProto.RowGroupInformation;
import cn.edu.ruc.iir.pixels.core.PixelsProto.RowGroupStatistic;
import cn.edu.ruc.iir.pixels.core.PixelsProto.PostScript;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.core.writer.BinaryColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.BooleanColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.BytesColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.CharColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.ColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.DoubleColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.FloatColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.IntegerColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.StringColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.TimestampColumnWriter;
import cn.edu.ruc.iir.pixels.core.writer.VarcharColumnWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Pixels file writer
 *
 * This writer is NOT thread safe!
 *
 * @author guodong
 */
public class PixelsWriter
{
    private final TypeDescription schema;
    private final int pixelSize;
    private final String filePath;
    private final int rowGroupSize;
    private final CompressionKind compressionKind;
    private final int compressionBlockSize;

    private final ColumnWriter[] columnWriters;
    private List<RowGroupInformation> rowGroupInformations;
    private List<RowGroupStatistic> rowGroupStatistics;
    private ColumnStatistic fileColumnStatistic;         // file level column statistic
    private PostScript postScript;
    private Footer footer;
    private long fileContentLength;
    private long fileRowNum;

    private int rowGroupNum = 0;

    private ByteBuffer rowGroupBuffer;

    private PixelsWriter(TypeDescription schema, int pixelSize, String filePath, int rowGroupSize)
    {
        this(schema, pixelSize, filePath, rowGroupSize, CompressionKind.NONE, 0);
    }

    private PixelsWriter(
            TypeDescription schema,
            int pixelSize,
            String filePath,
            int rowGroupSize,
            CompressionKind compressionKind,
            int compresseionBlockSize)
    {
        this.schema = schema;
        this.pixelSize = pixelSize;
        this.filePath = filePath;
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = compressionKind;
        this.compressionBlockSize = compresseionBlockSize;

        List<TypeDescription> children = schema.getChildren();
        this.columnWriters = new ColumnWriter[children.size()];
        for (int i = 0; i < columnWriters.length; ++i)
        {
            columnWriters[i] = createWriter(schema);
        }
        rowGroupInformations = new ArrayList<>();
        rowGroupStatistics = new ArrayList<>();

        rowGroupBuffer = ByteBuffer.allocate(rowGroupSize * Constants.MB1);
    }

    public static class Builer
    {
        private TypeDescription builderSchema;
        private int builderPixelSize;
        private String builderFilePath;
        private int builderRowGroupSize;  // group size in MB
        private CompressionKind builderCompressionKind;
        private int builderCompressionBlockSize;

        public void setSchema(TypeDescription schema)
        {
            this.builderSchema = schema;
        }

        public void setBuilderPixelSize(int pixelSize)
        {
            this.builderPixelSize = pixelSize;
        }

        public void setFilePath(String filePath)
        {
            this.builderFilePath = filePath;
        }

        public void setBuilderRowGroupSize(int rowGroupSize)
        {
            this.builderRowGroupSize = rowGroupSize;
        }

        public void setBuilderCompressionKind(CompressionKind compressionKind)
        {
            this.builderCompressionKind = compressionKind;
        }

        public void setBuilderCompressionBlockSize(int compressionBlockSize)
        {
            this.builderCompressionBlockSize = compressionBlockSize;
        }

        public PixelsWriter build()
        {
            return new PixelsWriter(builderSchema, builderPixelSize, builderFilePath, builderRowGroupSize);
        }
    }

    public TypeDescription getSchema()
    {
        return schema;
    }

    public int getPixelSize()
    {
        return pixelSize;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public int getCompressionBlockSize()
    {
        return compressionBlockSize;
    }

    public ColumnWriter[] getColumnWriters()
    {
        return columnWriters;
    }

    public void addRowBatch(VectorizedRowBatch rowBatch)
    {
        ColumnVector[] cvs = rowBatch.cols;
        for (int i = 0; i < cvs.length; i++)
        {
            ColumnWriter writer = columnWriters[i];
        }
    }

    public void close()
    {}

    /**
     * Write file tail
     * */
    private void writeFileTail()
    {}

    private void writeFooter()
    {}

    private void writePostScript()
    {}

    private void writeRowGroup()
    {
        // write each vector column:
        //     serialize vector
        //     add pixel statistics
        //     return serialized bytes and pixel statistics

        // add pixel statistics

        // update file column statistics
    }

    private ColumnWriter createWriter(TypeDescription schema)
    {
        switch (schema.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnWriter(schema);
            case BYTE:
                return new BytesColumnWriter(schema);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnWriter(schema);
            case FLOAT:
                return new FloatColumnWriter(schema);
            case DOUBLE:
                return new DoubleColumnWriter(schema);
            case STRING:
                return new StringColumnWriter(schema);
            case CHAR:
                return new CharColumnWriter(schema);
            case VARCHAR:
                return new VarcharColumnWriter(schema);
            case BINARY:
                return new BinaryColumnWriter(schema);
            case TIMESTAMP:
                return new TimestampColumnWriter(schema);
            default:
                throw new IllegalArgumentException("Bad schema type: " + schema.getCategory());
        }
    }
}
