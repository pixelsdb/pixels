package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.reader.VectorReader;

import java.io.IOException;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsReader
{
    /**
     * Get the iterator for reading rows inside the file
     * @return {@code VectorReader}
     * */
    VectorReader vectors(int[] rowGroups, String[] fieldNames) throws IOException;

    PixelsProto.RowGroupFooter readRowGroupFooter(int rowGroupId) throws IOException;

    /**
     * Get version of the Pixels file
     * @return version number
     * */
    int getFileVersion();

    /**
     * Get the number of rows of the file
     * @return num of rows
     * */
    long getNumberOfRows();

    /**
     * Get the compression codec used in this file
     * @return compression codec
     * */
    PixelsProto.CompressionKind getCompressionKind();

    /**
     * Get the compression block size
     * @return compression block size
     * */
    long getCompressionBlockSize();

    /**
     * Get the pixel stride
     * @return pixel stride
     * */
    long getPixelStride();

    /**
     * Get the writer's time zone
     * @return time zone
     * */
    String getWriterTimeZone();

    /**
     * Get schema of this file
     * @return schema
     * */
    TypeDescription getSchema();

    /**
     * Get the number of row groups in this file
     * @return row group num
     * */
    int getRowGroupNum();

    /**
     * Get file level statistics of each column
     * @return array of column stat
     * */
    List<PixelsProto.ColumnStatistic> getColumnStats();

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     * */
    PixelsProto.ColumnStatistic getColumnStat(String columnName);

    /**
     * Get information of all row groups
     * @return array of row group information
     * */
    List<PixelsProto.RowGroupInformation> getRowGroupInfos();

    /**
     * Get information of specified row group
     *
     * @param rowGroupId row group id
     * @return row group information
     * */
    PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId);

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     * */
    PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId);

    /**
     * Get statistics of all row groups
     * @return row groups statistics
     * */
    List<PixelsProto.RowGroupStatistic> getRowGroupStats();
}
