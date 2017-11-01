package cn.edu.ruc.iir.rainbow.common.metadata;

import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;

import java.util.List;

public interface MetadataStat
{
    /**
     * get the number of rows
     * @return
     */
    public long getRowCount();

    /**
     * get the average column chunk size of all the row groups
     * @return
     */
    public double[] getAvgColumnChunkSize();

    /**
     * get the standard deviation of the column chunk sizes.
     * @param avgSize
     * @return
     */
    public double[] getColumnChunkSizeStdDev(double[] avgSize) throws MetadataException;

    /**
     * get the field (column) names.
     * @return
     */
    public List<String> getFieldNames();

    /**
     * get the number of files.
     * @return
     */
    public int getFileCount();

    /**
     * get the number of row groups.
     * @return
     */
    public int getRowGroupCount();

    /**
     * get the average compressed size of the rows in the files.
     * @return
     */
    public double getRowSize();

    /**
     * get the total compressed size of the files.
     * @return
     */
    public long getTotalSize();
}
