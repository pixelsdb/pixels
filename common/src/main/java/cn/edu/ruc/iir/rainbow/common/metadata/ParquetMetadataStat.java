package cn.edu.ruc.iir.rainbow.common.metadata;

import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.Type;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/1/31.
 */
public class ParquetMetadataStat implements MetadataStat
{
    private List<ParquetFileMetadata> fileMetaDataList = new ArrayList<ParquetFileMetadata>();
    private List<Type> fields = null;
    private int columnCount = 0;
    private long rowCount = 0;

    /**
     * this block is parquet row group
     */
    private List<BlockMetaData> blockMetaDataList = null;

    /**
     * get the number of rows
     * @return
     */
    @Override
    public long getRowCount ()
    {
        if (this.rowCount <= 0)
        {
            long rowCount = 0;
            for (BlockMetaData block : getBlocks())
            {
                rowCount += block.getRowCount();
            }
            this.rowCount = rowCount;
        }
        return this.rowCount;
    }

    /**
     *
     * @param nameNode the hostname of hdfs namenode
     * @param hdfsPort the port of hdfs namenode, usually 9000 or 8020
     * @param dirPath the path of the directory which contains the parquet files, begin with /, for gen /msra/column/order/parquet/
     * @throws IOException
     * @throws MetadataException
     */
    public ParquetMetadataStat(String nameNode, int hdfsPort, String dirPath) throws IOException, MetadataException
    {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://" + nameNode + ":" + hdfsPort), conf);
        Path hdfsDirPath = new Path(dirPath);
        if (! fileSystem.isFile(hdfsDirPath))
        {
            FileStatus[] fileStatuses = fileSystem.listStatus(hdfsDirPath);
            for (FileStatus status : fileStatuses)
            {
                // compatibility for HDFS 1.x
                if (! status.isDir())
                {
                    //System.out.println(status.getPath().toString());
                    this.fileMetaDataList.add(new ParquetFileMetadata(conf, status.getPath()));
                }
            }
        }
        if (this.fileMetaDataList.size() == 0)
        {
            throw new MetadataException("fileMetaDataList is empty, path is not a dir.");
        }
        this.fields = this.fileMetaDataList.get(0).getFileMetaData().getSchema().getFields();
        this.columnCount = this.fileMetaDataList.get(0).getFileMetaData().getSchema().getFieldCount();
    }

    /**
     * get the blocks (row groups) of the parquet files.
     * @return
     */
    public List<BlockMetaData> getBlocks ()
    {
        if (this.blockMetaDataList == null || this.blockMetaDataList.size() == 0)
        {
            this.blockMetaDataList = new ArrayList<BlockMetaData>();
            for (ParquetFileMetadata meta : this.fileMetaDataList)
            {
                this.blockMetaDataList.addAll(meta.getBlocks());
            }
        }
        return this.blockMetaDataList;
    }

    public List<ParquetFileMetadata> getFileMetaData ()
    {
        return this.fileMetaDataList;
    }

    /**
     * get the average column chunk size of all the row groups
     * @return
     */
    @Override
    public double[] getAvgColumnChunkSize ()
    {
        double[] sum = new double[this.columnCount];

        for (int i = 0; i < this.columnCount; ++i)
        {
            sum[i] = 0;
        }

        for (BlockMetaData block : getBlocks())
        {
            int i = 0;
            for (ColumnChunkMetaData column : block.getColumns())
            {
                sum[i] += column.getTotalSize();
                i++;
            }
        }

        long blockCount = this.getRowGroupCount();
        for (int i = 0; i < this.columnCount; ++i)
        {
            sum[i] /= blockCount;
        }

        return sum;
    }

    /**
     * get the standard deviation of the column chunk sizes.
     * @param avgSize
     * @return
     */
    @Override
    public double[] getColumnChunkSizeStdDev (double[] avgSize)
    {
        double[] dev = new double[this.columnCount];

        for (int i = 0; i < this.columnCount; ++i)
        {
            dev[i] = 0;
        }

        for (BlockMetaData block : getBlocks())
        {
            int i = 0;
            for (ColumnChunkMetaData column : block.getColumns())
            {
                dev[i] += Math.pow(column.getTotalSize() - avgSize[i], 2);
                i++;
            }
        }

        long blockCount = this.getRowGroupCount();
        for (int i = 0; i < this.columnCount; ++i)
        {
            dev[i] = Math.sqrt(dev[i] / blockCount);
        }

        return dev;
    }

    /**
     * get the field (column) names.
     * @return
     */
    @Override
    public List<String> getFieldNames ()
    {
        List<String> names = new ArrayList<String>();
        for (Type type : this.fields)
        {
            names.add(type.getName());
        }
        return names;
    }

    /**
     * get the number of row groups (parquet blocks).
     * @return
     */
    public int getRowGroupCount ()
    {
        return this.getBlocks().size();
    }

    /**
     * get the number of files.
     * @return
     */
    @Override
    public int getFileCount ()
    {
        return this.fileMetaDataList.size();
    }

    /**
     * get the average compressed size of the rows in the parquet files.
     * @return
     */
    @Override
    public double getRowSize ()
    {
        double size = 0;
        for (double columnSize : getAvgColumnChunkSize())
        {
            size += columnSize;
        }
        return size*this.getRowGroupCount()/this.getRowCount();
    }

    /**
     * get the total compressed size of the parquet files.
     * @return
     */
    @Override
    public long getTotalSize ()
    {
        long size = 0;
        for (BlockMetaData meta : this.getBlocks())
        {
            size += meta.getCompressedSize();
        }
        return size;
    }
}
