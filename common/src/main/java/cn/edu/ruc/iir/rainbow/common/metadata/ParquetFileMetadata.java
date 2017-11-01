package cn.edu.ruc.iir.rainbow.common.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;

import java.io.IOException;
import java.util.List;

import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ParquetFileMetadata
{
    private parquet.hadoop.metadata.ParquetMetadata metaData = null;

    public ParquetFileMetadata(Configuration conf, Path hdfsFilePath) throws IOException
    {
        this.metaData = ParquetFileReader.readFooter(conf, hdfsFilePath, NO_FILTER);
    }

    public parquet.hadoop.metadata.FileMetaData getFileMetaData ()
    {
        return this.metaData.getFileMetaData();
    }

    public List<BlockMetaData> getBlocks ()
    {
        return this.metaData.getBlocks();
    }
}
